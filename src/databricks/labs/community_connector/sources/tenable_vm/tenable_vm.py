"""Tenable Vulnerability Management (legacy Tenable.io) Lakeflow connector."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.tenable_vm.tenable_vm_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class TenableVMLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Tenable VM APIs."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        access_key = options.get("access_key")
        secret_key = options.get("secret_key")
        if not access_key or not secret_key:
            raise ValueError("Tenable VM connector requires 'access_key' and 'secret_key'")

        self.base_url = options.get("base_url", "https://cloud.tenable.com").rstrip("/")
        self.timeout_seconds = int(options.get("timeout_seconds", "30"))
        self.chunk_size = max(100, min(int(options.get("chunk_size", "1000")), 10_000))
        self.max_poll_attempts = int(options.get("max_poll_attempts", "20"))
        self.poll_interval_seconds = float(options.get("poll_interval_seconds", "0.1"))

        self._init_time = datetime.now(timezone.utc)
        self._init_ts = int(self._init_time.timestamp())

        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-ApiKeys": f"accessKey={access_key};secretKey={secret_key}",
                "Accept": "application/json",
            }
        )

        self._table_readers: dict[str, Callable[[dict, dict[str, str]], tuple[Iterator[dict], dict]]] = {
            "assets": self._read_assets,
            "vulnerabilities": self._read_vulnerabilities,
            "findings": self._read_findings,
            "scans": self._read_scans,
            "scan_results": self._read_scan_results,
            "exports": self._read_exports,
            "tags": self._read_tags,
            "plugins": self._read_plugins,
            "policies": self._read_policies,
            "users": self._read_users,
        }

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        del table_options
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        del table_options
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        if table_name not in self._table_readers:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return self._table_readers[table_name](start_offset or {}, table_options)

    # ------------------------------------------------------------------
    # Table readers
    # ------------------------------------------------------------------

    def _read_assets(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        since_ts = self._resolve_cursor_ts(start_offset, table_options)
        payload: dict[str, Any] = {
            "chunk_size": self.chunk_size,
            "include_resource_tags": True,
        }
        if since_ts is not None:
            payload["filters"] = {"since": since_ts}

        records = self._run_export_job(
            request_path="/assets/v2/export",
            status_path_template="/assets/export/{export_uuid}/status",
            chunk_path_template="/assets/export/{export_uuid}/chunks/{chunk_id}",
            payload=payload,
            record_keys=("assets",),
        )

        mapped = [self._map_asset_record(r) for r in records]
        return self._finalize_cdc_batch(mapped, start_offset, "updated_at")

    def _read_vulnerabilities(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_vulnerability_family(start_offset, table_options, table_name="vulnerabilities")

    def _read_findings(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_vulnerability_family(start_offset, table_options, table_name="findings")

    def _read_vulnerability_family(
        self,
        start_offset: dict,
        table_options: dict[str, str],
        table_name: str,
    ) -> tuple[Iterator[dict], dict]:
        since_ts = self._resolve_cursor_ts(start_offset, table_options)
        payload: dict[str, Any] = {"num_assets": self.chunk_size}
        filters: dict[str, Any] = {}
        if since_ts is not None:
            filters["since"] = since_ts
        state_filter = table_options.get("state")
        if state_filter:
            filters["state"] = [state_filter.upper()]
        if filters:
            payload["filters"] = filters

        records = self._run_export_job(
            request_path="/vulns/export",
            status_path_template="/vulns/export/{export_uuid}/status",
            chunk_path_template="/vulns/export/{export_uuid}/chunks/{chunk_id}",
            payload=payload,
            record_keys=("vulnerabilities", "findings"),
        )

        mapped = [self._map_vulnerability_record(r, table_name=table_name) for r in records]
        return self._finalize_cdc_batch(mapped, start_offset, "indexed_at")

    def _read_scans(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        params: dict[str, Any] = {}
        since_ts = self._resolve_cursor_ts(start_offset, table_options)
        if since_ts is not None:
            params["last_modification_date"] = since_ts

        body = self._request_json("GET", "/scans", params=params)
        scans = self._extract_records(body, keys=("scans",))
        mapped = [self._map_scan_record(r) for r in scans]
        return self._finalize_cdc_batch(mapped, start_offset, "last_modification_date")

    def _read_scan_results(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        del table_options

        scan_body = self._request_json("GET", "/scans")
        scans = self._extract_records(scan_body, keys=("scans",))
        all_history: list[dict[str, Any]] = []

        for scan in scans:
            scan_id = scan.get("id")
            scan_uuid = scan.get("schedule_uuid") or scan.get("uuid") or str(scan_id)
            if scan_id is None or not scan_uuid:
                continue

            history = self._fetch_scan_history(scan_id)
            for item in history:
                all_history.append(self._map_scan_history_record(item, scan_id=scan_id, scan_uuid=scan_uuid))

        return self._finalize_cdc_batch(all_history, start_offset, "last_modification_date")

    def _read_exports(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        del start_offset
        del table_options

        out: list[dict[str, Any]] = []

        asset_jobs = self._request_json("GET", "/assets/export/status")
        for job in self._extract_records(asset_jobs, keys=("exports", "jobs", "asset_exports")):
            out.append(self._map_export_record(job, export_type="assets"))

        vuln_jobs = self._request_json("GET", "/vulns/export/status")
        for job in self._extract_records(vuln_jobs, keys=("exports", "jobs", "vuln_exports")):
            out.append(self._map_export_record(job, export_type="vulnerabilities"))

        return iter(out), {}

    def _read_tags(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        del start_offset
        del table_options

        body = self._request_json("GET", "/tags/values")
        tags = self._extract_records(body, keys=("values", "tags"))
        return iter(self._map_tag_record(t) for t in tags), {}

    def _read_plugins(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        since_ts = self._resolve_cursor_ts(start_offset, table_options)
        params: dict[str, Any] = {
            "size": int(table_options.get("plugin_page_size", "1000")),
            "page": int(table_options.get("plugin_page", "1")),
        }
        if since_ts is not None:
            params["last_updated"] = self._epoch_to_date(since_ts)

        body = self._request_json("GET", "/plugins/plugin", params=params)
        plugins = self._extract_records(body, keys=("plugins",))
        mapped = [self._map_plugin_record(r) for r in plugins]
        return self._finalize_cdc_batch(mapped, start_offset, "plugin_modification_date")

    def _read_policies(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        del start_offset
        del table_options

        body = self._request_json("GET", "/policies")
        policies = self._extract_records(body, keys=("policies",))
        return iter(self._map_policy_record(p) for p in policies), {}

    def _read_users(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        del start_offset
        del table_options

        body = self._request_json("GET", "/users")
        users = self._extract_records(body, keys=("users",))
        return iter(self._map_user_record(u) for u in users), {}

    # ------------------------------------------------------------------
    # HTTP + export helpers
    # ------------------------------------------------------------------

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        ok_statuses: tuple[int, ...] = (200,),
    ) -> Any:
        response = self._session.request(
            method=method,
            url=f"{self.base_url}{path}",
            params=params,
            json=payload,
            timeout=self.timeout_seconds,
        )
        if response.status_code not in ok_statuses:
            raise RuntimeError(
                f"Tenable VM API error {response.status_code} for {method} {path}: {response.text}"
            )
        return self._decode_response_json(response)

    def _decode_response_json(self, response: requests.Response) -> Any:
        try:
            return response.json()
        except ValueError:
            text = response.content.decode("utf-8", errors="replace").strip()
            if not text:
                return {}

            # Some export chunk endpoints respond as NDJSON/octet-stream.
            lines = [line for line in text.splitlines() if line.strip()]
            if len(lines) > 1:
                out: list[dict[str, Any]] = []
                for line in lines:
                    try:
                        parsed = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(parsed, dict):
                        out.append(parsed)
                if out:
                    return out

            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {}

    def _run_export_job(
        self,
        *,
        request_path: str,
        status_path_template: str,
        chunk_path_template: str,
        payload: dict[str, Any],
        record_keys: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        request_response = self._session.request(
            method="POST",
            url=f"{self.base_url}{request_path}",
            json=payload,
            timeout=self.timeout_seconds,
        )

        export_uuid = self._extract_export_uuid(request_response)
        status_body = self._poll_export_status(status_path_template, export_uuid)
        chunks = self._extract_chunks_available(status_body)

        records: list[dict[str, Any]] = []
        for chunk_id in chunks:
            chunk_path = chunk_path_template.format(export_uuid=export_uuid, chunk_id=chunk_id)
            chunk_body = self._request_json("GET", chunk_path)
            records.extend(self._extract_records(chunk_body, keys=record_keys))

        return records

    def _extract_export_uuid(self, response: requests.Response) -> str:
        if response.status_code in (200, 202):
            body = self._decode_response_json(response)
            export_uuid = body.get("export_uuid") or body.get("uuid")
            if export_uuid:
                return str(export_uuid)
            raise RuntimeError(f"Missing export_uuid in export response: {body}")

        if response.status_code == 409:
            body = self._decode_response_json(response)
            export_uuid = body.get("active_job_id")
            if export_uuid:
                return str(export_uuid)
            raise RuntimeError(f"Duplicate export without active_job_id: {body}")

        raise RuntimeError(
            f"Tenable VM export request failed with {response.status_code}: {response.text}"
        )

    def _poll_export_status(self, status_path_template: str, export_uuid: str) -> dict[str, Any]:
        last_status: dict[str, Any] = {}
        path = status_path_template.format(export_uuid=export_uuid)

        for _ in range(self.max_poll_attempts):
            body = self._request_json("GET", path)
            last_status = body if isinstance(body, dict) else {}
            status = str(last_status.get("status", "")).upper()
            if status in {"FINISHED", "READY", "CANCELLED"}:
                return last_status
            if status == "ERROR":
                raise RuntimeError(
                    f"Tenable VM export job {export_uuid} finished in ERROR status: {last_status}"
                )
            time.sleep(self.poll_interval_seconds)

        return last_status

    @staticmethod
    def _extract_chunks_available(status_body: dict[str, Any]) -> list[int]:
        chunks = status_body.get("chunks_available")
        if isinstance(chunks, list):
            return sorted(int(c) for c in chunks)

        if isinstance(status_body.get("chunks"), list):
            return sorted(int(c) for c in status_body["chunks"])

        return []

    def _fetch_scan_history(self, scan_id: int) -> list[dict[str, Any]]:
        history: list[dict[str, Any]] = []
        offset = 0
        limit = 200

        while True:
            body = self._request_json(
                "GET",
                f"/scans/{scan_id}/history",
                params={
                    "offset": offset,
                    "limit": limit,
                    "exclude_rollover": "false",
                },
            )
            items = self._extract_records(body, keys=("history", "scans", "results"))
            if not items:
                break

            history.extend(items)
            if len(items) < limit:
                break

            offset += limit

        return history

    # ------------------------------------------------------------------
    # Record mapping
    # ------------------------------------------------------------------

    def _map_asset_record(self, record: dict[str, Any]) -> dict[str, Any]:
        asset_id = record.get("id") or record.get("uuid")
        tags = record.get("tags") or []
        tag_values = []
        if isinstance(tags, list):
            for tag in tags:
                if isinstance(tag, str):
                    tag_values.append(tag)
                elif isinstance(tag, dict):
                    category = tag.get("category") or tag.get("category_name")
                    value = tag.get("value")
                    if category and value:
                        tag_values.append(f"{category}:{value}")
                    elif value:
                        tag_values.append(str(value))

        return {
            "asset_uuid": str(asset_id) if asset_id is not None else "",
            "asset_id": str(asset_id) if asset_id is not None else None,
            "ipv4": self._first_from_list(record.get("ipv4s")),
            "hostname": record.get("hostname"),
            "fqdn": self._first_from_list(record.get("fqdns")),
            "operating_system": record.get("operating_system") or record.get("os"),
            "last_seen": self._normalize_timestamp(record.get("last_seen")),
            "updated_at": self._normalize_timestamp(record.get("updated_at") or record.get("last_seen")),
            "terminated_at": self._normalize_timestamp(record.get("terminated_at")),
            "deleted_at": self._normalize_timestamp(record.get("deleted_at")),
            "is_deleted": bool(record.get("deleted_at") or record.get("terminated_at")),
            "tags": tag_values,
            "raw_payload": json.dumps(record, separators=(",", ":"), default=str),
        }

    def _map_vulnerability_record(self, record: dict[str, Any], *, table_name: str) -> dict[str, Any]:
        del table_name
        plugin = record.get("plugin") if isinstance(record.get("plugin"), dict) else {}
        asset = record.get("asset") if isinstance(record.get("asset"), dict) else {}
        port = record.get("port") if isinstance(record.get("port"), dict) else {}

        plugin_id = plugin.get("id")
        try:
            plugin_id_num = int(plugin_id) if plugin_id is not None else None
        except (TypeError, ValueError):
            plugin_id_num = None

        cve_values = plugin.get("cve") or plugin.get("cves") or []
        if not isinstance(cve_values, list):
            cve_values = [str(cve_values)]

        finding_id = record.get("finding_id")
        if not finding_id:
            asset_uuid = asset.get("uuid") or "unknown"
            finding_id = f"{asset_uuid}:{plugin_id_num or 0}:{record.get('state') or 'unknown'}"

        indexed = record.get("indexed")

        return {
            "finding_id": str(finding_id),
            "asset_uuid": str(asset.get("uuid")) if asset.get("uuid") is not None else None,
            "plugin_id": plugin_id_num,
            "plugin_name": plugin.get("name"),
            "plugin_family": plugin.get("family") or plugin.get("family_name"),
            "severity": str(record.get("severity")) if record.get("severity") is not None else None,
            "state": str(record.get("state")).upper() if record.get("state") is not None else None,
            "indexed_at": self._normalize_timestamp(indexed),
            "first_found": self._normalize_timestamp(record.get("first_found")),
            "last_found": self._normalize_timestamp(record.get("last_found")),
            "last_fixed": self._normalize_timestamp(record.get("last_fixed")),
            "cve": [str(c) for c in cve_values],
            "port": str(port.get("port")) if port.get("port") is not None else None,
            "protocol": str(port.get("protocol")) if port.get("protocol") is not None else None,
            "source": record.get("source"),
            "raw_payload": json.dumps(record, separators=(",", ":"), default=str),
        }

    def _map_scan_record(self, record: dict[str, Any]) -> dict[str, Any]:
        scan_uuid = record.get("schedule_uuid") or record.get("uuid") or record.get("id")
        scan_id = self._to_int(record.get("id"))
        return {
            "scan_uuid": str(scan_uuid) if scan_uuid is not None else "",
            "scan_id": scan_id,
            "name": record.get("name"),
            "status": record.get("status"),
            "folder_id": self._to_int(record.get("folder_id")),
            "enabled": bool(record.get("enabled")) if record.get("enabled") is not None else None,
            "creation_date": self._normalize_timestamp(record.get("creation_date")),
            "last_modification_date": self._normalize_timestamp(record.get("last_modification_date")),
            "start_time": self._normalize_timestamp(record.get("starttime") or record.get("start_time")),
            "owner": record.get("owner"),
            "raw_payload": json.dumps(record, separators=(",", ":"), default=str),
        }

    def _map_scan_history_record(
        self,
        record: dict[str, Any],
        *,
        scan_id: int,
        scan_uuid: str,
    ) -> dict[str, Any]:
        history_id = self._to_int(record.get("history_id") or record.get("id"))

        return {
            "scan_uuid": str(scan_uuid),
            "scan_id": scan_id,
            "history_id": history_id if history_id is not None else -1,
            "status": record.get("status"),
            "uuid": record.get("uuid"),
            "creation_date": self._normalize_timestamp(record.get("creation_date")),
            "last_modification_date": self._normalize_timestamp(
                record.get("last_modification_date") or record.get("creation_date")
            ),
            "schedule_uuid": record.get("schedule_uuid"),
            "scanner_uuid": record.get("scanner_uuid"),
            "owner_id": self._to_int(record.get("owner_id")),
            "type": record.get("type"),
            "raw_payload": json.dumps(record, separators=(",", ":"), default=str),
        }

    def _map_export_record(self, record: dict[str, Any], *, export_type: str) -> dict[str, Any]:
        export_uuid = record.get("uuid") or record.get("export_uuid")
        return {
            "export_type": export_type,
            "export_uuid": str(export_uuid) if export_uuid is not None else "",
            "status": record.get("status"),
            "chunks_available": json.dumps(record.get("chunks_available", []), separators=(",", ":")),
            "chunks_failed": json.dumps(record.get("chunks_failed", []), separators=(",", ":")),
            "created": self._normalize_timestamp(record.get("created")),
            "updated": self._normalize_timestamp(record.get("updated")),
            "raw_payload": json.dumps(record, separators=(",", ":"), default=str),
        }

    def _map_tag_record(self, record: dict[str, Any]) -> dict[str, Any]:
        tag_uuid = record.get("uuid")
        return {
            "tag_uuid": str(tag_uuid) if tag_uuid is not None else "",
            "category_uuid": record.get("category_uuid"),
            "category_name": record.get("category_name"),
            "value": record.get("value"),
            "type": record.get("type"),
            "created_at": self._normalize_timestamp(record.get("created_at")),
            "updated_at": self._normalize_timestamp(record.get("updated_at")),
            "updated_by": record.get("updated_by"),
            "raw_payload": json.dumps(record, separators=(",", ":"), default=str),
        }

    def _map_plugin_record(self, record: dict[str, Any]) -> dict[str, Any]:
        cves = record.get("cve") or record.get("cves") or []
        if not isinstance(cves, list):
            cves = [str(cves)]

        return {
            "plugin_id": self._to_int(record.get("id")) or -1,
            "plugin_name": record.get("name"),
            "family_name": record.get("family_name") or record.get("family"),
            "severity": record.get("severity"),
            "plugin_modification_date": self._normalize_timestamp(record.get("plugin_modification_date")),
            "plugin_publication_date": self._normalize_timestamp(record.get("plugin_publication_date")),
            "plugin_version": record.get("plugin_version"),
            "cve": [str(c) for c in cves],
            "raw_payload": json.dumps(record, separators=(",", ":"), default=str),
        }

    def _map_policy_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "policy_id": self._to_int(record.get("id")) or -1,
            "name": record.get("name"),
            "description": record.get("description"),
            "owner": record.get("owner"),
            "visibility": record.get("visibility"),
            "shared": bool(record.get("shared")) if record.get("shared") is not None else None,
            "creation_date": self._normalize_timestamp(record.get("creation_date")),
            "last_modification_date": self._normalize_timestamp(record.get("last_modification_date")),
            "raw_payload": json.dumps(record, separators=(",", ":"), default=str),
        }

    def _map_user_record(self, record: dict[str, Any]) -> dict[str, Any]:
        user_id = self._to_int(record.get("id")) or -1
        return {
            "user_id": user_id,
            "uuid": record.get("uuid"),
            "username": record.get("username"),
            "email": record.get("email"),
            "name": record.get("name"),
            "type": record.get("type"),
            "permissions": self._to_int(record.get("permissions")),
            "last_login": self._normalize_timestamp(record.get("last_login")),
            "enabled": bool(record.get("enabled")) if record.get("enabled") is not None else None,
            "raw_payload": json.dumps(record, separators=(",", ":"), default=str),
        }

    # ------------------------------------------------------------------
    # Offset and normalization helpers
    # ------------------------------------------------------------------

    def _resolve_cursor_ts(self, start_offset: dict, table_options: dict[str, str]) -> int | None:
        if start_offset.get("cursor_ts") is not None:
            return self._to_int(start_offset.get("cursor_ts"))

        if table_options.get("start_unix_ts") is not None:
            return self._to_int(table_options.get("start_unix_ts"))

        if table_options.get("start_date"):
            return self._timestamp_to_epoch(table_options.get("start_date"))

        return None

    def _finalize_cdc_batch(
        self,
        records: list[dict[str, Any]],
        start_offset: dict,
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        current = self._to_int(start_offset.get("cursor_ts")) if start_offset else None

        if not records:
            return iter([]), start_offset or {}

        max_cursor = self._max_cursor_ts(records, cursor_field)
        if max_cursor is None:
            # No usable cursor in payload; return one-shot batch then stop.
            if start_offset:
                return iter([]), start_offset
            return iter(records), {"cursor_ts": self._init_ts}

        # Cap to initialization time so one trigger run terminates deterministically.
        if max_cursor > self._init_ts:
            max_cursor = self._init_ts

        if current is not None and max_cursor <= current:
            return iter([]), start_offset

        return iter(records), {"cursor_ts": max_cursor}

    def _max_cursor_ts(self, records: list[dict[str, Any]], cursor_field: str) -> int | None:
        cursor_values = []
        for record in records:
            ts = self._timestamp_to_epoch(record.get(cursor_field))
            if ts is not None:
                cursor_values.append(ts)
        if not cursor_values:
            return None
        return max(cursor_values)

    @staticmethod
    def _extract_records(payload: Any, *, keys: tuple[str, ...]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [p for p in payload if isinstance(p, dict)]

        if not isinstance(payload, dict):
            return []

        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [p for p in value if isinstance(p, dict)]

        if any(isinstance(v, dict) for v in payload.values()):
            return []

        return []

    @staticmethod
    def _first_from_list(value: Any) -> str | None:
        if isinstance(value, list) and value:
            first = value[0]
            return str(first) if first is not None else None
        return None

    @staticmethod
    def _to_int(value: Any) -> int | None:
        try:
            if value is None or value == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_timestamp(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        text = str(value).strip()
        if not text:
            return None

        if text.isdigit():
            return datetime.fromtimestamp(int(text), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        if text.endswith("Z"):
            return text

        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return text

    @staticmethod
    def _timestamp_to_epoch(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)

        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            return int(text)

        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            return None

    @staticmethod
    def _epoch_to_date(epoch_seconds: int) -> str:
        return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime("%Y-%m-%d")
