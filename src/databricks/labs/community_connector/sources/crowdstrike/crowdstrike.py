"""CrowdStrike Falcon connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.crowdstrike.crowdstrike_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import (
    ClientCredentialsTokenProvider,
    HighValueApiClient,
    as_iso8601,
    first_present,
)


class CrowdStrikeLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        client_id = options.get("client_id")
        client_secret = options.get("client_secret")
        if not client_id or not client_secret:
            raise ValueError("crowdstrike requires client_id and client_secret")

        timeout_seconds = int(options.get("timeout_seconds", "30"))
        token_provider = ClientCredentialsTokenProvider(
            token_url=options.get("token_url", "https://api.crowdstrike.com/oauth2/token"),
            client_id=client_id,
            client_secret=client_secret,
            timeout_seconds=timeout_seconds,
            scope=options.get("scope"),
        )
        access_token = token_provider.get_token()

        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://api.crowdstrike.com"),
            timeout_seconds=timeout_seconds,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}",
            },
        )
        self._page_size = int(options.get("page_size", "200"))
        self._init_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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
        del table_options
        start_offset = start_offset or {}

        if table_name == "hosts":
            rows = self._paged_resources("/devices/combined/devices/v1")
            records = [self._map_host(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "last_seen")
        if table_name == "detections":
            rows = self._paged_resources("/detects/combined/detects/v1")
            records = [self._map_detection(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "incidents":
            rows = self._paged_resources("/incidents/combined/incidents/v1")
            records = [self._map_incident(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "vulnerabilities":
            rows = self._paged_resources("/spotlight/combined/vulnerabilities/v1")
            records = [self._map_vulnerability(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "last_seen")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _paged_resources(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        offset = int(params.get("offset", 0))
        page_size = int(params.get("limit", self._page_size))
        all_rows: list[dict[str, Any]] = []

        for _ in range(1000):
            request_params = {**params, "offset": offset, "limit": page_size}
            body = self.client.request_json("GET", path, params=request_params)
            rows = self._rows(body)
            all_rows.extend(rows)

            next_offset = self._next_offset(body, offset, page_size, len(rows))
            if next_offset is None:
                break
            offset = next_offset

        return all_rows

    def _rows(self, body: Any) -> list[dict[str, Any]]:
        if isinstance(body, dict):
            for key in ("resources", "items", "data"):
                value = body.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]
        if isinstance(body, list):
            return [x for x in body if isinstance(x, dict)]
        return []

    def _next_offset(self, body: Any, offset: int, page_size: int, rows_count: int) -> int | None:
        if not isinstance(body, dict):
            return None

        meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
        pagination = meta.get("pagination") if isinstance(meta.get("pagination"), dict) else {}

        next_offset = pagination.get("next_offset")
        if isinstance(next_offset, int):
            return next_offset

        total = pagination.get("total")
        if isinstance(total, int):
            if offset + rows_count >= total:
                return None
            return offset + page_size

        if rows_count < page_size:
            return None
        return offset + page_size

    def _map_host(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "host_id": str(first_present(row.get("device_id"), row.get("id"), "")),
            "hostname": first_present(row.get("hostname"), row.get("device_name")),
            "platform": first_present(row.get("platform_name"), row.get("platform")),
            "os_version": row.get("os_version"),
            "status": first_present(row.get("status"), row.get("state")),
            "first_seen": as_iso8601(first_present(row.get("first_seen"), row.get("first_seen_timestamp"))),
            "last_seen": as_iso8601(first_present(row.get("last_seen"), row.get("modified_timestamp"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_detection(self, row: dict[str, Any]) -> dict[str, Any]:
        behavior = row.get("behaviors")
        behavior_id = None
        if isinstance(behavior, list) and behavior and isinstance(behavior[0], dict):
            behavior_id = behavior[0].get("behavior_id")

        return {
            "detection_id": str(first_present(row.get("detection_id"), row.get("id"), "")),
            "status": row.get("status"),
            "severity": str(first_present(row.get("severity"), row.get("max_severity_displayname"))),
            "device_id": first_present(row.get("device_id"), row.get("aid")),
            "behavior_id": behavior_id,
            "created_at": as_iso8601(first_present(row.get("created_timestamp"), row.get("created_at"))),
            "updated_at": as_iso8601(first_present(row.get("updated_timestamp"), row.get("last_behavior"), row.get("created_timestamp"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_incident(self, row: dict[str, Any]) -> dict[str, Any]:
        host_ids = row.get("host_ids")
        return {
            "incident_id": str(first_present(row.get("incident_id"), row.get("id"), "")),
            "name": first_present(row.get("name"), row.get("title")),
            "status": row.get("status"),
            "severity": str(first_present(row.get("severity"), row.get("criticality"))),
            "host_ids": json.dumps(host_ids, separators=(",", ":"), default=str) if host_ids is not None else None,
            "created_at": as_iso8601(first_present(row.get("created_timestamp"), row.get("created"))),
            "updated_at": as_iso8601(first_present(row.get("updated_timestamp"), row.get("modified_timestamp"), row.get("created_timestamp"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_vulnerability(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "vulnerability_id": str(first_present(row.get("id"), row.get("vuln_id"), "")),
            "cve": first_present(row.get("cve"), row.get("cve_id")),
            "severity": str(first_present(row.get("severity"), row.get("severity_label"))),
            "status": first_present(row.get("status"), row.get("remediation_status")),
            "device_id": first_present(row.get("aid"), row.get("device_id")),
            "first_seen": as_iso8601(first_present(row.get("first_seen"), row.get("created_timestamp"))),
            "last_seen": as_iso8601(first_present(row.get("last_seen"), row.get("updated_timestamp"), row.get("modified_timestamp"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _finalize_cdc(self, records: list[dict[str, Any]], start_offset: dict, cursor_field: str) -> tuple[Iterator[dict], dict]:
        if not records:
            return iter([]), start_offset or {}
        current = start_offset.get("cursor") if start_offset else None
        max_cursor = current
        for record in records:
            val = record.get(cursor_field)
            if val and (max_cursor is None or str(val) > str(max_cursor)):
                max_cursor = val
        if max_cursor is None:
            max_cursor = self._init_time
        if current is not None and str(max_cursor) <= str(current):
            return iter([]), start_offset
        return iter(records), {"cursor": max_cursor}
