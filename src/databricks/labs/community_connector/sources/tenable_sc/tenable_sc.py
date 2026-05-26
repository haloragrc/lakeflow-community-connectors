"""Tenable.sc connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
    first_present,
)
from databricks.labs.community_connector.sources.tenable_sc.tenable_sc_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class TenableScLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_key = options.get("access_key")
        secret_key = options.get("secret_key")
        if not access_key or not secret_key:
            raise ValueError("tenable_sc requires access_key and secret_key")

        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://tenable-sc.example.local"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "x-apikey": f"accesskey={access_key}; secretkey={secret_key}",
            },
        )
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
        if table_name == "assets":
            payload = self.client.request_json("GET", "/rest/asset")
            records = [self._map_asset(r) for r in self._records(payload)]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "repositories":
            payload = self.client.request_json("GET", "/rest/repository")
            records = [self._map_repository(r) for r in self._records(payload)]
            return iter(records), {}
        if table_name == "findings":
            payload = self.client.request_json("GET", "/rest/vuln")
            records = [self._map_finding(r) for r in self._records(payload)]
            return self._finalize_cdc(records, start_offset, "last_seen")
        if table_name == "scans":
            payload = self.client.request_json("GET", "/rest/scan")
            records = [self._map_scan(r) for r in self._records(payload)]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "scan_results":
            payload = self.client.request_json("GET", "/rest/scanResult")
            records = [self._map_scan_result(r) for r in self._records(payload)]
            return self._finalize_cdc(records, start_offset, "updated_at")
        raise ValueError(f"Unsupported table: {table_name!r}")

    def _records(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if not isinstance(payload, dict):
            return []
        if isinstance(payload.get("response"), list):
            return [x for x in payload["response"] if isinstance(x, dict)]
        if isinstance(payload.get("response"), dict):
            response = payload["response"]
            for key in ("usable", "manageable", "results", "records"):
                value = response.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]
            if all(not isinstance(v, list) for v in response.values()):
                return [response]
        for key in ("assets", "repositories", "vulns", "scans", "scanResults", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
        return []

    def _map_asset(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "asset_id": self._to_int(first_present(row.get("id"), row.get("assetID"), row.get("asset_id"))) or -1,
            "ip": first_present(row.get("ip"), row.get("ipAddress")),
            "dns_name": first_present(row.get("dnsName"), row.get("dns_name")),
            "netbios_name": first_present(row.get("netbiosName"), row.get("netbios_name")),
            "repository_id": self._to_int(first_present((row.get("repository") or {}).get("id") if isinstance(row.get("repository"), dict) else None, row.get("repositoryID"))),
            "repository_name": first_present((row.get("repository") or {}).get("name") if isinstance(row.get("repository"), dict) else None, row.get("repositoryName")),
            "updated_at": as_iso8601(first_present(row.get("modifiedTime"), row.get("lastSeen"), row.get("updated_at"))),
        }
        out["raw_payload"] = json.dumps(row, separators=(",", ":"), default=str)
        return out

    def _map_repository(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "repository_id": self._to_int(row.get("id")) or -1,
            "name": row.get("name"),
            "description": row.get("description"),
            "type": row.get("type"),
            "data_format": first_present(row.get("dataFormat"), row.get("data_format")),
            "owner": first_present((row.get("owner") or {}).get("username") if isinstance(row.get("owner"), dict) else None, row.get("owner")),
            "updated_at": as_iso8601(first_present(row.get("modifiedTime"), row.get("createdTime"))),
        }
        out["raw_payload"] = json.dumps(row, separators=(",", ":"), default=str)
        return out

    def _map_finding(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "finding_id": str(first_present(row.get("id"), row.get("findingID"), f"{row.get('assetID','0')}:{row.get('pluginID','0')}")),
            "plugin_id": self._to_int(first_present(row.get("pluginID"), row.get("plugin_id"))),
            "severity": str(first_present(row.get("severity"), row.get("severityLevel"), "0")),
            "state": first_present(row.get("state"), row.get("status"), "OPEN"),
            "asset_id": self._to_int(first_present(row.get("assetID"), row.get("asset_id"))),
            "repository_id": self._to_int(first_present(row.get("repositoryID"), row.get("repository_id"))),
            "first_seen": as_iso8601(first_present(row.get("firstSeen"), row.get("first_seen"), row.get("firstDiscovered"))),
            "last_seen": as_iso8601(first_present(row.get("lastSeen"), row.get("last_seen"), row.get("modifiedTime"))),
        }
        out["raw_payload"] = json.dumps(row, separators=(",", ":"), default=str)
        return out

    def _map_scan(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "scan_id": self._to_int(row.get("id")) or -1,
            "name": row.get("name"),
            "description": row.get("description"),
            "status": first_present(row.get("status"), row.get("state")),
            "repository_id": self._to_int(first_present((row.get("repository") or {}).get("id") if isinstance(row.get("repository"), dict) else None, row.get("repositoryID"))),
            "owner": first_present((row.get("owner") or {}).get("username") if isinstance(row.get("owner"), dict) else None, row.get("owner")),
            "created_at": as_iso8601(first_present(row.get("createdTime"), row.get("creationDate"))),
            "updated_at": as_iso8601(first_present(row.get("modifiedTime"), row.get("lastModTime"), row.get("createdTime"))),
        }
        out["raw_payload"] = json.dumps(row, separators=(",", ":"), default=str)
        return out

    def _map_scan_result(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "scan_result_id": self._to_int(first_present(row.get("id"), row.get("scanResultID"))) or -1,
            "scan_id": self._to_int(first_present((row.get("scan") or {}).get("id") if isinstance(row.get("scan"), dict) else None, row.get("scanID"))),
            "repository_id": self._to_int(first_present((row.get("repository") or {}).get("id") if isinstance(row.get("repository"), dict) else None, row.get("repositoryID"))),
            "status": row.get("status"),
            "import_status": first_present(row.get("importStatus"), row.get("import_status")),
            "start_time": as_iso8601(first_present(row.get("startTime"), row.get("scanStart"))),
            "finish_time": as_iso8601(first_present(row.get("finishTime"), row.get("scanFinish"))),
            "updated_at": as_iso8601(first_present(row.get("finishTime"), row.get("startTime"), row.get("createdTime"))),
        }
        out["raw_payload"] = json.dumps(row, separators=(",", ":"), default=str)
        return out

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

    @staticmethod
    def _to_int(value: Any) -> int | None:
        try:
            if value is None or value == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None
