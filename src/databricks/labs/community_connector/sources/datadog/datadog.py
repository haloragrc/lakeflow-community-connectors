"""Datadog API connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.datadog.datadog_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
    first_present,
)


class DatadogLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_key = options.get("api_key")
        app_key = options.get("app_key")
        if not api_key or not app_key:
            raise ValueError("datadog requires api_key and app_key")

        self.page_size = int(options.get("page_size", "100"))
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://api.datadoghq.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "DD-API-KEY": api_key,
                "DD-APPLICATION-KEY": app_key,
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
            body = self.client.request_json("GET", "/api/v1/hosts", params={"count": self.page_size, "start": 0})
            rows = body.get("host_list", []) if isinstance(body, dict) and isinstance(body.get("host_list"), list) else []
            records = [self._map_host(r) for r in rows if isinstance(r, dict)]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "incidents":
            body = self.client.request_json("GET", "/api/v2/incidents", params={"page[size]": self.page_size})
            rows = body.get("data", []) if isinstance(body, dict) and isinstance(body.get("data"), list) else []
            records = [self._map_incident(r) for r in rows if isinstance(r, dict)]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "monitors":
            rows = self.client.request_json("GET", "/api/v1/monitor", params={"page": 0, "page_size": self.page_size})
            records = [self._map_monitor(r) for r in rows if isinstance(rows, list) and isinstance(r, dict)] if isinstance(rows, list) else []
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "signals":
            body = self.client.request_json("GET", "/api/v2/security_monitoring/signals", params={"page[limit]": self.page_size})
            rows = body.get("data", []) if isinstance(body, dict) and isinstance(body.get("data"), list) else []
            records = [self._map_signal(r) for r in rows if isinstance(r, dict)]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "services":
            body = self.client.request_json("GET", "/api/v2/services/definitions", params={"page[size]": self.page_size})
            rows = body.get("data", []) if isinstance(body, dict) and isinstance(body.get("data"), list) else []
            records = [self._map_service(r) for r in rows if isinstance(r, dict)]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _map_host(self, row: dict[str, Any]) -> dict[str, Any]:
        aliases = row.get("aliases") if isinstance(row.get("aliases"), list) else []
        return {
            "host_id": str(first_present(row.get("id"), row.get("host_name"), "")),
            "host_name": first_present(row.get("host_name"), row.get("name")),
            "aliases": json.dumps(aliases, separators=(",", ":")) if aliases else None,
            "platform": row.get("platform"),
            "agent_version": row.get("agent_version"),
            "last_reported_time": as_iso8601(row.get("last_reported_time")),
            "updated_at": as_iso8601(first_present(row.get("last_reported_time"), row.get("up"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_incident(self, row: dict[str, Any]) -> dict[str, Any]:
        attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
        return {
            "incident_id": str(row.get("id") or ""),
            "title": attrs.get("title"),
            "severity": attrs.get("severity"),
            "state": attrs.get("state"),
            "customer_impact_scope": attrs.get("customer_impact_scope"),
            "created_at": as_iso8601(attrs.get("created")),
            "modified_at": as_iso8601(attrs.get("modified")),
            "updated_at": as_iso8601(first_present(attrs.get("modified"), attrs.get("created"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_monitor(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "monitor_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "type": row.get("type"),
            "monitor_state": row.get("overall_state"),
            "query": row.get("query"),
            "overall_state_modified": as_iso8601(row.get("overall_state_modified")),
            "updated_at": as_iso8601(first_present(row.get("overall_state_modified"), row.get("modified"), row.get("created"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_signal(self, row: dict[str, Any]) -> dict[str, Any]:
        attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
        return {
            "signal_id": str(row.get("id") or ""),
            "title": attrs.get("title"),
            "severity": attrs.get("severity"),
            "status": attrs.get("status"),
            "signal_type": attrs.get("signal_type"),
            "source": attrs.get("source"),
            "timestamp": as_iso8601(attrs.get("timestamp")),
            "updated_at": as_iso8601(first_present(attrs.get("timestamp"), attrs.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_service(self, row: dict[str, Any]) -> dict[str, Any]:
        attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
        schema = attrs.get("schema") if isinstance(attrs.get("schema"), dict) else {}
        dd = schema.get("datadog") if isinstance(schema.get("datadog"), dict) else {}
        return {
            "service_id": str(row.get("id") or ""),
            "name": first_present(attrs.get("name"), dd.get("service")),
            "app": dd.get("application"),
            "team": dd.get("team"),
            "lifecycle": dd.get("lifecycle"),
            "tier": dd.get("tier"),
            "updated_at": as_iso8601(first_present(attrs.get("meta", {}).get("last-modified") if isinstance(attrs.get("meta"), dict) else None, row.get("id"))),
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
