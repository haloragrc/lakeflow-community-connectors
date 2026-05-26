"""Microsoft Sentinel bounded workspace connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.high_value_common import as_iso8601, first_present, HighValueApiClient
from databricks.labs.community_connector.sources.sentinel_bounded.sentinel_bounded_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class SentinelBoundedLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_token = options.get("access_token")
        subscription_id = options.get("subscription_id")
        resource_group = options.get("resource_group")
        workspace_name = options.get("workspace_name")
        if not access_token or not subscription_id or not resource_group or not workspace_name:
            raise ValueError(
                "sentinel_bounded requires access_token, subscription_id, resource_group, and workspace_name"
            )

        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.workspace_name = workspace_name
        self.base_path = (
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
            f"/providers/Microsoft.OperationalInsights/workspaces/{workspace_name}"
            f"/providers/Microsoft.SecurityInsights"
        )
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://management.azure.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}",
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

        if table_name == "incidents":
            rows = self._paged_value(f"{self.base_path}/incidents", {"api-version": "2025-06-01"})
            records = [self._map_incident(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "alerts":
            rows = self._paged_value(f"{self.base_path}/alerts", {"api-version": "2025-06-01"})
            records = [self._map_alert(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "analytics_rules":
            rows = self._paged_value(f"{self.base_path}/alertRules", {"api-version": "2025-06-01"})
            records = [self._map_rule(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "entities":
            rows = self._paged_value(f"{self.base_path}/entities", {"api-version": "2025-06-01"})
            records = [self._map_entity(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "bookmarks":
            rows = self._paged_value(f"{self.base_path}/bookmarks", {"api-version": "2025-06-01"})
            records = [self._map_bookmark(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _paged_value(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        next_url: str | None = path
        next_params: dict[str, Any] | None = params

        for _ in range(1000):
            if next_url is None:
                break
            if next_url.startswith("http"):
                rel = next_url.replace(self.client.base_url, "")
                body = self.client.request_json("GET", rel)
            else:
                body = self.client.request_json("GET", next_url, params=next_params)
            rows = body.get("value") if isinstance(body, dict) else None
            if isinstance(rows, list):
                out.extend([x for x in rows if isinstance(x, dict)])
            next_url = body.get("nextLink") if isinstance(body, dict) else None
            next_params = None
            if not next_url:
                break

        return out

    def _map_incident(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        owner = props.get("owner") if isinstance(props.get("owner"), dict) else {}
        labels = props.get("labels") if isinstance(props.get("labels"), list) else []
        return {
            "incident_id": str(row.get("id") or ""),
            "title": props.get("title"),
            "severity": props.get("severity"),
            "status": props.get("status"),
            "owner": owner.get("email"),
            "labels": json.dumps(labels, separators=(",", ":")) if labels else None,
            "created_time_utc": as_iso8601(props.get("createdTimeUtc")),
            "last_modified_time_utc": as_iso8601(props.get("lastModifiedTimeUtc")),
            "updated_at": as_iso8601(first_present(props.get("lastModifiedTimeUtc"), props.get("createdTimeUtc"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_alert(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        return {
            "alert_id": str(row.get("id") or ""),
            "alert_display_name": props.get("alertDisplayName"),
            "severity": props.get("severity"),
            "status": props.get("status"),
            "provider_name": props.get("providerName"),
            "product_name": props.get("productName"),
            "start_time_utc": as_iso8601(props.get("startTimeUtc")),
            "end_time_utc": as_iso8601(props.get("endTimeUtc")),
            "updated_at": as_iso8601(first_present(props.get("endTimeUtc"), props.get("startTimeUtc"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_rule(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        tactics = props.get("tactics") if isinstance(props.get("tactics"), list) else []
        enabled = props.get("enabled")
        return {
            "rule_id": str(row.get("id") or ""),
            "display_name": props.get("displayName"),
            "enabled": str(enabled) if enabled is not None else None,
            "tactics": json.dumps(tactics, separators=(",", ":")) if tactics else None,
            "query_frequency": str(props.get("queryFrequency")) if props.get("queryFrequency") is not None else None,
            "query_period": str(props.get("queryPeriod")) if props.get("queryPeriod") is not None else None,
            "updated_at": as_iso8601(first_present(props.get("lastModifiedUtc"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_entity(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        etype = first_present(row.get("kind"), props.get("type"))
        name = first_present(props.get("name"), props.get("hostName"), props.get("accountName"))
        return {
            "entity_id": str(row.get("id") or ""),
            "entity_type": etype,
            "name": name,
            "properties": json.dumps(props, separators=(",", ":")) if props else None,
            "updated_at": as_iso8601(first_present(props.get("lastModifiedUtc"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_bookmark(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        created_by = props.get("createdBy") if isinstance(props.get("createdBy"), dict) else {}
        return {
            "bookmark_id": str(row.get("id") or ""),
            "display_name": props.get("displayName"),
            "created_by": first_present(created_by.get("email"), created_by.get("name")),
            "notes": props.get("notes"),
            "query": props.get("query"),
            "created": as_iso8601(props.get("created")),
            "updated_at": as_iso8601(first_present(props.get("updated"), props.get("created"), row.get("id"))),
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
