"""PagerDuty connector."""

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
from databricks.labs.community_connector.sources.pagerduty.pagerduty_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class PagerdutyLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_token = options.get("api_token")
        if not api_token:
            raise ValueError("pagerduty requires api_token")

        self.page_size = int(options.get("page_size", "100"))
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://api.pagerduty.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token}",
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

        if table_name == "users":
            rows = self._paged_key("/users", "users")
            records = [self._map_user(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "services":
            rows = self._paged_key("/services", "services")
            records = [self._map_service(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "incidents":
            rows = self._paged_key("/incidents", "incidents")
            records = [self._map_incident(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "escalation_policies":
            rows = self._paged_key("/escalation_policies", "escalation_policies")
            records = [self._map_escalation_policy(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "oncalls":
            rows = self._paged_key("/oncalls", "oncalls")
            records = [self._map_oncall(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _paged_key(self, path: str, records_key: str) -> list[dict[str, Any]]:
        offset = 0
        out: list[dict[str, Any]] = []
        for _ in range(1000):
            body = self.client.request_json(
                "GET",
                path,
                params={"limit": self.page_size, "offset": offset},
            )
            rows = body.get(records_key) if isinstance(body, dict) else None
            rows = [x for x in rows if isinstance(x, dict)] if isinstance(rows, list) else []
            out.extend(rows)
            more = body.get("more") if isinstance(body, dict) else False
            if not more:
                break
            offset += self.page_size
        return out

    def _map_user(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "user_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "email": row.get("email"),
            "role": row.get("role"),
            "job_title": row.get("job_title"),
            "time_zone": row.get("time_zone"),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_service(self, row: dict[str, Any]) -> dict[str, Any]:
        ep = row.get("escalation_policy") if isinstance(row.get("escalation_policy"), dict) else {}
        return {
            "service_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "status": row.get("status"),
            "auto_resolve_timeout": str(row.get("auto_resolve_timeout")) if row.get("auto_resolve_timeout") is not None else None,
            "acknowledgement_timeout": str(row.get("acknowledgement_timeout")) if row.get("acknowledgement_timeout") is not None else None,
            "escalation_policy_id": ep.get("id"),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_incident(self, row: dict[str, Any]) -> dict[str, Any]:
        service = row.get("service") if isinstance(row.get("service"), dict) else {}
        return {
            "incident_id": str(row.get("id") or ""),
            "title": row.get("title"),
            "status": row.get("status"),
            "urgency": row.get("urgency"),
            "service_id": service.get("id"),
            "created_at": as_iso8601(row.get("created_at")),
            "last_status_change_at": as_iso8601(row.get("last_status_change_at")),
            "updated_at": as_iso8601(first_present(row.get("last_status_change_at"), row.get("updated_at"), row.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_escalation_policy(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "escalation_policy_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "description": row.get("description"),
            "num_loops": str(row.get("num_loops")) if row.get("num_loops") is not None else None,
            "on_call_handoff_notifications": str(row.get("on_call_handoff_notifications")) if row.get("on_call_handoff_notifications") is not None else None,
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_oncall(self, row: dict[str, Any]) -> dict[str, Any]:
        user = row.get("user") if isinstance(row.get("user"), dict) else {}
        schedule = row.get("schedule") if isinstance(row.get("schedule"), dict) else {}
        ep = row.get("escalation_policy") if isinstance(row.get("escalation_policy"), dict) else {}
        oid = first_present(row.get("id"), f"{user.get('id')}:{schedule.get('id')}:{row.get('start')}")
        return {
            "oncall_id": str(oid) if oid is not None else "",
            "user_id": user.get("id"),
            "schedule_id": schedule.get("id"),
            "escalation_policy_id": ep.get("id"),
            "start": as_iso8601(row.get("start")),
            "end": as_iso8601(row.get("end")),
            "updated_at": as_iso8601(first_present(row.get("end"), row.get("start"), oid)),
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
