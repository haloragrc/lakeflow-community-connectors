"""KnowBe4 reporting API connector."""

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
from databricks.labs.community_connector.sources.knowbe4.knowbe4_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class Knowbe4LakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_key = options.get("api_key")
        if not api_key:
            raise ValueError("knowbe4 requires api_key")

        self.page_size = int(options.get("page_size", "500"))
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://us.api.knowbe4.com/v1"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
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
            rows = self._paged_list("/users")
            records = [self._map_user(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "groups":
            rows = self._paged_list("/groups")
            records = [self._map_group(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "phishing_campaigns":
            rows = self._paged_list("/phishing/campaigns")
            records = [self._map_phishing_campaign(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "training_campaigns":
            rows = self._paged_list("/training/campaigns")
            records = [self._map_training_campaign(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "training_enrollments":
            rows = self._paged_list("/training/enrollments")
            records = [self._map_training_enrollment(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _paged_list(self, path: str) -> list[dict[str, Any]]:
        page = 1
        out: list[dict[str, Any]] = []
        for _ in range(1000):
            body = self.client.request_json(
                "GET",
                path,
                params={"page": page, "per_page": self.page_size},
            )
            rows = [x for x in body if isinstance(x, dict)] if isinstance(body, list) else []
            out.extend(rows)
            if len(rows) < self.page_size:
                break
            page += 1
        return out

    def _map_user(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "user_id": str(row.get("id") or ""),
            "email": row.get("email"),
            "first_name": row.get("first_name"),
            "last_name": row.get("last_name"),
            "status": row.get("status"),
            "employee_number": row.get("employee_number"),
            "phish_prone_percentage": str(row.get("phish_prone_percentage")) if row.get("phish_prone_percentage") is not None else None,
            "joined_on": as_iso8601(row.get("joined_on")),
            "updated_at": as_iso8601(first_present(row.get("last_login"), row.get("joined_on"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_group(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "group_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "member_count": str(row.get("member_count")) if row.get("member_count") is not None else None,
            "status": row.get("status"),
            "updated_at": as_iso8601(first_present(row.get("created_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_phishing_campaign(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "campaign_id": str(row.get("campaign_id") or row.get("id") or ""),
            "name": row.get("name"),
            "status": row.get("status"),
            "send_duration": row.get("send_duration"),
            "last_phish_date": as_iso8601(row.get("last_phish_date")),
            "scheduled_at": as_iso8601(row.get("scheduled_at")),
            "updated_at": as_iso8601(first_present(row.get("last_phish_date"), row.get("scheduled_at"), row.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_training_campaign(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "training_campaign_id": str(row.get("campaign_id") or row.get("id") or ""),
            "name": row.get("name"),
            "status": row.get("status"),
            "start_date": as_iso8601(row.get("start_date")),
            "end_date": as_iso8601(row.get("end_date")),
            "duration_type": row.get("duration_type"),
            "updated_at": as_iso8601(first_present(row.get("end_date"), row.get("start_date"), row.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_training_enrollment(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "enrollment_id": str(row.get("enrollment_id") or row.get("id") or ""),
            "user_id": str(row.get("user_id")) if row.get("user_id") is not None else None,
            "training_campaign_id": str(row.get("campaign_id")) if row.get("campaign_id") is not None else None,
            "module_name": row.get("module_name"),
            "status": row.get("status"),
            "enrollment_date": as_iso8601(row.get("enrollment_date")),
            "completion_date": as_iso8601(row.get("completion_date")),
            "updated_at": as_iso8601(first_present(row.get("completion_date"), row.get("enrollment_date"), row.get("created_at"))),
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
