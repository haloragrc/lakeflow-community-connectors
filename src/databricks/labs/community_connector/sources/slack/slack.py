"""Slack Web API connector."""

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
from databricks.labs.community_connector.sources.slack.slack_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class SlackLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_token = options.get("api_token")
        if not api_token:
            raise ValueError("slack requires api_token")

        self.page_size = int(options.get("page_size", "200"))
        self.max_channels = int(options.get("max_channels", "100"))
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://slack.com/api"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {api_token}",
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
            rows = self._paged_list("/users.list", "members")
            records = [self._map_user(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "channels":
            rows = self._paged_list("/conversations.list", "channels", params={"types": "public_channel,private_channel"})
            records = [self._map_channel(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "messages":
            channels = self._paged_list("/conversations.list", "channels", params={"types": "public_channel,private_channel"})
            records: list[dict[str, Any]] = []
            for ch in channels[: self.max_channels]:
                cid = ch.get("id")
                if not cid:
                    continue
                msgs = self._paged_list("/conversations.history", "messages", params={"channel": cid})
                records.extend(self._map_message(cid, m) for m in msgs)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "files":
            rows = self._paged_list("/files.list", "files")
            records = [self._map_file(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "usergroups":
            body = self.client.request_json("GET", "/usergroups.list")
            rows = body.get("usergroups") if isinstance(body, dict) and isinstance(body.get("usergroups"), list) else []
            records = [self._map_usergroup(r) for r in rows if isinstance(r, dict)]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _paged_list(self, path: str, key: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = dict(params or {})
        params.setdefault("limit", self.page_size)
        cursor = ""
        out: list[dict[str, Any]] = []
        for _ in range(1000):
            if cursor:
                params["cursor"] = cursor
            body = self.client.request_json("GET", path, params=params)
            rows = body.get(key) if isinstance(body, dict) else None
            rows = [x for x in rows if isinstance(x, dict)] if isinstance(rows, list) else []
            out.extend(rows)
            metadata = body.get("response_metadata") if isinstance(body, dict) and isinstance(body.get("response_metadata"), dict) else {}
            cursor = metadata.get("next_cursor") if isinstance(metadata.get("next_cursor"), str) else ""
            if not cursor:
                break
        return out

    def _map_user(self, row: dict[str, Any]) -> dict[str, Any]:
        profile = row.get("profile") if isinstance(row.get("profile"), dict) else {}
        return {
            "user_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "real_name": profile.get("real_name"),
            "email": profile.get("email"),
            "is_admin": str(row.get("is_admin")) if row.get("is_admin") is not None else None,
            "is_bot": str(row.get("is_bot")) if row.get("is_bot") is not None else None,
            "updated_at": as_iso8601(first_present(row.get("updated"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_channel(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "channel_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "is_private": str(row.get("is_private")) if row.get("is_private") is not None else None,
            "is_archived": str(row.get("is_archived")) if row.get("is_archived") is not None else None,
            "is_member": str(row.get("is_member")) if row.get("is_member") is not None else None,
            "created": as_iso8601(row.get("created")),
            "updated_at": as_iso8601(first_present(row.get("updated"), row.get("created"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_message(self, channel_id: str, row: dict[str, Any]) -> dict[str, Any]:
        ts = row.get("ts")
        return {
            "message_id": f"{channel_id}:{ts}",
            "channel_id": channel_id,
            "user_id": row.get("user"),
            "text": row.get("text"),
            "subtype": row.get("subtype"),
            "thread_ts": row.get("thread_ts"),
            "ts": str(ts) if ts is not None else None,
            "updated_at": as_iso8601(first_present(row.get("ts"), row.get("thread_ts"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_file(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "file_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "mimetype": row.get("mimetype"),
            "filetype": row.get("filetype"),
            "size": str(row.get("size")) if row.get("size") is not None else None,
            "user_id": row.get("user"),
            "created": as_iso8601(row.get("created")),
            "updated_at": as_iso8601(first_present(row.get("updated"), row.get("created"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_usergroup(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "usergroup_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "handle": row.get("handle"),
            "description": row.get("description"),
            "is_usergroup": str(row.get("is_usergroup")) if row.get("is_usergroup") is not None else None,
            "date_update": as_iso8601(row.get("date_update")),
            "updated_at": as_iso8601(first_present(row.get("date_update"), row.get("date_create"), row.get("id"))),
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
