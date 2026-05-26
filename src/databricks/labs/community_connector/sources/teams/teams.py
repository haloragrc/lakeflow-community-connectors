"""Microsoft Teams (Graph) connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.high_value_common import as_iso8601, first_present
from databricks.labs.community_connector.sources.teams.teams_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.google_workspace_common import GoogleWorkspaceApiClient


class TeamsLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_token = options.get("access_token")
        if not access_token:
            raise ValueError("teams requires access_token")

        self.max_teams = int(options.get("max_teams", "100"))
        self.max_channels = int(options.get("max_channels", "100"))
        self.client = GoogleWorkspaceApiClient(
            base_url=options.get("base_url", "https://graph.microsoft.com"),
            access_token=access_token,
            timeout_seconds=int(options.get("timeout_seconds", "30")),
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
            rows = self.client.paged_list("/v1.0/users", records_key="value")
            records = [self._map_user(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "teams":
            rows = self.client.paged_list("/v1.0/teams", records_key="value")
            records = [self._map_team(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "channels":
            teams = self.client.paged_list("/v1.0/teams", records_key="value")
            records: list[dict[str, Any]] = []
            for team in teams[: self.max_teams]:
                tid = team.get("id")
                if not tid:
                    continue
                rows = self.client.paged_list(f"/v1.0/teams/{tid}/channels", records_key="value")
                records.extend(self._map_channel(str(tid), r) for r in rows[: self.max_channels])
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "messages":
            records = self._collect_messages()
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "files":
            message_records = self._collect_messages(raw=True)
            files: list[dict[str, Any]] = []
            for item in message_records:
                attachments = item.get("attachments") if isinstance(item.get("attachments"), list) else []
                for att in attachments:
                    if not isinstance(att, dict):
                        continue
                    aid = first_present(att.get("id"), att.get("name"), att.get("contentUrl"))
                    if not aid:
                        continue
                    files.append(
                        {
                            "file_id": f"{item.get('message_id')}:{aid}",
                            "team_id": item.get("team_id"),
                            "channel_id": item.get("channel_id"),
                            "message_id": item.get("message_id"),
                            "name": att.get("name"),
                            "content_type": att.get("contentType"),
                            "content_url": att.get("contentUrl"),
                            "updated_at": item.get("updated_at"),
                            "raw_payload": json.dumps(att, separators=(",", ":"), default=str),
                        }
                    )
            return self._finalize_cdc(files, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _collect_messages(self, raw: bool = False) -> list[dict[str, Any]]:
        teams = self.client.paged_list("/v1.0/teams", records_key="value")
        records: list[dict[str, Any]] = []
        for team in teams[: self.max_teams]:
            tid = team.get("id")
            if not tid:
                continue
            channels = self.client.paged_list(f"/v1.0/teams/{tid}/channels", records_key="value")
            for channel in channels[: self.max_channels]:
                cid = channel.get("id")
                if not cid:
                    continue
                messages = self.client.paged_list(f"/v1.0/teams/{tid}/channels/{cid}/messages", records_key="value")
                for msg in messages:
                    mapped = self._map_message(str(tid), str(cid), msg)
                    if raw:
                        mapped["attachments"] = msg.get("attachments")
                    records.append(mapped)
        return records

    def _map_user(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "user_id": str(row.get("id") or ""),
            "user_principal_name": row.get("userPrincipalName"),
            "display_name": row.get("displayName"),
            "mail": row.get("mail"),
            "account_enabled": str(row.get("accountEnabled")) if row.get("accountEnabled") is not None else None,
            "created_at": as_iso8601(row.get("createdDateTime")),
            "updated_at": as_iso8601(first_present(row.get("createdDateTime"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_team(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "team_id": str(row.get("id") or ""),
            "display_name": row.get("displayName"),
            "description": row.get("description"),
            "visibility": row.get("visibility"),
            "is_archived": str(row.get("isArchived")) if row.get("isArchived") is not None else None,
            "created_at": as_iso8601(row.get("createdDateTime")),
            "updated_at": as_iso8601(first_present(row.get("createdDateTime"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_channel(self, team_id: str, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "channel_id": str(row.get("id") or ""),
            "team_id": team_id,
            "display_name": row.get("displayName"),
            "membership_type": row.get("membershipType"),
            "description": row.get("description"),
            "created_at": as_iso8601(row.get("createdDateTime")),
            "updated_at": as_iso8601(first_present(row.get("createdDateTime"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_message(self, team_id: str, channel_id: str, row: dict[str, Any]) -> dict[str, Any]:
        from_user = ((row.get("from") or {}).get("user") or {}) if isinstance(row.get("from"), dict) else {}
        return {
            "message_id": str(row.get("id") or ""),
            "team_id": team_id,
            "channel_id": channel_id,
            "from_user_id": from_user.get("id"),
            "message_type": row.get("messageType"),
            "subject": row.get("subject"),
            "summary": row.get("summary"),
            "created_at": as_iso8601(row.get("createdDateTime")),
            "updated_at": as_iso8601(first_present(row.get("lastModifiedDateTime"), row.get("createdDateTime"), row.get("id"))),
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
