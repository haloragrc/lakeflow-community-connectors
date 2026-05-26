"""Google Workspace Drive connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.google_workspace_common import (
    GoogleWorkspaceApiClient,
)
from databricks.labs.community_connector.sources.google_workspace_drive.google_workspace_drive_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import as_iso8601, first_present


_FOLDER_MIME = "application/vnd.google-apps.folder"


class GoogleWorkspaceDriveLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_token = options.get("access_token")
        if not access_token:
            raise ValueError("google_workspace_drive requires access_token")

        self.page_size = int(options.get("page_size", "200"))
        self.max_permission_files = int(options.get("max_permission_files", "100"))
        self.client = GoogleWorkspaceApiClient(
            base_url=options.get("base_url", "https://www.googleapis.com"),
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

        if table_name == "files":
            rows = self._list_files()
            records = [self._map_file(r) for r in rows if r.get("mimeType") != _FOLDER_MIME]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "folders":
            rows = self._list_files()
            records = [self._map_folder(r) for r in rows if r.get("mimeType") == _FOLDER_MIME]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "permissions":
            files = self._list_files()[: self.max_permission_files]
            records: list[dict[str, Any]] = []
            for file_row in files:
                file_id = file_row.get("id")
                if not file_id:
                    continue
                perms = self.client.paged_list(
                    f"/drive/v3/files/{file_id}/permissions",
                    records_key="permissions",
                    params={
                        "supportsAllDrives": "true",
                        "pageSize": self.page_size,
                    },
                )
                records.extend(self._map_permission(file_id, p) for p in perms)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "drives":
            rows = self.client.paged_list(
                "/drive/v3/drives",
                records_key="drives",
                params={"useDomainAdminAccess": "true", "pageSize": self.page_size},
            )
            records = [self._map_drive(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _list_files(self) -> list[dict[str, Any]]:
        return self.client.paged_list(
            "/drive/v3/files",
            records_key="files",
            params={
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
                "pageSize": self.page_size,
                "fields": "nextPageToken,files(id,name,mimeType,driveId,owners(emailAddress),webViewLink,createdTime,modifiedTime)",
            },
        )

    def _map_file(self, row: dict[str, Any]) -> dict[str, Any]:
        owners = row.get("owners") if isinstance(row.get("owners"), list) else []
        owner_emails = [o.get("emailAddress") for o in owners if isinstance(o, dict) and o.get("emailAddress")]
        return {
            "file_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "mime_type": row.get("mimeType"),
            "drive_id": row.get("driveId"),
            "owners": ",".join(owner_emails) if owner_emails else None,
            "web_view_link": row.get("webViewLink"),
            "created_time": as_iso8601(row.get("createdTime")),
            "modified_time": as_iso8601(row.get("modifiedTime")),
            "updated_at": as_iso8601(first_present(row.get("modifiedTime"), row.get("createdTime"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_folder(self, row: dict[str, Any]) -> dict[str, Any]:
        owners = row.get("owners") if isinstance(row.get("owners"), list) else []
        owner_emails = [o.get("emailAddress") for o in owners if isinstance(o, dict) and o.get("emailAddress")]
        return {
            "folder_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "drive_id": row.get("driveId"),
            "owners": ",".join(owner_emails) if owner_emails else None,
            "web_view_link": row.get("webViewLink"),
            "created_time": as_iso8601(row.get("createdTime")),
            "modified_time": as_iso8601(row.get("modifiedTime")),
            "updated_at": as_iso8601(first_present(row.get("modifiedTime"), row.get("createdTime"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_permission(self, file_id: str, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "permission_id": f"{file_id}:{row.get('id','')}",
            "file_id": file_id,
            "permission_type": row.get("type"),
            "role": row.get("role"),
            "email_address": row.get("emailAddress"),
            "domain": row.get("domain"),
            "allow_file_discovery": str(row.get("allowFileDiscovery")) if row.get("allowFileDiscovery") is not None else None,
            "expiration_time": as_iso8601(row.get("expirationTime")),
            "updated_at": as_iso8601(first_present(row.get("expirationTime"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_drive(self, row: dict[str, Any]) -> dict[str, Any]:
        restrictions = row.get("restrictions") if isinstance(row.get("restrictions"), dict) else {}
        organizer_count = restrictions.get("organizerCount")
        return {
            "drive_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "hidden": str(row.get("hidden")) if row.get("hidden") is not None else None,
            "organizer_count": str(organizer_count) if organizer_count is not None else None,
            "created_time": as_iso8601(row.get("createdTime")),
            "updated_at": as_iso8601(first_present(row.get("createdTime"), row.get("id"))),
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
