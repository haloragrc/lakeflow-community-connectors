"""Google Workspace Identity connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.google_workspace_common import (
    GoogleWorkspaceApiClient,
)
from databricks.labs.community_connector.sources.google_workspace_identity.google_workspace_identity_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import as_iso8601, first_present


class GoogleWorkspaceIdentityLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_token = options.get("access_token")
        if not access_token:
            raise ValueError("google_workspace_identity requires access_token")

        self.customer = options.get("customer", "my_customer")
        self.page_size = int(options.get("page_size", "200"))
        self.client = GoogleWorkspaceApiClient(
            base_url=options.get("base_url", "https://admin.googleapis.com"),
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
            rows = self.client.paged_list(
                "/admin/directory/v1/users",
                records_key="users",
                params={"customer": self.customer, "maxResults": self.page_size, "orderBy": "email"},
            )
            records = [self._map_user(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "groups":
            rows = self.client.paged_list(
                "/admin/directory/v1/groups",
                records_key="groups",
                params={"customer": self.customer, "maxResults": self.page_size},
            )
            records = [self._map_group(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "memberships":
            groups = self.client.paged_list(
                "/admin/directory/v1/groups",
                records_key="groups",
                params={"customer": self.customer, "maxResults": self.page_size},
            )
            records: list[dict[str, Any]] = []
            for group in groups:
                group_id = first_present(group.get("id"), group.get("email"))
                if not group_id:
                    continue
                rows = self.client.paged_list(
                    f"/admin/directory/v1/groups/{group_id}/members",
                    records_key="members",
                    params={"maxResults": self.page_size},
                )
                records.extend(self._map_membership(group, m) for m in rows)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "roles":
            rows = self.client.paged_list(
                f"/admin/directory/v1/customer/{self.customer}/roles",
                records_key="items",
                params={"maxResults": self.page_size},
            )
            records = [self._map_role(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "role_assignments":
            rows = self.client.paged_list(
                f"/admin/directory/v1/customer/{self.customer}/roleassignments",
                records_key="items",
                params={"maxResults": self.page_size},
            )
            records = [self._map_role_assignment(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _map_user(self, row: dict[str, Any]) -> dict[str, Any]:
        name = row.get("name") if isinstance(row.get("name"), dict) else {}
        return {
            "user_id": str(row.get("id") or ""),
            "primary_email": row.get("primaryEmail"),
            "full_name": name.get("fullName"),
            "is_admin": str(row.get("isAdmin")) if row.get("isAdmin") is not None else None,
            "is_suspended": str(row.get("suspended")) if row.get("suspended") is not None else None,
            "org_unit_path": row.get("orgUnitPath"),
            "last_login_time": as_iso8601(row.get("lastLoginTime")),
            "creation_time": as_iso8601(row.get("creationTime")),
            "updated_at": as_iso8601(first_present(row.get("lastLoginTime"), row.get("creationTime"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_group(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "group_id": str(row.get("id") or ""),
            "email": row.get("email"),
            "name": row.get("name"),
            "description": row.get("description"),
            "admin_created": str(row.get("adminCreated")) if row.get("adminCreated") is not None else None,
            "direct_members_count": row.get("directMembersCount"),
            "updated_at": as_iso8601(first_present(row.get("etag"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_membership(self, group: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
        group_id = first_present(group.get("id"), group.get("email"))
        member_id = first_present(row.get("id"), row.get("email"))
        return {
            "membership_id": f"{group_id}:{member_id}",
            "group_id": str(group_id) if group_id is not None else None,
            "member_id": str(member_id) if member_id is not None else None,
            "member_email": row.get("email"),
            "member_type": row.get("type"),
            "role": row.get("role"),
            "status": row.get("status"),
            "updated_at": as_iso8601(first_present(row.get("etag"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_role(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "role_id": str(row.get("roleId") or ""),
            "role_name": row.get("roleName"),
            "role_description": row.get("roleDescription"),
            "is_system_role": str(row.get("isSystemRole")) if row.get("isSystemRole") is not None else None,
            "is_super_admin_role": str(row.get("isSuperAdminRole")) if row.get("isSuperAdminRole") is not None else None,
            "updated_at": as_iso8601(first_present(row.get("etag"), row.get("roleId"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_role_assignment(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "role_assignment_id": str(row.get("roleAssignmentId") or ""),
            "role_id": str(row.get("roleId")) if row.get("roleId") is not None else None,
            "assigned_to": row.get("assignedTo"),
            "assignee_type": row.get("assigneeType"),
            "scope_type": row.get("scopeType"),
            "org_unit_id": row.get("orgUnitId"),
            "condition": row.get("condition"),
            "updated_at": as_iso8601(first_present(row.get("etag"), row.get("roleAssignmentId"))),
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
