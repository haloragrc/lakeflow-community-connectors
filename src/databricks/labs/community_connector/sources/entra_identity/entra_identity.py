"""Microsoft Entra ID connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
)
from databricks.labs.community_connector.sources.entra_identity.entra_identity_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class EntraIdentityLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_token = options.get("access_token")
        if not access_token:
            raise ValueError("entra_identity requires access_token")

        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://graph.microsoft.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={"Accept": "application/json", "Authorization": f"Bearer {access_token}"},
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
            rows = self._value(self.client.request_json("GET", "/v1.0/users"))
            records = [self._map_user(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "last_updated")
        if table_name == "groups":
            rows = self._value(self.client.request_json("GET", "/v1.0/groups"))
            records = [self._map_group(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "last_updated")
        if table_name == "memberships":
            return self._read_memberships()
        if table_name == "service_principals":
            rows = self._value(self.client.request_json("GET", "/v1.0/servicePrincipals"))
            records = [self._map_service_principal(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "last_updated")
        if table_name == "app_role_assignments":
            rows = self._value(self.client.request_json("GET", "/v1.0/servicePrincipals/appRoleAssignedTo"))
            records = [self._map_app_role_assignment(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "created_at")
        if table_name == "directory_roles":
            rows = self._value(self.client.request_json("GET", "/v1.0/directoryRoles"))
            records = [self._map_directory_role(r) for r in rows]
            return iter(records), {}
        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_memberships(self) -> tuple[Iterator[dict], dict]:
        groups = self._value(self.client.request_json("GET", "/v1.0/groups"))
        records = []
        for group in groups:
            group_id = group.get("id")
            if not group_id:
                continue
            members = self._value(self.client.request_json("GET", f"/v1.0/groups/{group_id}/members"))
            for member in members:
                member_id = member.get("id")
                if not member_id:
                    continue
                records.append(
                    {
                        "membership_id": f"{group_id}:{member_id}",
                        "group_id": group_id,
                        "member_id": member_id,
                        "member_type": member.get("@odata.type"),
                        "member_display_name": member.get("displayName"),
                        "raw_payload": json.dumps(member, separators=(",", ":"), default=str),
                    }
                )
        return iter(records), {}

    def _map_user(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "user_id": row.get("id") or "",
            "user_principal_name": row.get("userPrincipalName"),
            "display_name": row.get("displayName"),
            "mail": row.get("mail"),
            "account_enabled": str(row.get("accountEnabled")) if row.get("accountEnabled") is not None else None,
            "created_at": as_iso8601(row.get("createdDateTime")),
            "last_updated": as_iso8601(row.get("createdDateTime")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }
        return out

    def _map_group(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "group_id": row.get("id") or "",
            "display_name": row.get("displayName"),
            "mail_nickname": row.get("mailNickname"),
            "mail_enabled": str(row.get("mailEnabled")) if row.get("mailEnabled") is not None else None,
            "security_enabled": str(row.get("securityEnabled")) if row.get("securityEnabled") is not None else None,
            "created_at": as_iso8601(row.get("createdDateTime")),
            "last_updated": as_iso8601(row.get("createdDateTime")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }
        return out

    def _map_service_principal(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "service_principal_id": row.get("id") or "",
            "app_id": row.get("appId"),
            "display_name": row.get("displayName"),
            "app_owner_org_id": row.get("appOwnerOrganizationId"),
            "account_enabled": str(row.get("accountEnabled")) if row.get("accountEnabled") is not None else None,
            "created_at": as_iso8601(row.get("createdDateTime")),
            "last_updated": as_iso8601(row.get("createdDateTime")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }
        return out

    def _map_app_role_assignment(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "assignment_id": row.get("id") or "",
            "principal_id": row.get("principalId"),
            "resource_id": row.get("resourceId"),
            "app_role_id": row.get("appRoleId"),
            "principal_type": row.get("principalType"),
            "created_at": as_iso8601(row.get("createdDateTime")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }
        return out

    def _map_directory_role(self, row: dict[str, Any]) -> dict[str, Any]:
        out = {
            "directory_role_id": row.get("id") or "",
            "display_name": row.get("displayName"),
            "description": row.get("description"),
            "template_id": row.get("roleTemplateId"),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }
        return out

    def _value(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict) and isinstance(payload.get("value"), list):
            return [x for x in payload["value"] if isinstance(x, dict)]
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        return []

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
