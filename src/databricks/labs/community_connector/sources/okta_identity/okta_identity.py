"""Okta identity connector."""

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
from databricks.labs.community_connector.sources.okta_identity.okta_identity_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class OktaIdentityLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_token = options.get("api_token")
        if not api_token:
            raise ValueError("okta_identity requires api_token")
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://example.okta.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={"Accept": "application/json", "Authorization": f"SSWS {api_token}"},
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
            rows = self.client.request_json("GET", "/api/v1/users")
            records = [self._map_user(r) for r in self._as_list(rows)]
            return self._finalize_cdc(records, start_offset, "last_updated")
        if table_name == "groups":
            rows = self.client.request_json("GET", "/api/v1/groups")
            records = [self._map_group(r) for r in self._as_list(rows)]
            return self._finalize_cdc(records, start_offset, "last_updated")
        if table_name == "memberships":
            return self._read_memberships()
        if table_name == "app_assignments":
            return self._read_app_assignments(start_offset)
        if table_name == "roles":
            return self._read_roles(start_offset)
        if table_name == "factors":
            return self._read_factors(start_offset)
        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_memberships(self) -> tuple[Iterator[dict], dict]:
        groups = self._as_list(self.client.request_json("GET", "/api/v1/groups"))
        records: list[dict[str, Any]] = []
        for group in groups:
            group_id = group.get("id")
            if not group_id:
                continue
            users = self._as_list(self.client.request_json("GET", f"/api/v1/groups/{group_id}/users"))
            for user in users:
                user_id = user.get("id")
                if not user_id:
                    continue
                rec = {
                    "membership_id": f"{group_id}:{user_id}",
                    "group_id": group_id,
                    "user_id": user_id,
                    "user_login": first_present((user.get("profile") or {}).get("login") if isinstance(user.get("profile"), dict) else None, user.get("login")),
                    "user_status": user.get("status"),
                    "raw_payload": json.dumps(user, separators=(",", ":"), default=str),
                }
                records.append(rec)
        return iter(records), {}

    def _read_app_assignments(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        apps = self._as_list(self.client.request_json("GET", "/api/v1/apps"))
        records: list[dict[str, Any]] = []
        for app in apps:
            app_id = app.get("id")
            if not app_id:
                continue
            assignments = self._as_list(self.client.request_json("GET", f"/api/v1/apps/{app_id}/users"))
            for assignment in assignments:
                principal_id = first_present(assignment.get("id"), assignment.get("credentials", {}).get("userName") if isinstance(assignment.get("credentials"), dict) else None)
                assignment_id = f"{app_id}:{principal_id}"
                records.append(
                    {
                        "assignment_id": assignment_id,
                        "app_id": app_id,
                        "app_label": app.get("label"),
                        "principal_id": str(principal_id) if principal_id is not None else None,
                        "scope": first_present(assignment.get("scope"), "USER"),
                        "status": first_present(assignment.get("status"), app.get("status")),
                        "last_updated": as_iso8601(first_present(assignment.get("lastUpdated"), app.get("lastUpdated"))),
                        "raw_payload": json.dumps(assignment, separators=(",", ":"), default=str),
                    }
                )
        return self._finalize_cdc(records, start_offset, "last_updated")

    def _read_roles(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        rows = self._as_list(self.client.request_json("GET", "/api/v1/iam/roles"))
        records = []
        for row in rows:
            role_id = row.get("id")
            if not role_id:
                continue
            records.append(
                {
                    "role_id": role_id,
                    "role_type": first_present(row.get("type"), row.get("roleType")),
                    "label": first_present(row.get("label"), row.get("name")),
                    "status": row.get("status"),
                    "assignment_type": first_present(row.get("assignmentType"), "DIRECT"),
                    "last_updated": as_iso8601(first_present(row.get("lastUpdated"), row.get("created"))),
                    "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
                }
            )
        return self._finalize_cdc(records, start_offset, "last_updated")

    def _read_factors(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        users = self._as_list(self.client.request_json("GET", "/api/v1/users"))
        records: list[dict[str, Any]] = []
        for user in users:
            user_id = user.get("id")
            if not user_id:
                continue
            factors = self._as_list(self.client.request_json("GET", f"/api/v1/users/{user_id}/factors"))
            for factor in factors:
                factor_id = factor.get("id")
                if not factor_id:
                    continue
                records.append(
                    {
                        "factor_id": factor_id,
                        "user_id": user_id,
                        "factor_type": factor.get("factorType"),
                        "provider": factor.get("provider"),
                        "status": factor.get("status"),
                        "created": as_iso8601(factor.get("created")),
                        "last_updated": as_iso8601(first_present(factor.get("lastUpdated"), factor.get("created"))),
                        "raw_payload": json.dumps(factor, separators=(",", ":"), default=str),
                    }
                )
        return self._finalize_cdc(records, start_offset, "last_updated")

    def _map_user(self, row: dict[str, Any]) -> dict[str, Any]:
        profile = row.get("profile") if isinstance(row.get("profile"), dict) else {}
        out = {
            "user_id": row.get("id") or "",
            "status": row.get("status"),
            "login": profile.get("login"),
            "email": first_present(profile.get("email"), profile.get("login")),
            "display_name": first_present(profile.get("displayName"), profile.get("firstName")),
            "last_updated": as_iso8601(row.get("lastUpdated")),
            "created": as_iso8601(row.get("created")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }
        return out

    def _map_group(self, row: dict[str, Any]) -> dict[str, Any]:
        profile = row.get("profile") if isinstance(row.get("profile"), dict) else {}
        out = {
            "group_id": row.get("id") or "",
            "type": row.get("type"),
            "name": profile.get("name"),
            "description": profile.get("description"),
            "last_updated": as_iso8601(row.get("lastUpdated")),
            "created": as_iso8601(row.get("created")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }
        return out

    def _as_list(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            for key in ("items", "data", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]
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
