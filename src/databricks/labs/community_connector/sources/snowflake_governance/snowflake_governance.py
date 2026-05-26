"""Snowflake governance and audit API connector."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
    first_present,
)
from databricks.labs.community_connector.sources.snowflake_governance.snowflake_governance_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class SnowflakeGovernanceLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        account_identifier = options.get("account_identifier")
        user = options.get("user")
        password = options.get("password")
        if not account_identifier or not user or not password:
            raise ValueError("snowflake_governance requires account_identifier, user, and password")

        self.account_identifier = account_identifier
        self.page_size = int(options.get("page_size", "200"))
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://api.snowflakecomputing.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "X-Snowflake-Account": account_identifier,
                "X-Snowflake-User": user,
                "X-Snowflake-Password": password,
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

        if table_name == "accounts":
            records = self._read_collection("/v1/accounts", "accounts", self._map_account)
        elif table_name == "users":
            records = self._read_collection("/v1/users", "users", self._map_user)
        elif table_name == "roles":
            records = self._read_collection("/v1/roles", "roles", self._map_role)
        elif table_name == "role_grants":
            records = self._read_collection("/v1/rolegrants", "grants", self._map_role_grant)
        elif table_name == "warehouses":
            records = self._read_collection("/v1/warehouses", "warehouses", self._map_warehouse)
        elif table_name == "databases":
            records = self._read_collection("/v1/databases", "databases", self._map_database)
        elif table_name == "schemas":
            records = self._read_collection("/v1/schemas", "schemas", self._map_schema)
        elif table_name == "tables":
            records = self._read_collection("/v1/tables", "tables", self._map_table)
        elif table_name == "stages":
            records = self._read_collection("/v1/stages", "stages", self._map_stage)
        elif table_name == "masking_policies":
            records = self._read_collection("/v1/maskingpolicies", "policies", self._map_masking_policy)
        elif table_name == "network_policies":
            records = self._read_collection("/v1/networkpolicies", "policies", self._map_network_policy)
        elif table_name == "login_history":
            records = self._read_collection("/v1/loginhistory", "events", self._map_login_event)
        elif table_name == "query_history":
            records = self._read_collection("/v1/queryhistory", "queries", self._map_query)
        else:
            raise ValueError(f"Unsupported table: {table_name!r}")

        return self._finalize_cdc(records, start_offset, "updated_at")

    def _read_collection(
        self,
        path: str,
        records_key: str,
        mapper: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        token = None
        results: list[dict[str, Any]] = []
        for _ in range(100):
            params = {"page_size": self.page_size}
            if token:
                params["page_token"] = token
            body = self.client.request_json("GET", path, params=params)
            rows = self._extract_rows(body, records_key)
            results.extend(mapper(row) for row in rows if isinstance(row, dict))
            if not isinstance(body, dict):
                break
            next_token = body.get("next_page_token") or body.get("nextPageToken")
            if not next_token or next_token == token:
                break
            token = str(next_token)
        return results

    def _extract_rows(self, body: Any, records_key: str) -> list[dict[str, Any]]:
        if isinstance(body, list):
            return [r for r in body if isinstance(r, dict)]
        if isinstance(body, dict):
            rows = body.get(records_key)
            if isinstance(rows, list):
                return [r for r in rows if isinstance(r, dict)]
            data = body.get("data")
            if isinstance(data, list):
                return [r for r in data if isinstance(r, dict)]
        return []

    def _stable_id(self, *parts: Any) -> str:
        basis = "|".join(str(p or "") for p in parts)
        return hashlib.sha1(basis.encode("utf-8")).hexdigest()

    def _map_account(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "account_name": str(first_present(row.get("account_name"), row.get("name"), self.account_identifier, "")),
            "organization_name": first_present(row.get("organization_name"), row.get("organization")),
            "edition": row.get("edition"),
            "region": row.get("region"),
            "locator": row.get("locator"),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_on"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_user(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "user_name": str(first_present(row.get("name"), row.get("user_name"), "")),
            "login_name": first_present(row.get("login_name"), row.get("loginName")),
            "email": row.get("email"),
            "default_role": first_present(row.get("default_role"), row.get("defaultRole")),
            "disabled": str(first_present(row.get("disabled"), "")),
            "created_on": as_iso8601(first_present(row.get("created_on"), row.get("createdOn"))),
            "last_success_login": as_iso8601(first_present(row.get("last_success_login"), row.get("lastSuccessLogin"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("last_success_login"), row.get("lastSuccessLogin"), row.get("created_on"), row.get("createdOn"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_role(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "role_name": str(first_present(row.get("name"), row.get("role_name"), "")),
            "owner": row.get("owner"),
            "comment": row.get("comment"),
            "created_on": as_iso8601(first_present(row.get("created_on"), row.get("createdOn"))),
            "deleted_on": as_iso8601(first_present(row.get("deleted_on"), row.get("deletedOn"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("deleted_on"), row.get("deletedOn"), row.get("created_on"), row.get("createdOn"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_role_grant(self, row: dict[str, Any]) -> dict[str, Any]:
        role_name = first_present(row.get("role_name"), row.get("roleName"), row.get("name"))
        granted_to = first_present(row.get("granted_to"), row.get("grantedTo"))
        grantee_name = first_present(row.get("grantee_name"), row.get("granteeName"))
        grant_id = first_present(row.get("grant_id"), row.get("id"), self._stable_id(role_name, granted_to, grantee_name))
        return {
            "grant_id": str(grant_id or ""),
            "role_name": role_name,
            "granted_to": granted_to,
            "grantee_name": grantee_name,
            "grant_option": str(first_present(row.get("grant_option"), row.get("grantOption"), "")),
            "granted_on": as_iso8601(first_present(row.get("granted_on"), row.get("grantedOn"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("granted_on"), row.get("grantedOn"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_warehouse(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "warehouse_name": str(first_present(row.get("name"), row.get("warehouse_name"), "")),
            "state": row.get("state"),
            "size": first_present(row.get("size"), row.get("warehouse_size")),
            "type": row.get("type"),
            "auto_suspend": str(first_present(row.get("auto_suspend"), row.get("autoSuspend"), "")),
            "auto_resume": str(first_present(row.get("auto_resume"), row.get("autoResume"), "")),
            "created_on": as_iso8601(first_present(row.get("created_on"), row.get("createdOn"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_on"), row.get("createdOn"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_database(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "database_name": str(first_present(row.get("name"), row.get("database_name"), "")),
            "owner": row.get("owner"),
            "is_transient": str(first_present(row.get("is_transient"), row.get("isTransient"), "")),
            "retention_time": str(first_present(row.get("retention_time"), row.get("retentionTime"), "")),
            "created_on": as_iso8601(first_present(row.get("created_on"), row.get("createdOn"))),
            "comment": row.get("comment"),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_on"), row.get("createdOn"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_schema(self, row: dict[str, Any]) -> dict[str, Any]:
        database_name = first_present(row.get("database_name"), row.get("databaseName"))
        schema_name = first_present(row.get("name"), row.get("schema_name"), row.get("schemaName"))
        schema_id = first_present(row.get("schema_id"), row.get("id"), self._stable_id(database_name, schema_name))
        return {
            "schema_id": str(schema_id or ""),
            "database_name": database_name,
            "schema_name": schema_name,
            "owner": row.get("owner"),
            "is_managed_access": str(first_present(row.get("is_managed_access"), row.get("isManagedAccess"), "")),
            "created_on": as_iso8601(first_present(row.get("created_on"), row.get("createdOn"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_on"), row.get("createdOn"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_table(self, row: dict[str, Any]) -> dict[str, Any]:
        database_name = first_present(row.get("database_name"), row.get("databaseName"))
        schema_name = first_present(row.get("schema_name"), row.get("schemaName"))
        table_name = first_present(row.get("name"), row.get("table_name"), row.get("tableName"))
        table_id = first_present(row.get("table_id"), row.get("id"), self._stable_id(database_name, schema_name, table_name))
        return {
            "table_id": str(table_id or ""),
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "table_type": first_present(row.get("table_type"), row.get("tableType")),
            "row_count": str(first_present(row.get("row_count"), row.get("rowCount"), "")),
            "bytes": str(first_present(row.get("bytes"), row.get("bytes_on_disk"), "")),
            "created_on": as_iso8601(first_present(row.get("created_on"), row.get("createdOn"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("last_altered"), row.get("lastAltered"), row.get("created_on"), row.get("createdOn"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_stage(self, row: dict[str, Any]) -> dict[str, Any]:
        database_name = first_present(row.get("database_name"), row.get("databaseName"))
        schema_name = first_present(row.get("schema_name"), row.get("schemaName"))
        stage_name = first_present(row.get("name"), row.get("stage_name"), row.get("stageName"))
        stage_id = first_present(row.get("stage_id"), row.get("id"), self._stable_id(database_name, schema_name, stage_name))
        return {
            "stage_id": str(stage_id or ""),
            "database_name": database_name,
            "schema_name": schema_name,
            "stage_name": stage_name,
            "url": row.get("url"),
            "owner": row.get("owner"),
            "created_on": as_iso8601(first_present(row.get("created_on"), row.get("createdOn"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_on"), row.get("createdOn"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_masking_policy(self, row: dict[str, Any]) -> dict[str, Any]:
        database_name = first_present(row.get("database_name"), row.get("databaseName"))
        schema_name = first_present(row.get("schema_name"), row.get("schemaName"))
        policy_name = first_present(row.get("name"), row.get("policy_name"), row.get("policyName"))
        policy_id = first_present(row.get("policy_id"), row.get("id"), self._stable_id(database_name, schema_name, policy_name))
        return {
            "policy_id": str(policy_id or ""),
            "database_name": database_name,
            "schema_name": schema_name,
            "policy_name": policy_name,
            "owner": row.get("owner"),
            "signature": row.get("signature"),
            "created_on": as_iso8601(first_present(row.get("created_on"), row.get("createdOn"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("last_altered"), row.get("lastAltered"), row.get("created_on"), row.get("createdOn"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_network_policy(self, row: dict[str, Any]) -> dict[str, Any]:
        allowed = row.get("allowed_ip_list") if isinstance(row.get("allowed_ip_list"), list) else row.get("allowedIpList")
        blocked = row.get("blocked_ip_list") if isinstance(row.get("blocked_ip_list"), list) else row.get("blockedIpList")
        return {
            "network_policy_name": str(first_present(row.get("name"), row.get("network_policy_name"), "")),
            "allowed_ip_list": json.dumps(allowed, separators=(",", ":"), default=str) if allowed is not None else None,
            "blocked_ip_list": json.dumps(blocked, separators=(",", ":"), default=str) if blocked is not None else None,
            "comment": row.get("comment"),
            "created_on": as_iso8601(first_present(row.get("created_on"), row.get("createdOn"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_on"), row.get("createdOn"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_login_event(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "event_id": str(first_present(row.get("event_id"), row.get("eventId"), row.get("id"), "")),
            "event_timestamp": as_iso8601(first_present(row.get("event_timestamp"), row.get("eventTimestamp"))),
            "user_name": first_present(row.get("user_name"), row.get("userName")),
            "client_ip": first_present(row.get("client_ip"), row.get("clientIp")),
            "reported_client_type": first_present(row.get("reported_client_type"), row.get("reportedClientType")),
            "is_success": str(first_present(row.get("is_success"), row.get("isSuccess"), "")),
            "error_code": str(first_present(row.get("error_code"), row.get("errorCode"), "")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("event_timestamp"), row.get("eventTimestamp"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_query(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "query_id": str(first_present(row.get("query_id"), row.get("queryId"), row.get("id"), "")),
            "user_name": first_present(row.get("user_name"), row.get("userName")),
            "warehouse_name": first_present(row.get("warehouse_name"), row.get("warehouseName")),
            "database_name": first_present(row.get("database_name"), row.get("databaseName")),
            "schema_name": first_present(row.get("schema_name"), row.get("schemaName")),
            "execution_status": first_present(row.get("execution_status"), row.get("executionStatus")),
            "start_time": as_iso8601(first_present(row.get("start_time"), row.get("startTime"))),
            "end_time": as_iso8601(first_present(row.get("end_time"), row.get("endTime"))),
            "bytes_scanned": str(first_present(row.get("bytes_scanned"), row.get("bytesScanned"), "")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("end_time"), row.get("endTime"), row.get("start_time"), row.get("startTime"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _finalize_cdc(
        self, records: list[dict[str, Any]], start_offset: dict, cursor_field: str
    ) -> tuple[Iterator[dict], dict]:
        if not records:
            return iter([]), start_offset or {}
        current = start_offset.get("cursor") if start_offset else None
        max_cursor = current
        for record in records:
            value = record.get(cursor_field)
            if value and (max_cursor is None or str(value) > str(max_cursor)):
                max_cursor = value
        if max_cursor is None:
            max_cursor = self._init_time
        if current is not None and str(max_cursor) <= str(current):
            return iter([]), start_offset
        return iter(records), {"cursor": max_cursor}
