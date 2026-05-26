"""Databricks governance and workspace metadata connector."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.databricks_governance.databricks_governance_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
    first_present,
)


class DatabricksGovernanceLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        account_id = options.get("account_id")
        client_id = options.get("client_id")
        client_secret = options.get("client_secret")
        if not account_id or not client_id or not client_secret:
            raise ValueError("databricks_governance requires account_id, client_id, and client_secret")

        self.default_workspace_id = options.get("workspace_id")
        self.page_size = int(options.get("page_size", "200"))
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://accounts.cloud.databricks.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "X-Databricks-Account-Id": account_id,
                "X-Databricks-Client-Id": client_id,
                "X-Databricks-Client-Secret": client_secret,
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

        if table_name == "account_users":
            records = self._read_collection("/api/2.0/accounts/users", "users", self._map_user)
        elif table_name == "account_groups":
            records = self._read_collection("/api/2.0/accounts/groups", "groups", self._map_group)
        elif table_name == "group_memberships":
            records = self._read_collection("/api/2.0/accounts/group-memberships", "memberships", self._map_group_membership)
        elif table_name == "service_principals":
            records = self._read_collection("/api/2.0/accounts/service-principals", "service_principals", self._map_service_principal)
        elif table_name == "workspaces":
            records = self._read_collection("/api/2.0/accounts/workspaces", "workspaces", self._map_workspace)
        elif table_name == "clusters":
            records = self._read_collection("/api/2.0/workspaces/clusters", "clusters", self._map_cluster)
        elif table_name == "jobs":
            records = self._read_collection("/api/2.1/workspaces/jobs", "jobs", self._map_job)
        elif table_name == "job_runs":
            records = self._read_collection("/api/2.1/workspaces/job-runs", "runs", self._map_job_run)
        elif table_name == "uc_catalogs":
            records = self._read_collection("/api/2.1/unity-catalog/catalogs", "catalogs", self._map_uc_catalog)
        elif table_name == "uc_schemas":
            records = self._read_collection("/api/2.1/unity-catalog/schemas", "schemas", self._map_uc_schema)
        elif table_name == "uc_tables":
            records = self._read_collection("/api/2.1/unity-catalog/tables", "tables", self._map_uc_table)
        elif table_name == "uc_grants":
            records = self._read_collection("/api/2.1/unity-catalog/grants", "grants", self._map_uc_grant)
        elif table_name == "audit_logs":
            records = self._read_collection("/api/2.0/workspaces/audit-logs", "events", self._map_audit_event)
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

    def _workspace(self, row: dict[str, Any]) -> str | None:
        return first_present(row.get("workspace_id"), row.get("workspaceId"), self.default_workspace_id)

    def _map_user(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "user_id": str(first_present(row.get("id"), row.get("user_id"), row.get("userId"), "")),
            "user_name": first_present(row.get("user_name"), row.get("userName"), row.get("userName")),
            "display_name": first_present(row.get("display_name"), row.get("displayName")),
            "active": str(first_present(row.get("active"), "")),
            "created_at": as_iso8601(first_present(row.get("created_at"), row.get("createdAt"))),
            "last_login_time": as_iso8601(first_present(row.get("last_login_time"), row.get("lastLoginTime"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("last_login_time"), row.get("lastLoginTime"), row.get("created_at"), row.get("createdAt"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_group(self, row: dict[str, Any]) -> dict[str, Any]:
        entitlements = row.get("entitlements") if isinstance(row.get("entitlements"), list) else []
        return {
            "group_id": str(first_present(row.get("id"), row.get("group_id"), row.get("groupId"), "")),
            "display_name": first_present(row.get("display_name"), row.get("displayName")),
            "external_id": first_present(row.get("external_id"), row.get("externalId")),
            "entitlements": json.dumps(entitlements, separators=(",", ":"), default=str) if entitlements else None,
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_group_membership(self, row: dict[str, Any]) -> dict[str, Any]:
        group_id = first_present(row.get("group_id"), row.get("groupId"))
        member_id = first_present(row.get("member_id"), row.get("memberId"))
        member_type = first_present(row.get("member_type"), row.get("memberType"))
        membership_id = first_present(row.get("membership_id"), row.get("id"), self._stable_id(group_id, member_id, member_type))
        return {
            "membership_id": str(membership_id or ""),
            "group_id": group_id,
            "member_id": member_id,
            "member_type": member_type,
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"), row.get("groupId"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_service_principal(self, row: dict[str, Any]) -> dict[str, Any]:
        entitlements = row.get("entitlements") if isinstance(row.get("entitlements"), list) else []
        return {
            "service_principal_id": str(first_present(row.get("id"), row.get("service_principal_id"), row.get("servicePrincipalId"), "")),
            "application_id": first_present(row.get("application_id"), row.get("applicationId")),
            "display_name": first_present(row.get("display_name"), row.get("displayName")),
            "active": str(first_present(row.get("active"), "")),
            "entitlements": json.dumps(entitlements, separators=(",", ":"), default=str) if entitlements else None,
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_workspace(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "workspace_id": str(first_present(row.get("workspace_id"), row.get("workspaceId"), row.get("id"), "")),
            "workspace_name": first_present(row.get("workspace_name"), row.get("workspaceName")),
            "deployment_name": first_present(row.get("deployment_name"), row.get("deploymentName")),
            "workspace_url": first_present(row.get("workspace_url"), row.get("workspaceUrl")),
            "location": row.get("location"),
            "pricing_tier": first_present(row.get("pricing_tier"), row.get("pricingTier")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("creation_time"), row.get("creationTime"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_cluster(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "cluster_id": str(first_present(row.get("cluster_id"), row.get("clusterId"), row.get("id"), "")),
            "workspace_id": self._workspace(row),
            "cluster_name": first_present(row.get("cluster_name"), row.get("clusterName")),
            "state": row.get("state"),
            "node_type_id": first_present(row.get("node_type_id"), row.get("nodeTypeId")),
            "spark_version": first_present(row.get("spark_version"), row.get("sparkVersion")),
            "autotermination_minutes": str(first_present(row.get("autotermination_minutes"), row.get("autoterminationMinutes"), "")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("last_state_loss_time"), row.get("lastStateLossTime"), row.get("cluster_id"), row.get("clusterId"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_job(self, row: dict[str, Any]) -> dict[str, Any]:
        schedule = row.get("schedule") if isinstance(row.get("schedule"), dict) else {}
        return {
            "job_id": str(first_present(row.get("job_id"), row.get("jobId"), row.get("id"), "")),
            "workspace_id": self._workspace(row),
            "name": row.get("name"),
            "creator_user_name": first_present(row.get("creator_user_name"), row.get("creatorUserName")),
            "schedule": json.dumps(schedule, separators=(",", ":"), default=str) if schedule else None,
            "max_concurrent_runs": str(first_present(row.get("max_concurrent_runs"), row.get("maxConcurrentRuns"), "")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_time"), row.get("createdTime"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_job_run(self, row: dict[str, Any]) -> dict[str, Any]:
        state = row.get("state") if isinstance(row.get("state"), dict) else {}
        return {
            "run_id": str(first_present(row.get("run_id"), row.get("runId"), row.get("id"), "")),
            "job_id": str(first_present(row.get("job_id"), row.get("jobId"), "")),
            "workspace_id": self._workspace(row),
            "run_name": first_present(row.get("run_name"), row.get("runName")),
            "life_cycle_state": first_present(state.get("life_cycle_state"), state.get("life_cycle_state"), state.get("lifeCycleState"), row.get("lifeCycleState")),
            "result_state": first_present(state.get("result_state"), state.get("resultState"), row.get("resultState")),
            "start_time": as_iso8601(first_present(row.get("start_time"), row.get("startTime"))),
            "end_time": as_iso8601(first_present(row.get("end_time"), row.get("endTime"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("end_time"), row.get("endTime"), row.get("start_time"), row.get("startTime"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_uc_catalog(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "catalog_name": str(first_present(row.get("name"), row.get("catalog_name"), "")),
            "owner": row.get("owner"),
            "comment": row.get("comment"),
            "isolation_mode": first_present(row.get("isolation_mode"), row.get("isolationMode")),
            "storage_root": first_present(row.get("storage_root"), row.get("storageRoot")),
            "created_at": as_iso8601(first_present(row.get("created_at"), row.get("createdAt"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("updated_at"), row.get("created_at"), row.get("createdAt"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_uc_schema(self, row: dict[str, Any]) -> dict[str, Any]:
        catalog_name = first_present(row.get("catalog_name"), row.get("catalogName"))
        schema_name = first_present(row.get("name"), row.get("schema_name"), row.get("schemaName"))
        schema_id = first_present(row.get("schema_id"), row.get("id"), self._stable_id(catalog_name, schema_name))
        return {
            "schema_id": str(schema_id or ""),
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "owner": row.get("owner"),
            "comment": row.get("comment"),
            "storage_root": first_present(row.get("storage_root"), row.get("storageRoot")),
            "created_at": as_iso8601(first_present(row.get("created_at"), row.get("createdAt"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_at"), row.get("createdAt"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_uc_table(self, row: dict[str, Any]) -> dict[str, Any]:
        catalog_name = first_present(row.get("catalog_name"), row.get("catalogName"))
        schema_name = first_present(row.get("schema_name"), row.get("schemaName"))
        table_name = first_present(row.get("name"), row.get("table_name"), row.get("tableName"))
        table_id = first_present(row.get("table_id"), row.get("id"), self._stable_id(catalog_name, schema_name, table_name))
        return {
            "table_id": str(table_id or ""),
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "table_type": first_present(row.get("table_type"), row.get("tableType")),
            "data_source_format": first_present(row.get("data_source_format"), row.get("dataSourceFormat")),
            "storage_location": first_present(row.get("storage_location"), row.get("storageLocation")),
            "created_at": as_iso8601(first_present(row.get("created_at"), row.get("createdAt"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_at"), row.get("createdAt"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_uc_grant(self, row: dict[str, Any]) -> dict[str, Any]:
        securable_type = first_present(row.get("securable_type"), row.get("securableType"))
        securable_name = first_present(row.get("securable_name"), row.get("securableName"))
        principal = row.get("principal")
        privilege = row.get("privilege")
        grant_id = first_present(row.get("grant_id"), row.get("id"), self._stable_id(securable_type, securable_name, principal, privilege))
        return {
            "grant_id": str(grant_id or ""),
            "securable_type": securable_type,
            "securable_name": securable_name,
            "principal": principal,
            "privilege": privilege,
            "inherited": str(first_present(row.get("inherited"), "")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_audit_event(self, row: dict[str, Any]) -> dict[str, Any]:
        user_identity = row.get("user_identity") if isinstance(row.get("user_identity"), dict) else row.get("userIdentity")
        return {
            "audit_event_id": str(first_present(row.get("audit_event_id"), row.get("eventId"), row.get("id"), "")),
            "workspace_id": self._workspace(row),
            "event_time": as_iso8601(first_present(row.get("event_time"), row.get("eventTime"))),
            "service_name": first_present(row.get("service_name"), row.get("serviceName")),
            "action_name": first_present(row.get("action_name"), row.get("actionName")),
            "user_identity": json.dumps(user_identity, separators=(",", ":"), default=str) if user_identity is not None else None,
            "source_ip": first_present(row.get("source_ip"), row.get("sourceIp")),
            "response_status": first_present(row.get("response_status"), row.get("responseStatus")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("event_time"), row.get("eventTime"), row.get("id"))),
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
