"""GCP inventory and governance API connector."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.gcp_inventory.gcp_inventory_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
    first_present,
)


class GcpInventoryLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        service_account_email = options.get("service_account_email")
        service_account_key = options.get("service_account_key")
        if not service_account_email or not service_account_key:
            raise ValueError("gcp_inventory requires service_account_email and service_account_key")

        self.project_id = options.get("project_id")
        self.page_size = int(options.get("page_size", "200"))
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://cloudresourcemanager.googleapis.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "X-Gcp-Service-Account": service_account_email,
                "X-Gcp-Service-Account-Key": service_account_key,
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

        if table_name == "organizations":
            records = self._read_collection("/v1/organizations", "organizations", self._map_organization)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "folders":
            records = self._read_collection("/v3/folders", "folders", self._map_folder)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "projects":
            records = self._read_collection("/v1/projects", "projects", self._map_project)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "iam_service_accounts":
            records = self._read_collection("/v1/iam/serviceAccounts", "accounts", self._map_service_account)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "iam_policy_bindings":
            records = self._read_collection("/v1/iam/policies", "bindings", self._map_policy_binding)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "compute_instances":
            records = self._read_collection("/v1/compute/instances", "instances", self._map_compute_instance)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "vpc_networks":
            records = self._read_collection("/v1/compute/networks", "networks", self._map_vpc_network)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "firewall_rules":
            records = self._read_collection("/v1/compute/firewalls", "firewalls", self._map_firewall_rule)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "storage_buckets":
            records = self._read_collection("/v1/storage/buckets", "buckets", self._map_storage_bucket)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "kms_crypto_keys":
            records = self._read_collection("/v1/kms/cryptokeys", "cryptoKeys", self._map_kms_key)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "logging_sinks":
            records = self._read_collection("/v2/logging/sinks", "sinks", self._map_logging_sink)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "scc_findings":
            records = self._read_collection("/v1/security/findings", "findings", self._map_scc_finding)
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_collection(
        self,
        path: str,
        records_key: str,
        mapper: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        next_token = None
        results: list[dict[str, Any]] = []

        for _ in range(100):
            params = {"pageSize": self.page_size}
            if next_token:
                params["pageToken"] = next_token
            body = self.client.request_json("GET", path, params=params)
            rows = self._extract_rows(body, records_key)
            results.extend(mapper(row) for row in rows if isinstance(row, dict))

            if not isinstance(body, dict):
                break
            maybe_token = body.get("nextPageToken") or body.get("next_page_token")
            if not maybe_token or maybe_token == next_token:
                break
            next_token = str(maybe_token)

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

    def _project(self, row: dict[str, Any]) -> str | None:
        return first_present(row.get("project_id"), row.get("projectId"), self.project_id)

    def _stable_binding_id(self, role: Any, members: Any, project_id: Any) -> str:
        basis = f"{project_id}|{role}|{json.dumps(members, sort_keys=True)}"
        return hashlib.sha1(basis.encode("utf-8")).hexdigest()

    def _map_organization(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "organization_id": str(first_present(row.get("name"), row.get("organizationId"), row.get("id"), "")),
            "display_name": first_present(row.get("displayName"), row.get("display_name")),
            "lifecycle_state": first_present(row.get("lifecycleState"), row.get("lifecycle_state")),
            "owner_directory_customer_id": first_present(row.get("owner", {}).get("directoryCustomerId") if isinstance(row.get("owner"), dict) else None, row.get("ownerDirectoryCustomerId")),
            "updated_at": as_iso8601(first_present(row.get("updateTime"), row.get("createTime"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_folder(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "folder_id": str(first_present(row.get("name"), row.get("folderId"), row.get("id"), "")),
            "display_name": first_present(row.get("displayName"), row.get("display_name")),
            "parent": row.get("parent"),
            "lifecycle_state": first_present(row.get("state"), row.get("lifecycleState"), row.get("lifecycle_state")),
            "create_time": as_iso8601(first_present(row.get("createTime"), row.get("create_time"))),
            "updated_at": as_iso8601(first_present(row.get("updateTime"), row.get("createTime"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_project(self, row: dict[str, Any]) -> dict[str, Any]:
        parent = row.get("parent") if isinstance(row.get("parent"), dict) else {}
        return {
            "project_id": str(first_present(row.get("projectId"), row.get("project_id"), row.get("project_id"), row.get("projectNumber"), "")),
            "project_number": str(first_present(row.get("projectNumber"), "")),
            "name": first_present(row.get("name"), row.get("displayName")),
            "parent": first_present(parent.get("id"), parent.get("type"), row.get("parentId")),
            "lifecycle_state": first_present(row.get("lifecycleState"), row.get("state")),
            "create_time": as_iso8601(first_present(row.get("createTime"), row.get("create_time"))),
            "updated_at": as_iso8601(first_present(row.get("updateTime"), row.get("createTime"), row.get("projectNumber"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_service_account(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "service_account_email": str(first_present(row.get("email"), row.get("name"), "")),
            "project_id": self._project(row),
            "unique_id": str(first_present(row.get("uniqueId"), row.get("unique_id"), "")),
            "display_name": first_present(row.get("displayName"), row.get("display_name")),
            "disabled": str(first_present(row.get("disabled"), "")),
            "oauth2_client_id": first_present(row.get("oauth2ClientId"), row.get("oauth2_client_id")),
            "create_time": as_iso8601(first_present(row.get("createTime"), row.get("create_time"))),
            "updated_at": as_iso8601(first_present(row.get("updateTime"), row.get("createTime"), row.get("email"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_policy_binding(self, row: dict[str, Any]) -> dict[str, Any]:
        members = row.get("members") if isinstance(row.get("members"), list) else []
        condition = row.get("condition") if isinstance(row.get("condition"), dict) else {}
        project_id = self._project(row)
        role = first_present(row.get("role"), row.get("role_name"))
        binding_id = first_present(
            row.get("binding_id"),
            row.get("id"),
            self._stable_binding_id(role, members, project_id),
        )
        return {
            "binding_id": str(binding_id or ""),
            "project_id": project_id,
            "role": role,
            "members": json.dumps(members, separators=(",", ":"), default=str) if members else None,
            "condition": json.dumps(condition, separators=(",", ":"), default=str) if condition else None,
            "etag": row.get("etag"),
            "updated_at": as_iso8601(first_present(row.get("updateTime"), row.get("version"), row.get("etag"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_compute_instance(self, row: dict[str, Any]) -> dict[str, Any]:
        nics = row.get("networkInterfaces") if isinstance(row.get("networkInterfaces"), list) else []
        return {
            "instance_id": str(first_present(row.get("id"), row.get("instanceId"), row.get("name"), "")),
            "project_id": self._project(row),
            "zone": row.get("zone"),
            "name": row.get("name"),
            "machine_type": first_present(row.get("machineType"), row.get("machine_type")),
            "status": row.get("status"),
            "network_interfaces": json.dumps(nics, separators=(",", ":"), default=str) if nics else None,
            "create_time": as_iso8601(first_present(row.get("creationTimestamp"), row.get("createTime"))),
            "updated_at": as_iso8601(first_present(row.get("lastStartTimestamp"), row.get("creationTimestamp"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_vpc_network(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "network_id": str(first_present(row.get("id"), row.get("networkId"), row.get("name"), "")),
            "project_id": self._project(row),
            "name": row.get("name"),
            "auto_create_subnetworks": str(first_present(row.get("autoCreateSubnetworks"), "")),
            "routing_mode": first_present(row.get("routingConfig", {}).get("routingMode") if isinstance(row.get("routingConfig"), dict) else None, row.get("routingMode")),
            "mtu": str(first_present(row.get("mtu"), "")),
            "self_link": first_present(row.get("selfLink"), row.get("self_link")),
            "updated_at": as_iso8601(first_present(row.get("creationTimestamp"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_firewall_rule(self, row: dict[str, Any]) -> dict[str, Any]:
        source_ranges = row.get("sourceRanges") if isinstance(row.get("sourceRanges"), list) else []
        target_tags = row.get("targetTags") if isinstance(row.get("targetTags"), list) else []
        return {
            "firewall_rule_id": str(first_present(row.get("id"), row.get("firewallRuleId"), row.get("name"), "")),
            "project_id": self._project(row),
            "name": row.get("name"),
            "network": row.get("network"),
            "direction": row.get("direction"),
            "priority": str(first_present(row.get("priority"), "")),
            "source_ranges": json.dumps(source_ranges, separators=(",", ":"), default=str) if source_ranges else None,
            "target_tags": json.dumps(target_tags, separators=(",", ":"), default=str) if target_tags else None,
            "updated_at": as_iso8601(first_present(row.get("creationTimestamp"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_storage_bucket(self, row: dict[str, Any]) -> dict[str, Any]:
        iam_config = row.get("iamConfiguration") if isinstance(row.get("iamConfiguration"), dict) else {}
        versioning = row.get("versioning") if isinstance(row.get("versioning"), dict) else {}
        return {
            "bucket_name": str(first_present(row.get("name"), row.get("bucketName"), "")),
            "project_id": self._project(row),
            "location": row.get("location"),
            "storage_class": first_present(row.get("storageClass"), row.get("storage_class")),
            "iam_configuration": json.dumps(iam_config, separators=(",", ":"), default=str) if iam_config else None,
            "versioning_enabled": str(first_present(versioning.get("enabled"), row.get("versioningEnabled"), "")),
            "create_time": as_iso8601(first_present(row.get("timeCreated"), row.get("createTime"))),
            "updated_at": as_iso8601(first_present(row.get("updated"), row.get("timeCreated"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_kms_key(self, row: dict[str, Any]) -> dict[str, Any]:
        primary = row.get("primary") if isinstance(row.get("primary"), dict) else {}
        return {
            "crypto_key_id": str(first_present(row.get("name"), row.get("cryptoKeyId"), "")),
            "project_id": self._project(row),
            "location": first_present(row.get("location"), row.get("labels", {}).get("location") if isinstance(row.get("labels"), dict) else None),
            "key_ring": first_present(row.get("keyRing"), row.get("key_ring")),
            "name": row.get("name"),
            "purpose": row.get("purpose"),
            "primary_state": first_present(primary.get("state"), row.get("primaryState")),
            "rotation_period": row.get("rotationPeriod"),
            "updated_at": as_iso8601(first_present(row.get("nextRotationTime"), row.get("createTime"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_logging_sink(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "sink_id": str(first_present(row.get("name"), row.get("sinkId"), row.get("id"), "")),
            "project_id": self._project(row),
            "name": row.get("name"),
            "destination": row.get("destination"),
            "filter": row.get("filter"),
            "writer_identity": first_present(row.get("writerIdentity"), row.get("writer_identity")),
            "include_children": str(first_present(row.get("includeChildren"), row.get("include_children"), "")),
            "updated_at": as_iso8601(first_present(row.get("updateTime"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_scc_finding(self, row: dict[str, Any]) -> dict[str, Any]:
        severity = first_present(row.get("severity"), row.get("findingClass"), row.get("state"))
        resource = row.get("resourceName") or row.get("resource_name")
        return {
            "finding_id": str(first_present(row.get("name"), row.get("findingId"), row.get("id"), "")),
            "project_id": self._project(row),
            "category": row.get("category"),
            "state": row.get("state"),
            "severity": str(severity) if severity is not None else None,
            "event_time": as_iso8601(first_present(row.get("eventTime"), row.get("event_time"))),
            "resource_name": resource,
            "updated_at": as_iso8601(first_present(row.get("updateTime"), row.get("eventTime"), row.get("name"))),
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
