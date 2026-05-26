"""Azure inventory and governance API connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.azure_inventory.azure_inventory_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
    first_present,
)


class AzureInventoryLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        tenant_id = options.get("tenant_id")
        client_id = options.get("client_id")
        client_secret = options.get("client_secret")
        if not tenant_id or not client_id or not client_secret:
            raise ValueError("azure_inventory requires tenant_id, client_id, and client_secret")

        self.subscription_id = options.get("subscription_id")
        self.page_size = int(options.get("page_size", "200"))
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://management.azure.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "X-Azure-Tenant-Id": tenant_id,
                "X-Azure-Client-Id": client_id,
                "X-Azure-Client-Secret": client_secret,
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

        if table_name == "subscriptions":
            records = self._read_collection("/subscriptions", "value", self._map_subscription)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "resource_groups":
            records = self._read_collection("/resources/resourcegroups", "value", self._map_resource_group)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "virtual_machines":
            records = self._read_collection("/compute/virtualmachines", "value", self._map_virtual_machine)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "virtual_networks":
            records = self._read_collection("/network/virtualnetworks", "value", self._map_virtual_network)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "network_security_groups":
            records = self._read_collection("/network/securitygroups", "value", self._map_network_security_group)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "storage_accounts":
            records = self._read_collection("/storage/accounts", "value", self._map_storage_account)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "key_vaults":
            records = self._read_collection("/keyvault/vaults", "value", self._map_key_vault)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "role_assignments":
            records = self._read_collection("/authorization/roleassignments", "value", self._map_role_assignment)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "policy_assignments":
            records = self._read_collection("/policy/assignments", "value", self._map_policy_assignment)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "activity_logs":
            records = self._read_collection("/monitor/activitylogs", "value", self._map_activity_log)
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "defender_assessments":
            records = self._read_collection("/security/assessments", "value", self._map_defender_assessment)
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_collection(
        self,
        path: str,
        records_key: str,
        mapper: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        next_link = None
        results: list[dict[str, Any]] = []
        for _ in range(100):
            request_path = next_link or path
            params = {"page_size": self.page_size} if not next_link else None
            body = self.client.request_json("GET", request_path, params=params)
            rows = self._extract_rows(body, records_key)
            results.extend(mapper(row) for row in rows if isinstance(row, dict))

            if not isinstance(body, dict):
                break
            token = body.get("nextLink") or body.get("next_link")
            if not token or token == next_link:
                break
            next_link = str(token)
            if next_link.startswith("http"):
                next_link = "/" + next_link.split("/", 3)[-1]
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

    def _sub(self, row: dict[str, Any]) -> str | None:
        return first_present(row.get("subscription_id"), row.get("subscriptionId"), self.subscription_id)

    def _resource_group_from_id(self, resource_id: Any) -> str | None:
        if not resource_id:
            return None
        parts = str(resource_id).split("/")
        for idx, part in enumerate(parts):
            if part.lower() == "resourcegroups" and idx + 1 < len(parts):
                return parts[idx + 1]
        return None

    def _map_subscription(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "subscription_id": str(first_present(row.get("subscriptionId"), row.get("subscription_id"), row.get("id"), "")),
            "display_name": first_present(row.get("displayName"), row.get("display_name"), row.get("name")),
            "state": row.get("state"),
            "tenant_id": first_present(row.get("tenantId"), row.get("tenant_id")),
            "quota_id": first_present(row.get("quotaId"), row.get("quota_id")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("authorizationSource"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_resource_group(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        return {
            "resource_group_id": str(first_present(row.get("id"), row.get("resourceGroupId"), "")),
            "subscription_id": self._sub(row),
            "name": row.get("name"),
            "location": row.get("location"),
            "provisioning_state": first_present(props.get("provisioningState"), props.get("provisioning_state")),
            "managed_by": first_present(row.get("managedBy"), row.get("managed_by")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), props.get("createdTime"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_virtual_machine(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        profile = row.get("hardwareProfile") if isinstance(row.get("hardwareProfile"), dict) else {}
        storage = props.get("storageProfile") if isinstance(props.get("storageProfile"), dict) else {}
        os_disk = storage.get("osDisk") if isinstance(storage.get("osDisk"), dict) else {}
        return {
            "vm_id": str(first_present(row.get("id"), row.get("vmId"), row.get("name"), "")),
            "subscription_id": self._sub(row),
            "resource_group": self._resource_group_from_id(row.get("id")),
            "name": row.get("name"),
            "location": row.get("location"),
            "vm_size": first_present(profile.get("vmSize"), row.get("vmSize")),
            "power_state": first_present(props.get("powerState"), row.get("powerState")),
            "os_type": first_present(os_disk.get("osType"), row.get("osType")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), props.get("timeCreated"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_virtual_network(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        address = props.get("addressSpace") if isinstance(props.get("addressSpace"), dict) else {}
        prefixes = address.get("addressPrefixes") if isinstance(address.get("addressPrefixes"), list) else []
        return {
            "vnet_id": str(first_present(row.get("id"), row.get("vnetId"), row.get("name"), "")),
            "subscription_id": self._sub(row),
            "resource_group": self._resource_group_from_id(row.get("id")),
            "name": row.get("name"),
            "location": row.get("location"),
            "address_space": json.dumps(prefixes, separators=(",", ":")) if prefixes else None,
            "provisioning_state": first_present(props.get("provisioningState"), row.get("provisioningState")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_network_security_group(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        rules = props.get("securityRules") if isinstance(props.get("securityRules"), list) else []
        defaults = props.get("defaultSecurityRules") if isinstance(props.get("defaultSecurityRules"), list) else []
        return {
            "nsg_id": str(first_present(row.get("id"), row.get("nsgId"), row.get("name"), "")),
            "subscription_id": self._sub(row),
            "resource_group": self._resource_group_from_id(row.get("id")),
            "name": row.get("name"),
            "location": row.get("location"),
            "security_rules": json.dumps(rules, separators=(",", ":"), default=str) if rules else None,
            "default_rules": json.dumps(defaults, separators=(",", ":"), default=str) if defaults else None,
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_storage_account(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        endpoints = props.get("primaryEndpoints") if isinstance(props.get("primaryEndpoints"), dict) else {}
        sku = row.get("sku") if isinstance(row.get("sku"), dict) else {}
        return {
            "storage_account_id": str(first_present(row.get("id"), row.get("storageAccountId"), row.get("name"), "")),
            "subscription_id": self._sub(row),
            "resource_group": self._resource_group_from_id(row.get("id")),
            "name": row.get("name"),
            "location": row.get("location"),
            "sku": first_present(sku.get("name"), row.get("skuName")),
            "kind": row.get("kind"),
            "primary_endpoints": json.dumps(endpoints, separators=(",", ":"), default=str) if endpoints else None,
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_key_vault(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        sku = props.get("sku") if isinstance(props.get("sku"), dict) else {}
        return {
            "key_vault_id": str(first_present(row.get("id"), row.get("vaultId"), row.get("name"), "")),
            "subscription_id": self._sub(row),
            "resource_group": self._resource_group_from_id(row.get("id")),
            "name": row.get("name"),
            "location": row.get("location"),
            "tenant_id": first_present(props.get("tenantId"), row.get("tenantId")),
            "sku": first_present(sku.get("name"), row.get("skuName")),
            "soft_delete_enabled": str(first_present(props.get("enableSoftDelete"), row.get("enableSoftDelete"), "")),
            "public_network_access": first_present(props.get("publicNetworkAccess"), row.get("publicNetworkAccess")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_role_assignment(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        return {
            "role_assignment_id": str(first_present(row.get("id"), row.get("roleAssignmentId"), row.get("name"), "")),
            "subscription_id": self._sub(row),
            "scope": props.get("scope"),
            "principal_id": first_present(props.get("principalId"), row.get("principalId")),
            "principal_type": first_present(props.get("principalType"), row.get("principalType")),
            "role_definition_id": first_present(props.get("roleDefinitionId"), row.get("roleDefinitionId")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), props.get("createdOn"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_policy_assignment(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        return {
            "policy_assignment_id": str(first_present(row.get("id"), row.get("policyAssignmentId"), row.get("name"), "")),
            "subscription_id": self._sub(row),
            "name": row.get("name"),
            "display_name": first_present(props.get("displayName"), row.get("displayName")),
            "scope": props.get("scope"),
            "policy_definition_id": first_present(props.get("policyDefinitionId"), row.get("policyDefinitionId")),
            "enforcement_mode": first_present(props.get("enforcementMode"), row.get("enforcementMode")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_activity_log(self, row: dict[str, Any]) -> dict[str, Any]:
        operation = row.get("operationName") if isinstance(row.get("operationName"), dict) else {}
        status = row.get("status") if isinstance(row.get("status"), dict) else {}
        category = row.get("category") if isinstance(row.get("category"), dict) else {}
        return {
            "event_id": str(first_present(row.get("eventDataId"), row.get("event_id"), row.get("id"), "")),
            "subscription_id": self._sub(row),
            "event_timestamp": as_iso8601(first_present(row.get("eventTimestamp"), row.get("event_timestamp"))),
            "operation_name": first_present(operation.get("value"), row.get("operationNameValue"), row.get("operation_name")),
            "status": first_present(status.get("value"), row.get("statusValue"), row.get("status")),
            "caller": row.get("caller"),
            "resource_id": first_present(row.get("resourceId"), row.get("resource_id")),
            "category": first_present(category.get("value"), row.get("categoryValue"), row.get("category")),
            "updated_at": as_iso8601(first_present(row.get("submissionTimestamp"), row.get("eventTimestamp"), row.get("event_timestamp"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_defender_assessment(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        status = props.get("status") if isinstance(props.get("status"), dict) else {}
        metadata = props.get("metadata") if isinstance(props.get("metadata"), dict) else {}
        links = props.get("links") if isinstance(props.get("links"), dict) else {}
        return {
            "assessment_id": str(first_present(row.get("id"), row.get("assessmentId"), row.get("name"), "")),
            "subscription_id": self._sub(row),
            "resource_id": first_present(props.get("resourceDetails", {}).get("id") if isinstance(props.get("resourceDetails"), dict) else None, row.get("resourceId")),
            "display_name": first_present(metadata.get("displayName"), row.get("displayName"), row.get("name")),
            "status_code": first_present(status.get("code"), row.get("statusCode")),
            "severity": first_present(metadata.get("severity"), row.get("severity")),
            "remediation_description": first_present(metadata.get("remediationDescription"), links.get("azurePortal")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), props.get("status", {}).get("firstEvaluationDate") if isinstance(props.get("status"), dict) else None, row.get("id"))),
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
