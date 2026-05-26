"""Schemas and metadata for azure_inventory connector."""

from pyspark.sql.types import StringType, StructField, StructType

SUBSCRIPTIONS_SCHEMA = StructType([
    StructField("subscription_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("tenant_id", StringType(), True),
    StructField("quota_id", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

RESOURCE_GROUPS_SCHEMA = StructType([
    StructField("resource_group_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("provisioning_state", StringType(), True),
    StructField("managed_by", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VIRTUAL_MACHINES_SCHEMA = StructType([
    StructField("vm_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("resource_group", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("vm_size", StringType(), True),
    StructField("power_state", StringType(), True),
    StructField("os_type", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VIRTUAL_NETWORKS_SCHEMA = StructType([
    StructField("vnet_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("resource_group", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("address_space", StringType(), True),
    StructField("provisioning_state", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

NETWORK_SECURITY_GROUPS_SCHEMA = StructType([
    StructField("nsg_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("resource_group", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("security_rules", StringType(), True),
    StructField("default_rules", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

STORAGE_ACCOUNTS_SCHEMA = StructType([
    StructField("storage_account_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("resource_group", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("kind", StringType(), True),
    StructField("primary_endpoints", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

KEY_VAULTS_SCHEMA = StructType([
    StructField("key_vault_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("resource_group", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("tenant_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("soft_delete_enabled", StringType(), True),
    StructField("public_network_access", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ROLE_ASSIGNMENTS_SCHEMA = StructType([
    StructField("role_assignment_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("scope", StringType(), True),
    StructField("principal_id", StringType(), True),
    StructField("principal_type", StringType(), True),
    StructField("role_definition_id", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

POLICY_ASSIGNMENTS_SCHEMA = StructType([
    StructField("policy_assignment_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("scope", StringType(), True),
    StructField("policy_definition_id", StringType(), True),
    StructField("enforcement_mode", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ACTIVITY_LOGS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("operation_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("caller", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

DEFENDER_ASSESSMENTS_SCHEMA = StructType([
    StructField("assessment_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("status_code", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("remediation_description", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "subscriptions": SUBSCRIPTIONS_SCHEMA,
    "resource_groups": RESOURCE_GROUPS_SCHEMA,
    "virtual_machines": VIRTUAL_MACHINES_SCHEMA,
    "virtual_networks": VIRTUAL_NETWORKS_SCHEMA,
    "network_security_groups": NETWORK_SECURITY_GROUPS_SCHEMA,
    "storage_accounts": STORAGE_ACCOUNTS_SCHEMA,
    "key_vaults": KEY_VAULTS_SCHEMA,
    "role_assignments": ROLE_ASSIGNMENTS_SCHEMA,
    "policy_assignments": POLICY_ASSIGNMENTS_SCHEMA,
    "activity_logs": ACTIVITY_LOGS_SCHEMA,
    "defender_assessments": DEFENDER_ASSESSMENTS_SCHEMA,
}

TABLE_METADATA = {
    "subscriptions": {"primary_keys": ["subscription_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "resource_groups": {"primary_keys": ["resource_group_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "virtual_machines": {"primary_keys": ["vm_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "virtual_networks": {"primary_keys": ["vnet_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "network_security_groups": {"primary_keys": ["nsg_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "storage_accounts": {"primary_keys": ["storage_account_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "key_vaults": {"primary_keys": ["key_vault_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "role_assignments": {"primary_keys": ["role_assignment_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "policy_assignments": {"primary_keys": ["policy_assignment_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "activity_logs": {"primary_keys": ["event_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "defender_assessments": {"primary_keys": ["assessment_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
