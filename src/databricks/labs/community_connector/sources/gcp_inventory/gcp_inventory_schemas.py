"""Schemas and metadata for gcp_inventory connector."""

from pyspark.sql.types import StringType, StructField, StructType

ORGANIZATIONS_SCHEMA = StructType([
    StructField("organization_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("lifecycle_state", StringType(), True),
    StructField("owner_directory_customer_id", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FOLDERS_SCHEMA = StructType([
    StructField("folder_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("parent", StringType(), True),
    StructField("lifecycle_state", StringType(), True),
    StructField("create_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

PROJECTS_SCHEMA = StructType([
    StructField("project_id", StringType(), False),
    StructField("project_number", StringType(), True),
    StructField("name", StringType(), True),
    StructField("parent", StringType(), True),
    StructField("lifecycle_state", StringType(), True),
    StructField("create_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

IAM_SERVICE_ACCOUNTS_SCHEMA = StructType([
    StructField("service_account_email", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("unique_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("disabled", StringType(), True),
    StructField("oauth2_client_id", StringType(), True),
    StructField("create_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

IAM_POLICY_BINDINGS_SCHEMA = StructType([
    StructField("binding_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("role", StringType(), True),
    StructField("members", StringType(), True),
    StructField("condition", StringType(), True),
    StructField("etag", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

COMPUTE_INSTANCES_SCHEMA = StructType([
    StructField("instance_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("name", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("network_interfaces", StringType(), True),
    StructField("create_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VPC_NETWORKS_SCHEMA = StructType([
    StructField("network_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("auto_create_subnetworks", StringType(), True),
    StructField("routing_mode", StringType(), True),
    StructField("mtu", StringType(), True),
    StructField("self_link", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FIREWALL_RULES_SCHEMA = StructType([
    StructField("firewall_rule_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("network", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("source_ranges", StringType(), True),
    StructField("target_tags", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

STORAGE_BUCKETS_SCHEMA = StructType([
    StructField("bucket_name", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("storage_class", StringType(), True),
    StructField("iam_configuration", StringType(), True),
    StructField("versioning_enabled", StringType(), True),
    StructField("create_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

KMS_CRYPTO_KEYS_SCHEMA = StructType([
    StructField("crypto_key_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("key_ring", StringType(), True),
    StructField("name", StringType(), True),
    StructField("purpose", StringType(), True),
    StructField("primary_state", StringType(), True),
    StructField("rotation_period", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

LOGGING_SINKS_SCHEMA = StructType([
    StructField("sink_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("filter", StringType(), True),
    StructField("writer_identity", StringType(), True),
    StructField("include_children", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCC_FINDINGS_SCHEMA = StructType([
    StructField("finding_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("state", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("resource_name", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "organizations": ORGANIZATIONS_SCHEMA,
    "folders": FOLDERS_SCHEMA,
    "projects": PROJECTS_SCHEMA,
    "iam_service_accounts": IAM_SERVICE_ACCOUNTS_SCHEMA,
    "iam_policy_bindings": IAM_POLICY_BINDINGS_SCHEMA,
    "compute_instances": COMPUTE_INSTANCES_SCHEMA,
    "vpc_networks": VPC_NETWORKS_SCHEMA,
    "firewall_rules": FIREWALL_RULES_SCHEMA,
    "storage_buckets": STORAGE_BUCKETS_SCHEMA,
    "kms_crypto_keys": KMS_CRYPTO_KEYS_SCHEMA,
    "logging_sinks": LOGGING_SINKS_SCHEMA,
    "scc_findings": SCC_FINDINGS_SCHEMA,
}

TABLE_METADATA = {
    "organizations": {"primary_keys": ["organization_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "folders": {"primary_keys": ["folder_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "projects": {"primary_keys": ["project_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "iam_service_accounts": {"primary_keys": ["service_account_email"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "iam_policy_bindings": {"primary_keys": ["binding_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "compute_instances": {"primary_keys": ["instance_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "vpc_networks": {"primary_keys": ["network_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "firewall_rules": {"primary_keys": ["firewall_rule_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "storage_buckets": {"primary_keys": ["bucket_name"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "kms_crypto_keys": {"primary_keys": ["crypto_key_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "logging_sinks": {"primary_keys": ["sink_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "scc_findings": {"primary_keys": ["finding_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
