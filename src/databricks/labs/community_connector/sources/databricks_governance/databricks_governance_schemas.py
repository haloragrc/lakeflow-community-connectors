"""Schemas and metadata for databricks_governance connector."""

from pyspark.sql.types import StringType, StructField, StructType

ACCOUNT_USERS_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("user_name", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("active", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("last_login_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ACCOUNT_GROUPS_SCHEMA = StructType([
    StructField("group_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("external_id", StringType(), True),
    StructField("entitlements", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

GROUP_MEMBERSHIPS_SCHEMA = StructType([
    StructField("membership_id", StringType(), False),
    StructField("group_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("member_type", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SERVICE_PRINCIPALS_SCHEMA = StructType([
    StructField("service_principal_id", StringType(), False),
    StructField("application_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("active", StringType(), True),
    StructField("entitlements", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

WORKSPACES_SCHEMA = StructType([
    StructField("workspace_id", StringType(), False),
    StructField("workspace_name", StringType(), True),
    StructField("deployment_name", StringType(), True),
    StructField("workspace_url", StringType(), True),
    StructField("location", StringType(), True),
    StructField("pricing_tier", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

CLUSTERS_SCHEMA = StructType([
    StructField("cluster_id", StringType(), False),
    StructField("workspace_id", StringType(), True),
    StructField("cluster_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("node_type_id", StringType(), True),
    StructField("spark_version", StringType(), True),
    StructField("autotermination_minutes", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

JOBS_SCHEMA = StructType([
    StructField("job_id", StringType(), False),
    StructField("workspace_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("creator_user_name", StringType(), True),
    StructField("schedule", StringType(), True),
    StructField("max_concurrent_runs", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

JOB_RUNS_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("job_id", StringType(), True),
    StructField("workspace_id", StringType(), True),
    StructField("run_name", StringType(), True),
    StructField("life_cycle_state", StringType(), True),
    StructField("result_state", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

UC_CATALOGS_SCHEMA = StructType([
    StructField("catalog_name", StringType(), False),
    StructField("owner", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("isolation_mode", StringType(), True),
    StructField("storage_root", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

UC_SCHEMAS_SCHEMA = StructType([
    StructField("schema_id", StringType(), False),
    StructField("catalog_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("storage_root", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

UC_TABLES_SCHEMA = StructType([
    StructField("table_id", StringType(), False),
    StructField("catalog_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("table_type", StringType(), True),
    StructField("data_source_format", StringType(), True),
    StructField("storage_location", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

UC_GRANTS_SCHEMA = StructType([
    StructField("grant_id", StringType(), False),
    StructField("securable_type", StringType(), True),
    StructField("securable_name", StringType(), True),
    StructField("principal", StringType(), True),
    StructField("privilege", StringType(), True),
    StructField("inherited", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

AUDIT_LOGS_SCHEMA = StructType([
    StructField("audit_event_id", StringType(), False),
    StructField("workspace_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("service_name", StringType(), True),
    StructField("action_name", StringType(), True),
    StructField("user_identity", StringType(), True),
    StructField("source_ip", StringType(), True),
    StructField("response_status", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "account_users": ACCOUNT_USERS_SCHEMA,
    "account_groups": ACCOUNT_GROUPS_SCHEMA,
    "group_memberships": GROUP_MEMBERSHIPS_SCHEMA,
    "service_principals": SERVICE_PRINCIPALS_SCHEMA,
    "workspaces": WORKSPACES_SCHEMA,
    "clusters": CLUSTERS_SCHEMA,
    "jobs": JOBS_SCHEMA,
    "job_runs": JOB_RUNS_SCHEMA,
    "uc_catalogs": UC_CATALOGS_SCHEMA,
    "uc_schemas": UC_SCHEMAS_SCHEMA,
    "uc_tables": UC_TABLES_SCHEMA,
    "uc_grants": UC_GRANTS_SCHEMA,
    "audit_logs": AUDIT_LOGS_SCHEMA,
}

TABLE_METADATA = {
    "account_users": {"primary_keys": ["user_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "account_groups": {"primary_keys": ["group_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "group_memberships": {"primary_keys": ["membership_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "service_principals": {"primary_keys": ["service_principal_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "workspaces": {"primary_keys": ["workspace_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "clusters": {"primary_keys": ["cluster_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "jobs": {"primary_keys": ["job_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "job_runs": {"primary_keys": ["run_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "uc_catalogs": {"primary_keys": ["catalog_name"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "uc_schemas": {"primary_keys": ["schema_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "uc_tables": {"primary_keys": ["table_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "uc_grants": {"primary_keys": ["grant_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "audit_logs": {"primary_keys": ["audit_event_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
