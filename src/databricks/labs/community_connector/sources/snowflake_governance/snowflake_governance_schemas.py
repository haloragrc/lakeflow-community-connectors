"""Schemas and metadata for snowflake_governance connector."""

from pyspark.sql.types import StringType, StructField, StructType

ACCOUNTS_SCHEMA = StructType([
    StructField("account_name", StringType(), False),
    StructField("organization_name", StringType(), True),
    StructField("edition", StringType(), True),
    StructField("region", StringType(), True),
    StructField("locator", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

USERS_SCHEMA = StructType([
    StructField("user_name", StringType(), False),
    StructField("login_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("default_role", StringType(), True),
    StructField("disabled", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("last_success_login", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ROLES_SCHEMA = StructType([
    StructField("role_name", StringType(), False),
    StructField("owner", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("deleted_on", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ROLE_GRANTS_SCHEMA = StructType([
    StructField("grant_id", StringType(), False),
    StructField("role_name", StringType(), True),
    StructField("granted_to", StringType(), True),
    StructField("grantee_name", StringType(), True),
    StructField("grant_option", StringType(), True),
    StructField("granted_on", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

WAREHOUSES_SCHEMA = StructType([
    StructField("warehouse_name", StringType(), False),
    StructField("state", StringType(), True),
    StructField("size", StringType(), True),
    StructField("type", StringType(), True),
    StructField("auto_suspend", StringType(), True),
    StructField("auto_resume", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

DATABASES_SCHEMA = StructType([
    StructField("database_name", StringType(), False),
    StructField("owner", StringType(), True),
    StructField("is_transient", StringType(), True),
    StructField("retention_time", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCHEMAS_SCHEMA = StructType([
    StructField("schema_id", StringType(), False),
    StructField("database_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("is_managed_access", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLES_SCHEMA = StructType([
    StructField("table_id", StringType(), False),
    StructField("database_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("table_type", StringType(), True),
    StructField("row_count", StringType(), True),
    StructField("bytes", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

STAGES_SCHEMA = StructType([
    StructField("stage_id", StringType(), False),
    StructField("database_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("stage_name", StringType(), True),
    StructField("url", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

MASKING_POLICIES_SCHEMA = StructType([
    StructField("policy_id", StringType(), False),
    StructField("database_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("policy_name", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("signature", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

NETWORK_POLICIES_SCHEMA = StructType([
    StructField("network_policy_name", StringType(), False),
    StructField("allowed_ip_list", StringType(), True),
    StructField("blocked_ip_list", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

LOGIN_HISTORY_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_timestamp", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("client_ip", StringType(), True),
    StructField("reported_client_type", StringType(), True),
    StructField("is_success", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

QUERY_HISTORY_SCHEMA = StructType([
    StructField("query_id", StringType(), False),
    StructField("user_name", StringType(), True),
    StructField("warehouse_name", StringType(), True),
    StructField("database_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("execution_status", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("bytes_scanned", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "accounts": ACCOUNTS_SCHEMA,
    "users": USERS_SCHEMA,
    "roles": ROLES_SCHEMA,
    "role_grants": ROLE_GRANTS_SCHEMA,
    "warehouses": WAREHOUSES_SCHEMA,
    "databases": DATABASES_SCHEMA,
    "schemas": SCHEMAS_SCHEMA,
    "tables": TABLES_SCHEMA,
    "stages": STAGES_SCHEMA,
    "masking_policies": MASKING_POLICIES_SCHEMA,
    "network_policies": NETWORK_POLICIES_SCHEMA,
    "login_history": LOGIN_HISTORY_SCHEMA,
    "query_history": QUERY_HISTORY_SCHEMA,
}

TABLE_METADATA = {
    "accounts": {"primary_keys": ["account_name"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "users": {"primary_keys": ["user_name"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "roles": {"primary_keys": ["role_name"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "role_grants": {"primary_keys": ["grant_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "warehouses": {"primary_keys": ["warehouse_name"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "databases": {"primary_keys": ["database_name"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "schemas": {"primary_keys": ["schema_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "tables": {"primary_keys": ["table_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "stages": {"primary_keys": ["stage_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "masking_policies": {"primary_keys": ["policy_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "network_policies": {"primary_keys": ["network_policy_name"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "login_history": {"primary_keys": ["event_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "query_history": {"primary_keys": ["query_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
