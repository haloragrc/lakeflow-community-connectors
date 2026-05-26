"""Schemas and metadata for entra_identity connector."""

from pyspark.sql.types import StringType, StructField, StructType

USERS_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("user_principal_name", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("mail", StringType(), True),
    StructField("account_enabled", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

GROUPS_SCHEMA = StructType([
    StructField("group_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("mail_nickname", StringType(), True),
    StructField("mail_enabled", StringType(), True),
    StructField("security_enabled", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

MEMBERSHIPS_SCHEMA = StructType([
    StructField("membership_id", StringType(), False),
    StructField("group_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("member_type", StringType(), True),
    StructField("member_display_name", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SERVICE_PRINCIPALS_SCHEMA = StructType([
    StructField("service_principal_id", StringType(), False),
    StructField("app_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("app_owner_org_id", StringType(), True),
    StructField("account_enabled", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

APP_ROLE_ASSIGNMENTS_SCHEMA = StructType([
    StructField("assignment_id", StringType(), False),
    StructField("principal_id", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("app_role_id", StringType(), True),
    StructField("principal_type", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

DIRECTORY_ROLES_SCHEMA = StructType([
    StructField("directory_role_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("template_id", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "users": USERS_SCHEMA,
    "groups": GROUPS_SCHEMA,
    "memberships": MEMBERSHIPS_SCHEMA,
    "service_principals": SERVICE_PRINCIPALS_SCHEMA,
    "app_role_assignments": APP_ROLE_ASSIGNMENTS_SCHEMA,
    "directory_roles": DIRECTORY_ROLES_SCHEMA,
}

TABLE_METADATA = {
    "users": {"primary_keys": ["user_id"], "cursor_field": "last_updated", "ingestion_type": "cdc"},
    "groups": {"primary_keys": ["group_id"], "cursor_field": "last_updated", "ingestion_type": "cdc"},
    "memberships": {"primary_keys": ["membership_id"], "cursor_field": None, "ingestion_type": "snapshot"},
    "service_principals": {"primary_keys": ["service_principal_id"], "cursor_field": "last_updated", "ingestion_type": "cdc"},
    "app_role_assignments": {"primary_keys": ["assignment_id"], "cursor_field": "created_at", "ingestion_type": "cdc"},
    "directory_roles": {"primary_keys": ["directory_role_id"], "cursor_field": None, "ingestion_type": "snapshot"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
