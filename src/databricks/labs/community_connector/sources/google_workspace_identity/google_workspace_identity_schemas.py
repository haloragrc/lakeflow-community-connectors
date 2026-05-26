"""Schemas and metadata for google_workspace_identity connector."""

from pyspark.sql.types import StringType, StructField, StructType

USERS_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("primary_email", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("is_admin", StringType(), True),
    StructField("is_suspended", StringType(), True),
    StructField("org_unit_path", StringType(), True),
    StructField("last_login_time", StringType(), True),
    StructField("creation_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

GROUPS_SCHEMA = StructType([
    StructField("group_id", StringType(), False),
    StructField("email", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("admin_created", StringType(), True),
    StructField("direct_members_count", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

MEMBERSHIPS_SCHEMA = StructType([
    StructField("membership_id", StringType(), False),
    StructField("group_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("member_email", StringType(), True),
    StructField("member_type", StringType(), True),
    StructField("role", StringType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ROLES_SCHEMA = StructType([
    StructField("role_id", StringType(), False),
    StructField("role_name", StringType(), True),
    StructField("role_description", StringType(), True),
    StructField("is_system_role", StringType(), True),
    StructField("is_super_admin_role", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ROLE_ASSIGNMENTS_SCHEMA = StructType([
    StructField("role_assignment_id", StringType(), False),
    StructField("role_id", StringType(), True),
    StructField("assigned_to", StringType(), True),
    StructField("assignee_type", StringType(), True),
    StructField("scope_type", StringType(), True),
    StructField("org_unit_id", StringType(), True),
    StructField("condition", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "users": USERS_SCHEMA,
    "groups": GROUPS_SCHEMA,
    "memberships": MEMBERSHIPS_SCHEMA,
    "roles": ROLES_SCHEMA,
    "role_assignments": ROLE_ASSIGNMENTS_SCHEMA,
}

TABLE_METADATA = {
    "users": {"primary_keys": ["user_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "groups": {"primary_keys": ["group_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "memberships": {"primary_keys": ["membership_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "roles": {"primary_keys": ["role_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "role_assignments": {"primary_keys": ["role_assignment_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
