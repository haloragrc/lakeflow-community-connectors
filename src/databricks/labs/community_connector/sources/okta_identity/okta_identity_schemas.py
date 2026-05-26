"""Schemas and metadata for okta_identity connector."""

from pyspark.sql.types import ArrayType, BooleanType, LongType, StringType, StructField, StructType

USERS_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("status", StringType(), True),
    StructField("login", StringType(), True),
    StructField("email", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("created", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

GROUPS_SCHEMA = StructType([
    StructField("group_id", StringType(), False),
    StructField("type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("created", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

MEMBERSHIPS_SCHEMA = StructType([
    StructField("membership_id", StringType(), False),
    StructField("group_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_login", StringType(), True),
    StructField("user_status", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

APP_ASSIGNMENTS_SCHEMA = StructType([
    StructField("assignment_id", StringType(), False),
    StructField("app_id", StringType(), True),
    StructField("app_label", StringType(), True),
    StructField("principal_id", StringType(), True),
    StructField("scope", StringType(), True),
    StructField("status", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ROLES_SCHEMA = StructType([
    StructField("role_id", StringType(), False),
    StructField("role_type", StringType(), True),
    StructField("label", StringType(), True),
    StructField("status", StringType(), True),
    StructField("assignment_type", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FACTORS_SCHEMA = StructType([
    StructField("factor_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("factor_type", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "users": USERS_SCHEMA,
    "groups": GROUPS_SCHEMA,
    "memberships": MEMBERSHIPS_SCHEMA,
    "app_assignments": APP_ASSIGNMENTS_SCHEMA,
    "roles": ROLES_SCHEMA,
    "factors": FACTORS_SCHEMA,
}

TABLE_METADATA = {
    "users": {"primary_keys": ["user_id"], "cursor_field": "last_updated", "ingestion_type": "cdc"},
    "groups": {"primary_keys": ["group_id"], "cursor_field": "last_updated", "ingestion_type": "cdc"},
    "memberships": {"primary_keys": ["membership_id"], "cursor_field": None, "ingestion_type": "snapshot"},
    "app_assignments": {"primary_keys": ["assignment_id"], "cursor_field": "last_updated", "ingestion_type": "cdc"},
    "roles": {"primary_keys": ["role_id"], "cursor_field": "last_updated", "ingestion_type": "cdc"},
    "factors": {"primary_keys": ["factor_id"], "cursor_field": "last_updated", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
