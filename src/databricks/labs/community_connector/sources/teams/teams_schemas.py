"""Schemas and metadata for teams connector."""

from pyspark.sql.types import StringType, StructField, StructType

USERS_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("user_principal_name", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("mail", StringType(), True),
    StructField("account_enabled", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TEAMS_SCHEMA = StructType([
    StructField("team_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("visibility", StringType(), True),
    StructField("is_archived", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

CHANNELS_SCHEMA = StructType([
    StructField("channel_id", StringType(), False),
    StructField("team_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("membership_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

MESSAGES_SCHEMA = StructType([
    StructField("message_id", StringType(), False),
    StructField("team_id", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("from_user_id", StringType(), True),
    StructField("message_type", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FILES_SCHEMA = StructType([
    StructField("file_id", StringType(), False),
    StructField("team_id", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("message_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("content_type", StringType(), True),
    StructField("content_url", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "users": USERS_SCHEMA,
    "teams": TEAMS_SCHEMA,
    "channels": CHANNELS_SCHEMA,
    "messages": MESSAGES_SCHEMA,
    "files": FILES_SCHEMA,
}

TABLE_METADATA = {
    "users": {"primary_keys": ["user_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "teams": {"primary_keys": ["team_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "channels": {"primary_keys": ["channel_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "messages": {"primary_keys": ["message_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "files": {"primary_keys": ["file_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
