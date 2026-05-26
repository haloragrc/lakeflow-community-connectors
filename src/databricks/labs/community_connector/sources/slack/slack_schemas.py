"""Schemas and metadata for slack connector."""

from pyspark.sql.types import StringType, StructField, StructType

USERS_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("real_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("is_admin", StringType(), True),
    StructField("is_bot", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

CHANNELS_SCHEMA = StructType([
    StructField("channel_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("is_private", StringType(), True),
    StructField("is_archived", StringType(), True),
    StructField("is_member", StringType(), True),
    StructField("created", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

MESSAGES_SCHEMA = StructType([
    StructField("message_id", StringType(), False),
    StructField("channel_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("subtype", StringType(), True),
    StructField("thread_ts", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FILES_SCHEMA = StructType([
    StructField("file_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("mimetype", StringType(), True),
    StructField("filetype", StringType(), True),
    StructField("size", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("created", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

USERGROUPS_SCHEMA = StructType([
    StructField("usergroup_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("handle", StringType(), True),
    StructField("description", StringType(), True),
    StructField("is_usergroup", StringType(), True),
    StructField("date_update", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "users": USERS_SCHEMA,
    "channels": CHANNELS_SCHEMA,
    "messages": MESSAGES_SCHEMA,
    "files": FILES_SCHEMA,
    "usergroups": USERGROUPS_SCHEMA,
}

TABLE_METADATA = {
    "users": {"primary_keys": ["user_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "channels": {"primary_keys": ["channel_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "messages": {"primary_keys": ["message_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "files": {"primary_keys": ["file_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "usergroups": {"primary_keys": ["usergroup_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
