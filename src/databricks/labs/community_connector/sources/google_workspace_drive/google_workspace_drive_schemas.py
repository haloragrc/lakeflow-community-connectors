"""Schemas and metadata for google_workspace_drive connector."""

from pyspark.sql.types import StringType, StructField, StructType

FILES_SCHEMA = StructType([
    StructField("file_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("mime_type", StringType(), True),
    StructField("drive_id", StringType(), True),
    StructField("owners", StringType(), True),
    StructField("web_view_link", StringType(), True),
    StructField("created_time", StringType(), True),
    StructField("modified_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FOLDERS_SCHEMA = StructType([
    StructField("folder_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("drive_id", StringType(), True),
    StructField("owners", StringType(), True),
    StructField("web_view_link", StringType(), True),
    StructField("created_time", StringType(), True),
    StructField("modified_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

PERMISSIONS_SCHEMA = StructType([
    StructField("permission_id", StringType(), False),
    StructField("file_id", StringType(), True),
    StructField("permission_type", StringType(), True),
    StructField("role", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("allow_file_discovery", StringType(), True),
    StructField("expiration_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

DRIVES_SCHEMA = StructType([
    StructField("drive_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("hidden", StringType(), True),
    StructField("organizer_count", StringType(), True),
    StructField("created_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "files": FILES_SCHEMA,
    "folders": FOLDERS_SCHEMA,
    "permissions": PERMISSIONS_SCHEMA,
    "drives": DRIVES_SCHEMA,
}

TABLE_METADATA = {
    "files": {"primary_keys": ["file_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "folders": {"primary_keys": ["folder_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "permissions": {"primary_keys": ["permission_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "drives": {"primary_keys": ["drive_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
