"""Schema and metadata definitions for the Tenable VM connector."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

ASSETS_SCHEMA = StructType(
    [
        StructField("asset_uuid", StringType(), False),
        StructField("asset_id", StringType(), True),
        StructField("ipv4", StringType(), True),
        StructField("hostname", StringType(), True),
        StructField("fqdn", StringType(), True),
        StructField("operating_system", StringType(), True),
        StructField("last_seen", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("terminated_at", StringType(), True),
        StructField("deleted_at", StringType(), True),
        StructField("is_deleted", BooleanType(), True),
        StructField("tags", ArrayType(StringType(), True), True),
        StructField("raw_payload", StringType(), True),
    ]
)

VULNERABILITIES_SCHEMA = StructType(
    [
        StructField("finding_id", StringType(), False),
        StructField("asset_uuid", StringType(), True),
        StructField("plugin_id", LongType(), True),
        StructField("plugin_name", StringType(), True),
        StructField("plugin_family", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("state", StringType(), True),
        StructField("indexed_at", StringType(), True),
        StructField("first_found", StringType(), True),
        StructField("last_found", StringType(), True),
        StructField("last_fixed", StringType(), True),
        StructField("cve", ArrayType(StringType(), True), True),
        StructField("port", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("source", StringType(), True),
        StructField("raw_payload", StringType(), True),
    ]
)

FINDINGS_SCHEMA = VULNERABILITIES_SCHEMA

SCANS_SCHEMA = StructType(
    [
        StructField("scan_uuid", StringType(), False),
        StructField("scan_id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("folder_id", LongType(), True),
        StructField("enabled", BooleanType(), True),
        StructField("creation_date", StringType(), True),
        StructField("last_modification_date", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("raw_payload", StringType(), True),
    ]
)

SCAN_RESULTS_SCHEMA = StructType(
    [
        StructField("scan_uuid", StringType(), False),
        StructField("scan_id", LongType(), True),
        StructField("history_id", LongType(), False),
        StructField("status", StringType(), True),
        StructField("uuid", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("last_modification_date", StringType(), True),
        StructField("schedule_uuid", StringType(), True),
        StructField("scanner_uuid", StringType(), True),
        StructField("owner_id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("raw_payload", StringType(), True),
    ]
)

EXPORTS_SCHEMA = StructType(
    [
        StructField("export_type", StringType(), False),
        StructField("export_uuid", StringType(), False),
        StructField("status", StringType(), True),
        StructField("chunks_available", StringType(), True),
        StructField("chunks_failed", StringType(), True),
        StructField("created", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("raw_payload", StringType(), True),
    ]
)

TAGS_SCHEMA = StructType(
    [
        StructField("tag_uuid", StringType(), False),
        StructField("category_uuid", StringType(), True),
        StructField("category_name", StringType(), True),
        StructField("value", StringType(), True),
        StructField("type", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("updated_by", StringType(), True),
        StructField("raw_payload", StringType(), True),
    ]
)

PLUGINS_SCHEMA = StructType(
    [
        StructField("plugin_id", LongType(), False),
        StructField("plugin_name", StringType(), True),
        StructField("family_name", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("plugin_modification_date", StringType(), True),
        StructField("plugin_publication_date", StringType(), True),
        StructField("plugin_version", StringType(), True),
        StructField("cve", ArrayType(StringType(), True), True),
        StructField("raw_payload", StringType(), True),
    ]
)

POLICIES_SCHEMA = StructType(
    [
        StructField("policy_id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("visibility", StringType(), True),
        StructField("shared", BooleanType(), True),
        StructField("creation_date", StringType(), True),
        StructField("last_modification_date", StringType(), True),
        StructField("raw_payload", StringType(), True),
    ]
)

USERS_SCHEMA = StructType(
    [
        StructField("user_id", LongType(), False),
        StructField("uuid", StringType(), True),
        StructField("username", StringType(), True),
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("permissions", LongType(), True),
        StructField("last_login", StringType(), True),
        StructField("enabled", BooleanType(), True),
        StructField("raw_payload", StringType(), True),
    ]
)

TABLE_SCHEMAS: dict[str, StructType] = {
    "assets": ASSETS_SCHEMA,
    "vulnerabilities": VULNERABILITIES_SCHEMA,
    "findings": FINDINGS_SCHEMA,
    "scans": SCANS_SCHEMA,
    "scan_results": SCAN_RESULTS_SCHEMA,
    "exports": EXPORTS_SCHEMA,
    "tags": TAGS_SCHEMA,
    "plugins": PLUGINS_SCHEMA,
    "policies": POLICIES_SCHEMA,
    "users": USERS_SCHEMA,
}

TABLE_METADATA: dict[str, dict] = {
    "assets": {
        "primary_keys": ["asset_uuid"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "vulnerabilities": {
        "primary_keys": ["finding_id"],
        "cursor_field": "indexed_at",
        "ingestion_type": "cdc",
    },
    "findings": {
        "primary_keys": ["finding_id"],
        "cursor_field": "indexed_at",
        "ingestion_type": "cdc",
    },
    "scans": {
        "primary_keys": ["scan_uuid"],
        "cursor_field": "last_modification_date",
        "ingestion_type": "cdc",
    },
    "scan_results": {
        "primary_keys": ["scan_uuid", "history_id"],
        "cursor_field": "last_modification_date",
        "ingestion_type": "cdc",
    },
    "exports": {
        "primary_keys": ["export_type", "export_uuid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "tags": {
        "primary_keys": ["tag_uuid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "plugins": {
        "primary_keys": ["plugin_id"],
        "cursor_field": "plugin_modification_date",
        "ingestion_type": "cdc",
    },
    "policies": {
        "primary_keys": ["policy_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "users": {
        "primary_keys": ["user_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}

SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())
