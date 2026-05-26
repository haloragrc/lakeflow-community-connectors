"""Schemas and metadata for tenable_sc connector."""

from pyspark.sql.types import LongType, StringType, StructField, StructType

ASSETS_SCHEMA = StructType([
    StructField("asset_id", LongType(), False),
    StructField("ip", StringType(), True),
    StructField("dns_name", StringType(), True),
    StructField("netbios_name", StringType(), True),
    StructField("repository_id", LongType(), True),
    StructField("repository_name", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

REPOSITORIES_SCHEMA = StructType([
    StructField("repository_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("type", StringType(), True),
    StructField("data_format", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FINDINGS_SCHEMA = StructType([
    StructField("finding_id", StringType(), False),
    StructField("plugin_id", LongType(), True),
    StructField("severity", StringType(), True),
    StructField("state", StringType(), True),
    StructField("asset_id", LongType(), True),
    StructField("repository_id", LongType(), True),
    StructField("first_seen", StringType(), True),
    StructField("last_seen", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCANS_SCHEMA = StructType([
    StructField("scan_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("status", StringType(), True),
    StructField("repository_id", LongType(), True),
    StructField("owner", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCAN_RESULTS_SCHEMA = StructType([
    StructField("scan_result_id", LongType(), False),
    StructField("scan_id", LongType(), True),
    StructField("repository_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("import_status", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("finish_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "assets": ASSETS_SCHEMA,
    "repositories": REPOSITORIES_SCHEMA,
    "findings": FINDINGS_SCHEMA,
    "scans": SCANS_SCHEMA,
    "scan_results": SCAN_RESULTS_SCHEMA,
}

TABLE_METADATA = {
    "assets": {"primary_keys": ["asset_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "repositories": {"primary_keys": ["repository_id"], "cursor_field": None, "ingestion_type": "snapshot"},
    "findings": {"primary_keys": ["finding_id"], "cursor_field": "last_seen", "ingestion_type": "cdc"},
    "scans": {"primary_keys": ["scan_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "scan_results": {"primary_keys": ["scan_result_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
