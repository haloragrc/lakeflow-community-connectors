"""Schemas and metadata for burp_enterprise connector."""

from pyspark.sql.types import StringType, StructField, StructType

ASSETS_SCHEMA = StructType([
    StructField("asset_id", StringType(), False),
    StructField("site_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("url", StringType(), True),
    StructField("environment", StringType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCANS_SCHEMA = StructType([
    StructField("scan_id", StringType(), False),
    StructField("site_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("scheduled_at", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("finished_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCAN_RESULTS_SCHEMA = StructType([
    StructField("scan_result_id", StringType(), False),
    StructField("scan_id", StringType(), True),
    StructField("site_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("issue_count", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FINDINGS_SCHEMA = StructType([
    StructField("finding_id", StringType(), False),
    StructField("scan_id", StringType(), True),
    StructField("site_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("confidence", StringType(), True),
    StructField("status", StringType(), True),
    StructField("path", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCAN_CONFIGURATIONS_SCHEMA = StructType([
    StructField("scan_configuration_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("crawl_profile", StringType(), True),
    StructField("audit_profile", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "assets": ASSETS_SCHEMA,
    "scans": SCANS_SCHEMA,
    "scan_results": SCAN_RESULTS_SCHEMA,
    "findings": FINDINGS_SCHEMA,
    "scan_configurations": SCAN_CONFIGURATIONS_SCHEMA,
}

TABLE_METADATA = {
    "assets": {"primary_keys": ["asset_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "scans": {"primary_keys": ["scan_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "scan_results": {"primary_keys": ["scan_result_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "findings": {"primary_keys": ["finding_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "scan_configurations": {"primary_keys": ["scan_configuration_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
