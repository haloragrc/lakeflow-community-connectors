"""Schemas and metadata for rapid7_insightvm connector."""

from pyspark.sql.types import StringType, StructField, StructType

ASSETS_SCHEMA = StructType([
    StructField("asset_id", StringType(), False),
    StructField("host_name", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("os", StringType(), True),
    StructField("risk_score", StringType(), True),
    StructField("site_id", StringType(), True),
    StructField("last_assessed_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VULNERABILITIES_SCHEMA = StructType([
    StructField("vulnerability_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("cvss_score", StringType(), True),
    StructField("cve", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FINDINGS_SCHEMA = StructType([
    StructField("finding_id", StringType(), False),
    StructField("asset_id", StringType(), True),
    StructField("vulnerability_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("proof", StringType(), True),
    StructField("first_discovered", StringType(), True),
    StructField("last_discovered", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCANS_SCHEMA = StructType([
    StructField("scan_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("engine_id", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("finished_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCAN_RESULTS_SCHEMA = StructType([
    StructField("scan_result_id", StringType(), False),
    StructField("scan_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("asset_count", StringType(), True),
    StructField("vulnerability_count", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SITES_SCHEMA = StructType([
    StructField("site_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("importance", StringType(), True),
    StructField("risk_score", StringType(), True),
    StructField("site_type", StringType(), True),
    StructField("last_scan_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "assets": ASSETS_SCHEMA,
    "vulnerabilities": VULNERABILITIES_SCHEMA,
    "findings": FINDINGS_SCHEMA,
    "scans": SCANS_SCHEMA,
    "scan_results": SCAN_RESULTS_SCHEMA,
    "sites": SITES_SCHEMA,
}

TABLE_METADATA = {
    "assets": {"primary_keys": ["asset_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "vulnerabilities": {"primary_keys": ["vulnerability_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "findings": {"primary_keys": ["finding_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "scans": {"primary_keys": ["scan_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "scan_results": {"primary_keys": ["scan_result_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "sites": {"primary_keys": ["site_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
