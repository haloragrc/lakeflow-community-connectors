"""Schemas and metadata for wiz connector."""

from pyspark.sql.types import StringType, StructField, StructType

ASSETS_SCHEMA = StructType([
    StructField("asset_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("asset_type", StringType(), True),
    StructField("cloud_provider", StringType(), True),
    StructField("subscription_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FINDINGS_SCHEMA = StructType([
    StructField("finding_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("asset_id", StringType(), True),
    StructField("vulnerability_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VULNERABILITIES_SCHEMA = StructType([
    StructField("vulnerability_id", StringType(), False),
    StructField("cve", StringType(), True),
    StructField("vendor_severity", StringType(), True),
    StructField("cvss_score", StringType(), True),
    StructField("status", StringType(), True),
    StructField("asset_id", StringType(), True),
    StructField("discovered_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

INCIDENTS_SCHEMA = StructType([
    StructField("incident_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("source", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCAN_RUNS_SCHEMA = StructType([
    StructField("scan_run_id", StringType(), False),
    StructField("scan_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("completed_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

PROJECTS_SCHEMA = StructType([
    StructField("project_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("business_unit", StringType(), True),
    StructField("risk_profile", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "assets": ASSETS_SCHEMA,
    "findings": FINDINGS_SCHEMA,
    "vulnerabilities": VULNERABILITIES_SCHEMA,
    "incidents": INCIDENTS_SCHEMA,
    "scan_runs": SCAN_RUNS_SCHEMA,
    "projects": PROJECTS_SCHEMA,
}

TABLE_METADATA = {
    "assets": {"primary_keys": ["asset_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "findings": {"primary_keys": ["finding_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "vulnerabilities": {"primary_keys": ["vulnerability_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "incidents": {"primary_keys": ["incident_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "scan_runs": {"primary_keys": ["scan_run_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "projects": {"primary_keys": ["project_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
