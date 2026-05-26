"""Schemas and metadata for semgrep connector."""

from pyspark.sql.types import StringType, StructField, StructType

DEPLOYMENTS_SCHEMA = StructType([
    StructField("deployment_slug", StringType(), False),
    StructField("name", StringType(), True),
    StructField("plan", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

PROJECTS_SCHEMA = StructType([
    StructField("project_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("repository", StringType(), True),
    StructField("branch", StringType(), True),
    StructField("status", StringType(), True),
    StructField("last_scan_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FINDINGS_SCHEMA = StructType([
    StructField("finding_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("rule_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("category", StringType(), True),
    StructField("cve", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VULNERABILITIES_SCHEMA = StructType([
    StructField("vulnerability_id", StringType(), False),
    StructField("finding_id", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("cve", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("category", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCANS_SCHEMA = StructType([
    StructField("scan_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("scan_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("completed_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "deployments": DEPLOYMENTS_SCHEMA,
    "projects": PROJECTS_SCHEMA,
    "findings": FINDINGS_SCHEMA,
    "vulnerabilities": VULNERABILITIES_SCHEMA,
    "scans": SCANS_SCHEMA,
}

TABLE_METADATA = {
    "deployments": {"primary_keys": ["deployment_slug"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "projects": {"primary_keys": ["project_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "findings": {"primary_keys": ["finding_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "vulnerabilities": {"primary_keys": ["vulnerability_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "scans": {"primary_keys": ["scan_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
