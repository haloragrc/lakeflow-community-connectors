"""Schemas and metadata for snyk connector."""

from pyspark.sql.types import StringType, StructField, StructType

ORGANIZATIONS_SCHEMA = StructType([
    StructField("org_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("group_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TARGETS_SCHEMA = StructType([
    StructField("target_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("target_type", StringType(), True),
    StructField("url", StringType(), True),
    StructField("integration_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

PROJECTS_SCHEMA = StructType([
    StructField("project_id", StringType(), False),
    StructField("target_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("project_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("lifecycle", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("last_tested_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FINDINGS_SCHEMA = StructType([
    StructField("finding_id", StringType(), False),
    StructField("issue_type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("target_id", StringType(), True),
    StructField("introduced_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VULNERABILITIES_SCHEMA = StructType([
    StructField("vulnerability_id", StringType(), False),
    StructField("cve", StringType(), True),
    StructField("issue_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("coordinates", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "organizations": ORGANIZATIONS_SCHEMA,
    "targets": TARGETS_SCHEMA,
    "projects": PROJECTS_SCHEMA,
    "findings": FINDINGS_SCHEMA,
    "vulnerabilities": VULNERABILITIES_SCHEMA,
}

TABLE_METADATA = {
    "organizations": {"primary_keys": ["org_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "targets": {"primary_keys": ["target_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "projects": {"primary_keys": ["project_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "findings": {"primary_keys": ["finding_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "vulnerabilities": {"primary_keys": ["vulnerability_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
