"""Schemas and metadata for gitlab connector."""

from pyspark.sql.types import StringType, StructField, StructType

PROJECTS_SCHEMA = StructType([
    StructField("project_id", StringType(), False),
    StructField("path_with_namespace", StringType(), True),
    StructField("name", StringType(), True),
    StructField("default_branch", StringType(), True),
    StructField("visibility", StringType(), True),
    StructField("web_url", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("last_activity_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

MERGE_REQUESTS_SCHEMA = StructType([
    StructField("merge_request_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("iid", StringType(), True),
    StructField("title", StringType(), True),
    StructField("state", StringType(), True),
    StructField("author_username", StringType(), True),
    StructField("source_branch", StringType(), True),
    StructField("target_branch", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("merged_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

PIPELINES_SCHEMA = StructType([
    StructField("pipeline_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("ref", StringType(), True),
    StructField("sha", StringType(), True),
    StructField("source", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ISSUES_SCHEMA = StructType([
    StructField("issue_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("iid", StringType(), True),
    StructField("title", StringType(), True),
    StructField("state", StringType(), True),
    StructField("author_username", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("closed_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VULNERABILITIES_SCHEMA = StructType([
    StructField("vulnerability_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("state", StringType(), True),
    StructField("report_type", StringType(), True),
    StructField("scanner", StringType(), True),
    StructField("cve", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "projects": PROJECTS_SCHEMA,
    "merge_requests": MERGE_REQUESTS_SCHEMA,
    "pipelines": PIPELINES_SCHEMA,
    "issues": ISSUES_SCHEMA,
    "vulnerabilities": VULNERABILITIES_SCHEMA,
}

TABLE_METADATA = {
    "projects": {"primary_keys": ["project_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "merge_requests": {"primary_keys": ["merge_request_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "pipelines": {"primary_keys": ["pipeline_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "issues": {"primary_keys": ["issue_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "vulnerabilities": {"primary_keys": ["vulnerability_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
