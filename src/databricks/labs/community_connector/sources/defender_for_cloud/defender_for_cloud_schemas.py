"""Schemas and metadata for defender_for_cloud connector."""

from pyspark.sql.types import StringType, StructField, StructType

ASSETS_SCHEMA = StructType([
    StructField("asset_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("resource_group", StringType(), True),
    StructField("subscription_id", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

FINDINGS_SCHEMA = StructType([
    StructField("finding_id", StringType(), False),
    StructField("assessment_key", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("status_code", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("remediation_description", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

INCIDENTS_SCHEMA = StructType([
    StructField("incident_id", StringType(), False),
    StructField("alert_display_name", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("time_generated", StringType(), True),
    StructField("compromised_entity", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SECURE_SCORES_SCHEMA = StructType([
    StructField("secure_score_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("current", StringType(), True),
    StructField("max", StringType(), True),
    StructField("percentage", StringType(), True),
    StructField("weight", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

RECOMMENDATIONS_SCHEMA = StructType([
    StructField("recommendation_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("state", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "assets": ASSETS_SCHEMA,
    "findings": FINDINGS_SCHEMA,
    "incidents": INCIDENTS_SCHEMA,
    "secure_scores": SECURE_SCORES_SCHEMA,
    "recommendations": RECOMMENDATIONS_SCHEMA,
}

TABLE_METADATA = {
    "assets": {"primary_keys": ["asset_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "findings": {"primary_keys": ["finding_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "incidents": {"primary_keys": ["incident_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "secure_scores": {"primary_keys": ["secure_score_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "recommendations": {"primary_keys": ["recommendation_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
