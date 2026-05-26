"""Schemas and metadata for sentinel_bounded connector."""

from pyspark.sql.types import StringType, StructField, StructType

INCIDENTS_SCHEMA = StructType([
    StructField("incident_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("labels", StringType(), True),
    StructField("created_time_utc", StringType(), True),
    StructField("last_modified_time_utc", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ALERTS_SCHEMA = StructType([
    StructField("alert_id", StringType(), False),
    StructField("alert_display_name", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("provider_name", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("start_time_utc", StringType(), True),
    StructField("end_time_utc", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ANALYTICS_RULES_SCHEMA = StructType([
    StructField("rule_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("enabled", StringType(), True),
    StructField("tactics", StringType(), True),
    StructField("query_frequency", StringType(), True),
    StructField("query_period", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ENTITIES_SCHEMA = StructType([
    StructField("entity_id", StringType(), False),
    StructField("entity_type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("properties", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

BOOKMARKS_SCHEMA = StructType([
    StructField("bookmark_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("query", StringType(), True),
    StructField("created", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "incidents": INCIDENTS_SCHEMA,
    "alerts": ALERTS_SCHEMA,
    "analytics_rules": ANALYTICS_RULES_SCHEMA,
    "entities": ENTITIES_SCHEMA,
    "bookmarks": BOOKMARKS_SCHEMA,
}

TABLE_METADATA = {
    "incidents": {"primary_keys": ["incident_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "alerts": {"primary_keys": ["alert_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "analytics_rules": {"primary_keys": ["rule_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "entities": {"primary_keys": ["entity_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "bookmarks": {"primary_keys": ["bookmark_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
