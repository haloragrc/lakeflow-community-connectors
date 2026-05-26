"""Schemas and metadata for datadog connector."""

from pyspark.sql.types import StringType, StructField, StructType

ASSETS_SCHEMA = StructType([
    StructField("host_id", StringType(), False),
    StructField("host_name", StringType(), True),
    StructField("aliases", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("agent_version", StringType(), True),
    StructField("last_reported_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

INCIDENTS_SCHEMA = StructType([
    StructField("incident_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("state", StringType(), True),
    StructField("customer_impact_scope", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("modified_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

MONITORS_SCHEMA = StructType([
    StructField("monitor_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("monitor_state", StringType(), True),
    StructField("query", StringType(), True),
    StructField("overall_state_modified", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SIGNALS_SCHEMA = StructType([
    StructField("signal_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("signal_type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SERVICES_SCHEMA = StructType([
    StructField("service_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("app", StringType(), True),
    StructField("team", StringType(), True),
    StructField("lifecycle", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "assets": ASSETS_SCHEMA,
    "incidents": INCIDENTS_SCHEMA,
    "monitors": MONITORS_SCHEMA,
    "signals": SIGNALS_SCHEMA,
    "services": SERVICES_SCHEMA,
}

TABLE_METADATA = {
    "assets": {"primary_keys": ["host_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "incidents": {"primary_keys": ["incident_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "monitors": {"primary_keys": ["monitor_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "signals": {"primary_keys": ["signal_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "services": {"primary_keys": ["service_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
