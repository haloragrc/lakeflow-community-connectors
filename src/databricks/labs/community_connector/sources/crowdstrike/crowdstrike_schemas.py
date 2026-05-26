"""Schemas and metadata for crowdstrike connector."""

from pyspark.sql.types import StringType, StructField, StructType

HOSTS_SCHEMA = StructType([
    StructField("host_id", StringType(), False),
    StructField("hostname", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("os_version", StringType(), True),
    StructField("status", StringType(), True),
    StructField("first_seen", StringType(), True),
    StructField("last_seen", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

DETECTIONS_SCHEMA = StructType([
    StructField("detection_id", StringType(), False),
    StructField("status", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("behavior_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

INCIDENTS_SCHEMA = StructType([
    StructField("incident_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("host_ids", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VULNERABILITIES_SCHEMA = StructType([
    StructField("vulnerability_id", StringType(), False),
    StructField("cve", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("first_seen", StringType(), True),
    StructField("last_seen", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "hosts": HOSTS_SCHEMA,
    "detections": DETECTIONS_SCHEMA,
    "incidents": INCIDENTS_SCHEMA,
    "vulnerabilities": VULNERABILITIES_SCHEMA,
}

TABLE_METADATA = {
    "hosts": {"primary_keys": ["host_id"], "cursor_field": "last_seen", "ingestion_type": "cdc"},
    "detections": {"primary_keys": ["detection_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "incidents": {"primary_keys": ["incident_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "vulnerabilities": {"primary_keys": ["vulnerability_id"], "cursor_field": "last_seen", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
