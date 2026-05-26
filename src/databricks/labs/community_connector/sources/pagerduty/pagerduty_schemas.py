"""Schemas and metadata for pagerduty connector."""

from pyspark.sql.types import StringType, StructField, StructType

USERS_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("role", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("time_zone", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SERVICES_SCHEMA = StructType([
    StructField("service_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("auto_resolve_timeout", StringType(), True),
    StructField("acknowledgement_timeout", StringType(), True),
    StructField("escalation_policy_id", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

INCIDENTS_SCHEMA = StructType([
    StructField("incident_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("status", StringType(), True),
    StructField("urgency", StringType(), True),
    StructField("service_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("last_status_change_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ESCALATION_POLICIES_SCHEMA = StructType([
    StructField("escalation_policy_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("num_loops", StringType(), True),
    StructField("on_call_handoff_notifications", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

ONCALLS_SCHEMA = StructType([
    StructField("oncall_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("schedule_id", StringType(), True),
    StructField("escalation_policy_id", StringType(), True),
    StructField("start", StringType(), True),
    StructField("end", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "users": USERS_SCHEMA,
    "services": SERVICES_SCHEMA,
    "incidents": INCIDENTS_SCHEMA,
    "escalation_policies": ESCALATION_POLICIES_SCHEMA,
    "oncalls": ONCALLS_SCHEMA,
}

TABLE_METADATA = {
    "users": {"primary_keys": ["user_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "services": {"primary_keys": ["service_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "incidents": {"primary_keys": ["incident_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "escalation_policies": {"primary_keys": ["escalation_policy_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "oncalls": {"primary_keys": ["oncall_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
