"""Schemas and metadata for knowbe4 connector."""

from pyspark.sql.types import StringType, StructField, StructType

USERS_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("employee_number", StringType(), True),
    StructField("phish_prone_percentage", StringType(), True),
    StructField("joined_on", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

GROUPS_SCHEMA = StructType([
    StructField("group_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("member_count", StringType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

PHISHING_CAMPAIGNS_SCHEMA = StructType([
    StructField("campaign_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("send_duration", StringType(), True),
    StructField("last_phish_date", StringType(), True),
    StructField("scheduled_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TRAINING_CAMPAIGNS_SCHEMA = StructType([
    StructField("training_campaign_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("duration_type", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TRAINING_ENROLLMENTS_SCHEMA = StructType([
    StructField("enrollment_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("training_campaign_id", StringType(), True),
    StructField("module_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("enrollment_date", StringType(), True),
    StructField("completion_date", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "users": USERS_SCHEMA,
    "groups": GROUPS_SCHEMA,
    "phishing_campaigns": PHISHING_CAMPAIGNS_SCHEMA,
    "training_campaigns": TRAINING_CAMPAIGNS_SCHEMA,
    "training_enrollments": TRAINING_ENROLLMENTS_SCHEMA,
}

TABLE_METADATA = {
    "users": {"primary_keys": ["user_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "groups": {"primary_keys": ["group_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "phishing_campaigns": {"primary_keys": ["campaign_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "training_campaigns": {"primary_keys": ["training_campaign_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "training_enrollments": {"primary_keys": ["enrollment_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
