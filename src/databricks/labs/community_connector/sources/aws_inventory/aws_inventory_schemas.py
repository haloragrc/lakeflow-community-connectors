"""Schemas and metadata for aws_inventory connector."""

from pyspark.sql.types import StringType, StructField, StructType

ACCOUNTS_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("account_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("status", StringType(), True),
    StructField("joined_method", StringType(), True),
    StructField("joined_timestamp", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

IAM_USERS_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("arn", StringType(), True),
    StructField("path", StringType(), True),
    StructField("create_date", StringType(), True),
    StructField("password_last_used", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

IAM_ROLES_SCHEMA = StructType([
    StructField("role_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("role_name", StringType(), True),
    StructField("arn", StringType(), True),
    StructField("path", StringType(), True),
    StructField("description", StringType(), True),
    StructField("create_date", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

IAM_POLICIES_SCHEMA = StructType([
    StructField("policy_arn", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("policy_name", StringType(), True),
    StructField("path", StringType(), True),
    StructField("default_version_id", StringType(), True),
    StructField("attachment_count", StringType(), True),
    StructField("create_date", StringType(), True),
    StructField("update_date", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

EC2_INSTANCES_SCHEMA = StructType([
    StructField("instance_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("instance_type", StringType(), True),
    StructField("state", StringType(), True),
    StructField("vpc_id", StringType(), True),
    StructField("subnet_id", StringType(), True),
    StructField("private_ip", StringType(), True),
    StructField("launch_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VPCS_SCHEMA = StructType([
    StructField("vpc_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("cidr_block", StringType(), True),
    StructField("is_default", StringType(), True),
    StructField("state", StringType(), True),
    StructField("owner_id", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SECURITY_GROUPS_SCHEMA = StructType([
    StructField("security_group_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("group_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("vpc_id", StringType(), True),
    StructField("owner_id", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

S3_BUCKETS_SCHEMA = StructType([
    StructField("bucket_name", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("arn", StringType(), True),
    StructField("region", StringType(), True),
    StructField("creation_date", StringType(), True),
    StructField("public_access_blocked", StringType(), True),
    StructField("versioning_status", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

KMS_KEYS_SCHEMA = StructType([
    StructField("key_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("arn", StringType(), True),
    StructField("region", StringType(), True),
    StructField("key_manager", StringType(), True),
    StructField("key_state", StringType(), True),
    StructField("creation_date", StringType(), True),
    StructField("enabled", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

CLOUDTRAIL_TRAILS_SCHEMA = StructType([
    StructField("trail_arn", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("home_region", StringType(), True),
    StructField("s3_bucket_name", StringType(), True),
    StructField("is_multi_region_trail", StringType(), True),
    StructField("log_file_validation_enabled", StringType(), True),
    StructField("kms_key_id", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

CONFIG_RECORDERS_SCHEMA = StructType([
    StructField("recorder_name", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("role_arn", StringType(), True),
    StructField("recording_group", StringType(), True),
    StructField("recording", StringType(), True),
    StructField("last_status", StringType(), True),
    StructField("last_status_change_time", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

GUARDDUTY_FINDINGS_SCHEMA = StructType([
    StructField("finding_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("detector_id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("title", StringType(), True),
    StructField("resource_type", StringType(), True),
    StructField("service_action", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "accounts": ACCOUNTS_SCHEMA,
    "iam_users": IAM_USERS_SCHEMA,
    "iam_roles": IAM_ROLES_SCHEMA,
    "iam_policies": IAM_POLICIES_SCHEMA,
    "ec2_instances": EC2_INSTANCES_SCHEMA,
    "vpcs": VPCS_SCHEMA,
    "security_groups": SECURITY_GROUPS_SCHEMA,
    "s3_buckets": S3_BUCKETS_SCHEMA,
    "kms_keys": KMS_KEYS_SCHEMA,
    "cloudtrail_trails": CLOUDTRAIL_TRAILS_SCHEMA,
    "config_recorders": CONFIG_RECORDERS_SCHEMA,
    "guardduty_findings": GUARDDUTY_FINDINGS_SCHEMA,
}

TABLE_METADATA = {
    "accounts": {"primary_keys": ["account_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "iam_users": {"primary_keys": ["user_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "iam_roles": {"primary_keys": ["role_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "iam_policies": {"primary_keys": ["policy_arn"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "ec2_instances": {"primary_keys": ["instance_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "vpcs": {"primary_keys": ["vpc_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "security_groups": {"primary_keys": ["security_group_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "s3_buckets": {"primary_keys": ["bucket_name"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "kms_keys": {"primary_keys": ["key_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "cloudtrail_trails": {"primary_keys": ["trail_arn"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "config_recorders": {"primary_keys": ["recorder_name"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "guardduty_findings": {"primary_keys": ["finding_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
