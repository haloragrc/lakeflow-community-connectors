# AWS Inventory connector

`aws_inventory` provides source-native AWS inventory and governance extraction for Lakeflow Connect.

Implemented table families in this phase-1 simulator-first spike:

- Identity/IAM: `accounts`, `iam_users`, `iam_roles`, `iam_policies`
- Compute/network/storage: `ec2_instances`, `vpcs`, `security_groups`, `s3_buckets`
- Security/governance/audit: `kms_keys`, `cloudtrail_trails`, `config_recorders`, `guardduty_findings`

Notes:

- This connector preserves source-native fields and includes `raw_payload` for forward schema evolution.
- No credentials are used in tests; deterministic simulator fixtures are used for all tables.
- This is a private spike and intentionally excludes any writeback/remediation features.
