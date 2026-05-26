# AWS Inventory API mapping notes

Reference docs used for field/table inspiration:

- AWS Organizations APIs (accounts)
- IAM APIs (`ListUsers`, `ListRoles`, `ListPolicies`)
- EC2 APIs (`DescribeInstances`, `DescribeVpcs`, `DescribeSecurityGroups`)
- S3 APIs (`ListBuckets` and related metadata)
- KMS APIs (`ListKeys`, `DescribeKey`)
- CloudTrail APIs (`DescribeTrails`)
- AWS Config APIs (`DescribeConfigurationRecorders`, recorder status)
- GuardDuty APIs (`ListFindings`, `GetFindings`)

Implementation notes for this repo:

- Connector emits source-native records, IDs, and timestamps where available.
- Pagination and cursor semantics are implemented connector-side with best-effort `next_token` support.
- Simulator corpus uses deterministic fake values only.
