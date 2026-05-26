# GCP Inventory connector

`gcp_inventory` extracts source-native Google Cloud inventory, IAM, governance, and security posture records.

Implemented table families in this phase-1 simulator-first spike:

- Org/project hierarchy: `organizations`, `folders`, `projects`
- IAM: `iam_service_accounts`, `iam_policy_bindings`
- Compute/network/storage: `compute_instances`, `vpc_networks`, `firewall_rules`, `storage_buckets`
- Governance/security/audit signals: `kms_crypto_keys`, `logging_sinks`, `scc_findings`

All tests use deterministic fake simulator fixtures only.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No Databricks workspace deployment validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
