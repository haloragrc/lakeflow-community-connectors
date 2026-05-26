# Databricks Governance connector

`databricks_governance` extracts source-native account/workspace governance metadata and audit signals.

Implemented table families:

- Identities: `account_users`, `account_groups`, `group_memberships`, `service_principals`
- Workspace compute/orchestration: `workspaces`, `clusters`, `jobs`, `job_runs`
- Unity Catalog governance: `uc_catalogs`, `uc_schemas`, `uc_tables`, `uc_grants`
- Audit signals: `audit_logs`

This phase-1 spike is simulator-first and does not perform credentialed validation.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No Databricks workspace deployment validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
