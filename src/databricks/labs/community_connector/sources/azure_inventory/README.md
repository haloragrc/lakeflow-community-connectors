# Azure Inventory connector

`azure_inventory` extracts source-native Azure resource, identity, governance, and activity inventory into Lakeflow.

Implemented table families in this phase-1 simulator-first spike:

- Subscription/resource inventory: `subscriptions`, `resource_groups`
- Compute/network/storage: `virtual_machines`, `virtual_networks`, `network_security_groups`, `storage_accounts`
- IAM/governance/posture: `role_assignments`, `policy_assignments`, `key_vaults`, `defender_assessments`
- Audit/activity: `activity_logs`

All tests run against deterministic simulator fixtures; no live credentials are required.
