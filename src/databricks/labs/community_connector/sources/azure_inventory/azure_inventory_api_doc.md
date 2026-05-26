# Azure Inventory API mapping notes

Reference docs used for table/field inspiration:

- Azure Resource Manager (`/subscriptions`, resource groups, resources)
- Microsoft Compute (`virtualMachines`)
- Microsoft Network (`virtualNetworks`, `networkSecurityGroups`)
- Microsoft Storage (`storageAccounts`)
- Microsoft Key Vault (`vaults`)
- Microsoft Authorization (`roleAssignments`)
- Microsoft Policy (`policyAssignments`)
- Azure Monitor Activity Logs
- Microsoft Defender for Cloud assessments

Implementation notes:

- Source-native semantics are preserved; no Halora lifecycle normalization is performed in connector code.
- `raw_payload` is included across tables for schema evolution.
- Simulator fixtures use fake deterministic values only.
