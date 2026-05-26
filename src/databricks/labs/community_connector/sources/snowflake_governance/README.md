# Snowflake Governance connector

`snowflake_governance` provides source-native Snowflake governance, identity, privilege, inventory, and audit extraction.

Implemented table families:

- Identity/access: `users`, `roles`, `role_grants`, `network_policies`
- Platform inventory: `accounts`, `warehouses`, `databases`, `schemas`, `tables`, `stages`
- Data governance/audit: `masking_policies`, `login_history`, `query_history`

This phase-1 private spike is simulator-first and uses deterministic fake fixtures only.
