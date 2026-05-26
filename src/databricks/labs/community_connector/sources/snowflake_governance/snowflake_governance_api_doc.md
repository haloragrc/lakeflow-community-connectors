# Snowflake Governance API mapping notes

Reference docs used for table/field inspiration:

- Snowflake SQL metadata / SHOW and INFORMATION_SCHEMA surfaces for users, roles, grants, warehouses, databases, schemas, tables, stages
- Snowflake governance metadata (masking policies, network policies)
- Account usage views for login/query history

Implementation notes:

- Connector keeps source-native semantics and IDs.
- `raw_payload` is emitted for all tables.
- Tests run only against deterministic simulator fixtures.
