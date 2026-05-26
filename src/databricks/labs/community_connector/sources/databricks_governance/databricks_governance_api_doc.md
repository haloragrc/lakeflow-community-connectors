# Databricks Governance API mapping notes

Reference docs used for table/field inspiration:

- Databricks Account SCIM / account APIs (users, groups, service principals, workspaces)
- Workspace APIs (clusters, jobs/runs)
- Unity Catalog APIs (catalogs, schemas, tables, grants)
- Audit log/event schemas

Implementation notes:

- Connector preserves source-native semantics and includes `raw_payload` fields.
- Deterministic simulator fixtures are used for all tests in this private spike.
