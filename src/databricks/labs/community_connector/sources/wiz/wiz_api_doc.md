# Wiz API Notes (Phase-1)

Primary API patterns used in this spike:
- OAuth client-credentials token request (`POST /oauth/token`)
- GraphQL reads (`POST /graphql`) with table-specific operation names:
  - `AssetsTable`
  - `FindingsTable`
  - `VulnerabilitiesTable`
  - `IncidentsTable`
  - `ScanRunsTable`
  - `ProjectsTable`

This implementation is simulator-first with deterministic fake fixtures and no live credentials.
