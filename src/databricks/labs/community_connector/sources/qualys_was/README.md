# Lakeflow Qualys WAS Connector

Tables:
- web_apps
- web_findings
- was_scans
- was_scan_results

Auth: basic (`username`, `password`).

Source-native WAS semantics are preserved and include `raw_payload`.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No Databricks workspace deployment validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
