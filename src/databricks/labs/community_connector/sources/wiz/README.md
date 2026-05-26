# Lakeflow Wiz Connector

Tables:
- assets
- findings
- vulnerabilities
- incidents
- scan_runs
- projects

This connector uses source-native Wiz GraphQL semantics and preserves raw response payloads for schema evolution.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
