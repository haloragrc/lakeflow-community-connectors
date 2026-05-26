# Lakeflow Snyk Connector

Tables:
- organizations
- targets
- projects
- findings
- vulnerabilities

This connector follows Snyk REST source-native semantics and preserves raw payloads for schema evolution.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
