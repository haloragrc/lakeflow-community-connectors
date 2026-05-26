# Lakeflow Okta Identity Connector

Tables:
- users
- groups
- memberships
- app_assignments
- roles
- factors

Source-native identity and access semantics are preserved with `raw_payload` capture.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
