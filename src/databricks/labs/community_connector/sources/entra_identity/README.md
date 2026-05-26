# Lakeflow Entra Identity Connector

Tables:
- users
- groups
- memberships
- service_principals
- app_role_assignments
- directory_roles

Source-native identity and role-assignment semantics are preserved with `raw_payload`.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
