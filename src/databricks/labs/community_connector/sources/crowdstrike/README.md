# Lakeflow CrowdStrike Connector

Tables:
- hosts
- detections
- incidents
- vulnerabilities

Source-native Falcon semantics are preserved, including host and vulnerability linkage fields and raw payload columns.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
