# Lakeflow PagerDuty Connector

Tables:
- users
- services
- incidents
- escalation_policies
- oncalls

Source-native PagerDuty incident and response semantics are preserved with raw payload fields.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
