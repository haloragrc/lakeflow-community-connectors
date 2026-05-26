# Lakeflow Sentinel Bounded Connector

Tables:
- incidents
- alerts
- analytics_rules
- entities
- bookmarks

This connector preserves source-native Microsoft Sentinel workspace-scoped semantics with raw payload fields.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
