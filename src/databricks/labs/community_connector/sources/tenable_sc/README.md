# Lakeflow Tenable.sc Connector

Tables:
- assets
- repositories
- findings
- scans
- scan_results

Uses source-native Tenable Security Center semantics and includes `raw_payload` columns for schema evolution.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
