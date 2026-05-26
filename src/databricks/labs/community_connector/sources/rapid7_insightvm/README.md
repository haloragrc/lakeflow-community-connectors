# Lakeflow Rapid7 InsightVM Connector

Tables:
- assets
- vulnerabilities
- findings
- scans
- scan_results
- sites

This connector keeps InsightVM source-native table semantics and emits raw payload fields for schema evolution.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
