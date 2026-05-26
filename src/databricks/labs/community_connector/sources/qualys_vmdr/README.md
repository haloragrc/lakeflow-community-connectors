# Lakeflow Qualys VMDR Connector

Tables:
- assets
- hosts
- detections
- vulnerabilities
- findings
- knowledgebase
- qids
- scans
- scan_results
- asset_groups

Auth: basic (`username`, `password`)

This connector preserves source-native VMDR semantics and includes `raw_payload` for schema evolution.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No Databricks workspace deployment validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
