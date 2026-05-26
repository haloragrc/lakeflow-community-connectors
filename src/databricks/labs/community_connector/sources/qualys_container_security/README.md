# Lakeflow Qualys Container Security Connector

Tables:
- containers
- container_images
- container_vulnerabilities
- registries
- sensors
- container_scan_results

Auth: Bearer token (`token`).

Source-native image/container evidence is preserved and includes `raw_payload`.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No Databricks workspace deployment validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
