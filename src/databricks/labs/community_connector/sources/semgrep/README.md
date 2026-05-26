# Lakeflow Semgrep Connector

Tables:
- deployments
- projects
- findings
- vulnerabilities
- scans

This connector follows source-native Semgrep AppSec Platform semantics and preserves raw payloads for schema evolution.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
