# Lakeflow GitLab Connector

Tables:
- projects
- merge_requests
- pipelines
- issues
- vulnerabilities

Source-native GitLab semantics are preserved, including per-project vulnerability and pipeline linkage.

## Validation scope

- Simulator-first connector spike using deterministic fake fixtures.
- No live credential validation is performed in this branch.
- No production-readiness or production-accuracy claims are made.
