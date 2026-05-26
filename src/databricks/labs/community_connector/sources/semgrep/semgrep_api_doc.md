# Semgrep API Notes (Phase-1)

Primary API scope used in this spike:
- Deployment endpoints under `/api/v1/deployments/{slug}`
- Findings endpoint (`/api/v1/deployments/{slug}/findings`)
- Project and scan listing under deployment scope

Notes:
- Semgrep API supports pagination (`page` / `page_size` and cursor-based variants depending on endpoint).

This implementation is simulator-first with deterministic fake fixtures and no live credentials.
