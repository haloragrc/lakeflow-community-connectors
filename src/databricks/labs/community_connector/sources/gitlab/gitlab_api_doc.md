# GitLab API Notes (Phase-1)

Primary API resources used in this spike:
- `GET /api/v4/projects`
- `GET /api/v4/merge_requests`
- `GET /api/v4/issues`
- `GET /api/v4/projects/{id}/pipelines`
- `GET /api/v4/projects/{id}/vulnerabilities`

Notes:
- Pagination is page/per_page and surfaced via `X-Next-Page` response header.

This implementation is simulator-first with deterministic fake fixtures and no live credentials.
