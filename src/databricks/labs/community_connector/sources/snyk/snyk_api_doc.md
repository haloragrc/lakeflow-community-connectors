# Snyk API Notes (Phase-1)

Primary REST endpoints used in this spike:
- `GET /rest/orgs/{org_id}`
- `GET /rest/orgs/{org_id}/targets`
- `GET /rest/orgs/{org_id}/projects`
- `GET /rest/orgs/{org_id}/issues`

Notes:
- Snyk REST is JSON:API and versioned per endpoint.
- Cursor pagination uses `starting_after` / `ending_before`.

This implementation is simulator-first with deterministic fake fixtures and no live credentials.
