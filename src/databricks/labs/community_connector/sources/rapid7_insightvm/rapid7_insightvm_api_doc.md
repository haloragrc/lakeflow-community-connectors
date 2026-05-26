# Rapid7 InsightVM API Notes (Phase-1)

Primary REST resources used in this spike:
- `GET /api/3/assets`
- `GET /api/3/vulnerabilities`
- `GET /api/3/vulnerability_findings`
- `GET /api/3/scans`
- `GET /api/3/scan_results`
- `GET /api/3/sites`

Pagination is modeled using `page` and `size` request parameters with page metadata support.
This is simulator-first with deterministic fake fixtures and no live credentials.
