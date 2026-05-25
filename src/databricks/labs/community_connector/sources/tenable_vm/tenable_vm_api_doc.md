# Tenable VM API Notes (Phase-1, no credentials)

This connector targets Tenable Vulnerability Management (legacy Tenable.io) API surfaces that support source-native ingestion for Lakeflow.

## Primary Endpoints Used

- Assets export (v2 request + status + chunk)
  - `POST /assets/v2/export`
  - `GET /assets/export/{export_uuid}/status`
  - `GET /assets/export/{export_uuid}/chunks/{chunk_id}`
- Vulnerability export (request + status + chunk)
  - `POST /vulns/export`
  - `GET /vulns/export/{export_uuid}/status`
  - `GET /vulns/export/{export_uuid}/chunks/{chunk_id}`
- Scan inventory and scan history
  - `GET /scans`
  - `GET /scans/{scan_id}/history`
- Export job listing
  - `GET /assets/export/status`
  - `GET /vulns/export/status`
- Tags, plugins, policies, users
  - `GET /tags/values`
  - `GET /plugins/plugin`
  - `GET /policies`
  - `GET /users`

## Source-native Table Mapping

- `assets` -> asset export chunks
- `vulnerabilities` -> vulnerability export chunks
- `findings` -> vulnerability export chunks (same source model, separate table for downstream semantics)
- `scans` -> scan list endpoint
- `scan_results` -> per-scan history endpoint
- `exports` -> export jobs listing endpoints
- `tags` -> tag values endpoint
- `plugins` -> plugins listing endpoint
- `policies` -> policy listing endpoint
- `users` -> users listing endpoint

## Pagination / Cursor Strategy

- Export-backed tables use async job polling + chunk download.
- `scans` and `plugins` are treated as cursor-based CDC via source timestamps.
- `scan_results` uses scan history fan-out with offset/limit pagination.
- Snapshot tables return full current-state list per read cycle.

## Guardrails

- No credentials in repo.
- No tenant/customer data in fixtures.
- No Tenable.sc / Security Center scope in this connector.
