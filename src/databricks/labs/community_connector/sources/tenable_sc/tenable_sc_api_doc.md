# Tenable.sc API Notes (Phase-1)

Primary endpoints used in this spike:
- `GET /rest/asset`
- `GET /rest/repository`
- `GET /rest/vuln`
- `GET /rest/scan`
- `GET /rest/scanResult`

Authentication:
- API key header format: `x-apikey: accesskey=<...>; secretkey=<...>`

This spike is simulator-first with deterministic fixtures and no live credentials.
