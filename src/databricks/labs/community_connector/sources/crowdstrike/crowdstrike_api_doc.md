# CrowdStrike API Notes (Phase-1)

Primary API patterns used in this spike:
- OAuth token acquisition (`POST /oauth2/token`)
- Combined collection endpoints:
  - `GET /devices/combined/devices/v1`
  - `GET /detects/combined/detects/v1`
  - `GET /incidents/combined/incidents/v1`
  - `GET /spotlight/combined/vulnerabilities/v1`

Implementation is simulator-first using deterministic fake fixtures and no live credentials.
