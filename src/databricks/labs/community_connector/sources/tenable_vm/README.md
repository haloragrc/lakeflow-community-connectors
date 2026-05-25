# Lakeflow Tenable VM Community Connector

This connector ingests source-native data from **Tenable Vulnerability Management** (legacy Tenable.io) APIs using the `LakeflowConnect` interface.

## Connection Parameters

| Name | Required | Description |
|---|---|---|
| `access_key` | yes | Tenable VM API access key |
| `secret_key` | yes | Tenable VM API secret key |
| `base_url` | no | API base URL (default: `https://cloud.tenable.com`) |
| `timeout_seconds` | no | HTTP timeout (default: `30`) |
| `chunk_size` | no | Export batch size hint (default: `1000`) |
| `max_poll_attempts` | no | Export status polling attempts (default: `20`) |
| `poll_interval_seconds` | no | Delay between polling attempts (default: `0.1`) |

`externalOptionsAllowList` values for table-level options:

`start_unix_ts,start_date,state,plugin_page_size,plugin_page`

## Supported Tables

- `assets` (cdc)
- `vulnerabilities` (cdc)
- `findings` (cdc)
- `scans` (cdc)
- `scan_results` (cdc)
- `exports` (snapshot)
- `tags` (snapshot)
- `plugins` (cdc)
- `policies` (snapshot)
- `users` (snapshot)

## Table Option Notes

- `assets` / `vulnerabilities` / `findings` / `scans` / `scan_results` / `plugins`
  - `start_unix_ts` or `start_date` sets an initial cursor when no checkpoint exists.
- `vulnerabilities` / `findings`
  - `state` optionally constrains export requests.
- `plugins`
  - `plugin_page_size`, `plugin_page` control list request pagination inputs.

## Implementation Notes

- Export-backed tables use asynchronous Tenable export jobs (`request -> status -> chunks`).
- `scan_results` is sourced from `GET /scans/{scan_id}/history` fan-out.
- Records preserve source-native semantics and include `raw_payload` for schema evolution.

## Out of Scope (This Connector)

- Tenable.sc / Security Center
- Writeback or remediation actions
- Credential validation fixtures in-repo
