# Google Workspace Drive API Notes (Phase-1)

Primary Google Drive API endpoints used:
- `GET /drive/v3/files`
- `GET /drive/v3/files/{fileId}/permissions`
- `GET /drive/v3/drives`

Notes:
- Uses all-drives options for shared drive visibility where available.

This implementation is simulator-first with deterministic fake fixtures and no live credentials.
