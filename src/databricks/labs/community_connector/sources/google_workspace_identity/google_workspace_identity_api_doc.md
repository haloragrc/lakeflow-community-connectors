# Google Workspace Identity API Notes (Phase-1)

Primary Admin SDK Directory API endpoints used:
- `GET /admin/directory/v1/users`
- `GET /admin/directory/v1/groups`
- `GET /admin/directory/v1/groups/{groupKey}/members`
- `GET /admin/directory/v1/customer/{customer}/roles`
- `GET /admin/directory/v1/customer/{customer}/roleassignments`

This implementation is simulator-first with deterministic fake fixtures and no live credentials.
