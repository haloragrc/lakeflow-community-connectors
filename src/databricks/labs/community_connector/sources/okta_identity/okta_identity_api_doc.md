# Okta Identity API Notes (Phase-1)

Primary endpoints used:
- `GET /api/v1/users`
- `GET /api/v1/groups`
- `GET /api/v1/groups/{groupId}/users`
- `GET /api/v1/apps`
- `GET /api/v1/apps/{appId}/users`
- `GET /api/v1/iam/roles`
- `GET /api/v1/users/{userId}/factors`

Authentication:
- `Authorization: SSWS <api_token>`

Simulator-first deterministic fixtures only in this phase.
