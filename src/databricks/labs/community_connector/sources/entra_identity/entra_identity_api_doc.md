# Entra Identity API Notes (Phase-1)

Primary Microsoft Graph endpoints used:
- `GET /v1.0/users`
- `GET /v1.0/groups`
- `GET /v1.0/groups/{id}/members`
- `GET /v1.0/servicePrincipals`
- `GET /v1.0/servicePrincipals/appRoleAssignedTo`
- `GET /v1.0/directoryRoles`

Authentication:
- `Authorization: Bearer <access_token>`

Simulator-first deterministic fixtures only in this phase.
