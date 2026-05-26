# Defender For Cloud API Notes (Phase-1)

Primary Azure management endpoints used:
- `GET /subscriptions/{subscriptionId}/resources`
- `GET /subscriptions/{subscriptionId}/providers/Microsoft.Security/assessments`
- `GET /subscriptions/{subscriptionId}/providers/Microsoft.Security/alerts`
- `GET /subscriptions/{subscriptionId}/providers/Microsoft.Security/secureScores`
- `GET /subscriptions/{subscriptionId}/providers/Microsoft.Security/recommendations`

This implementation is simulator-first with deterministic fake fixtures and no live credentials.
