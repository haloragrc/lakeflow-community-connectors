# Sentinel Bounded API Notes (Phase-1)

Primary Microsoft Sentinel (SecurityInsights) endpoints used:
- `GET .../providers/Microsoft.SecurityInsights/incidents`
- `GET .../providers/Microsoft.SecurityInsights/alerts`
- `GET .../providers/Microsoft.SecurityInsights/alertRules`
- `GET .../providers/Microsoft.SecurityInsights/entities`
- `GET .../providers/Microsoft.SecurityInsights/bookmarks`

This implementation is simulator-first with deterministic fake fixtures and no live credentials.
