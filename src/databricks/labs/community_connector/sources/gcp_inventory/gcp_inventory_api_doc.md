# GCP Inventory API mapping notes

Reference docs used for table/field inspiration:

- Cloud Resource Manager organizations/folders/projects
- IAM service accounts and policy bindings
- Compute Engine instances/networks/firewalls
- Cloud Storage buckets
- Cloud KMS crypto keys
- Cloud Logging sinks
- Security Command Center findings

Implementation notes:

- Source-native semantics and IDs are preserved.
- `raw_payload` is included across tables for schema evolution.
- No live calls are used in tests; simulator fixtures are deterministic.
