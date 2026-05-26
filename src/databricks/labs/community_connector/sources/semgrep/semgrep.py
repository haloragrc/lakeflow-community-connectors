"""Semgrep AppSec Platform connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
    first_present,
)
from databricks.labs.community_connector.sources.semgrep.semgrep_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class SemgrepLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_token = options.get("api_token")
        deployment_slug = options.get("deployment_slug")
        if not api_token or not deployment_slug:
            raise ValueError("semgrep requires api_token and deployment_slug")

        self.deployment_slug = deployment_slug
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://semgrep.dev"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {api_token}",
            },
        )
        self._page_size = int(options.get("page_size", "100"))
        self._init_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        del table_options
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        del table_options
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        del table_options
        start_offset = start_offset or {}

        if table_name == "deployments":
            payload = self.client.request_json("GET", f"/api/v1/deployments/{self.deployment_slug}")
            row = self._single(payload)
            records = [self._map_deployment(row)] if row else []
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "projects":
            rows = self._paged_rows(f"/api/v1/deployments/{self.deployment_slug}/projects")
            records = [self._map_project(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "findings":
            rows = self._paged_rows(f"/api/v1/deployments/{self.deployment_slug}/findings")
            records = [self._map_finding(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "vulnerabilities":
            rows = self._paged_rows(f"/api/v1/deployments/{self.deployment_slug}/findings")
            records = [self._map_vulnerability(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "scans":
            rows = self._paged_rows(f"/api/v1/deployments/{self.deployment_slug}/scans")
            records = [self._map_scan(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _single(self, payload: Any) -> dict[str, Any] | None:
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), dict):
                return payload["data"]
            if "id" in payload:
                return payload
        return None

    def _paged_rows(self, path: str) -> list[dict[str, Any]]:
        cursor: str | None = None
        out: list[dict[str, Any]] = []
        for _ in range(1000):
            params: dict[str, Any] = {"page_size": self._page_size}
            if cursor:
                params["cursor"] = cursor
            payload = self.client.request_json("GET", path, params=params)
            rows = self._rows(payload)
            out.extend(rows)
            cursor = self._next_cursor(payload)
            if not cursor:
                break
        return out

    def _rows(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            for key in ("data", "projects", "findings", "scans", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]
                if isinstance(value, dict) and isinstance(value.get("items"), list):
                    return [x for x in value["items"] if isinstance(x, dict)]
        return []

    def _next_cursor(self, payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        cursor = payload.get("cursor")
        if isinstance(cursor, str) and cursor:
            return cursor
        page = payload.get("page") if isinstance(payload.get("page"), dict) else {}
        next_cursor = page.get("next_cursor")
        if isinstance(next_cursor, str) and next_cursor:
            return next_cursor
        return None

    def _map_deployment(self, row: dict[str, Any]) -> dict[str, Any]:
        attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else row
        return {
            "deployment_slug": str(first_present(row.get("slug"), row.get("id"), self.deployment_slug)),
            "name": attrs.get("name"),
            "plan": attrs.get("plan"),
            "status": attrs.get("status"),
            "created_at": as_iso8601(first_present(attrs.get("created_at"), attrs.get("createdAt"))),
            "updated_at": as_iso8601(first_present(attrs.get("updated_at"), attrs.get("updatedAt"), attrs.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_project(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "project_id": str(first_present(row.get("id"), row.get("project_id"), "")),
            "name": first_present(row.get("name"), row.get("display_name")),
            "repository": first_present(row.get("repository"), row.get("repo"), row.get("full_name")),
            "branch": first_present(row.get("branch"), row.get("default_branch")),
            "status": row.get("status"),
            "last_scan_at": as_iso8601(first_present(row.get("last_scan_at"), row.get("last_scan_time"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("last_scan_at"), row.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_finding(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "finding_id": str(first_present(row.get("id"), row.get("finding_id"), "")),
            "project_id": first_present(row.get("project_id"), row.get("projectId")),
            "rule_id": first_present(row.get("rule_id"), row.get("ruleId")),
            "title": first_present(row.get("title"), row.get("message")),
            "severity": first_present(row.get("severity"), row.get("severity_level")),
            "status": first_present(row.get("status"), row.get("triage_state")),
            "category": first_present(row.get("category"), row.get("product")),
            "cve": first_present(row.get("cve"), row.get("cve_id")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("triaged_at"), row.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_vulnerability(self, row: dict[str, Any]) -> dict[str, Any]:
        vuln_id = str(first_present(row.get("id"), row.get("finding_id"), ""))
        return {
            "vulnerability_id": vuln_id,
            "finding_id": vuln_id,
            "project_id": first_present(row.get("project_id"), row.get("projectId")),
            "cve": first_present(row.get("cve"), row.get("cve_id")),
            "severity": first_present(row.get("severity"), row.get("severity_level")),
            "status": first_present(row.get("status"), row.get("triage_state")),
            "category": first_present(row.get("category"), row.get("product")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("triaged_at"), row.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_scan(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "scan_id": str(first_present(row.get("id"), row.get("scan_id"), "")),
            "project_id": first_present(row.get("project_id"), row.get("projectId")),
            "scan_type": first_present(row.get("scan_type"), row.get("product")),
            "status": row.get("status"),
            "started_at": as_iso8601(first_present(row.get("started_at"), row.get("startedAt"))),
            "completed_at": as_iso8601(first_present(row.get("completed_at"), row.get("completedAt"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("completed_at"), row.get("started_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _finalize_cdc(self, records: list[dict[str, Any]], start_offset: dict, cursor_field: str) -> tuple[Iterator[dict], dict]:
        if not records:
            return iter([]), start_offset or {}
        current = start_offset.get("cursor") if start_offset else None
        max_cursor = current
        for record in records:
            val = record.get(cursor_field)
            if val and (max_cursor is None or str(val) > str(max_cursor)):
                max_cursor = val
        if max_cursor is None:
            max_cursor = self._init_time
        if current is not None and str(max_cursor) <= str(current):
            return iter([]), start_offset
        return iter(records), {"cursor": max_cursor}
