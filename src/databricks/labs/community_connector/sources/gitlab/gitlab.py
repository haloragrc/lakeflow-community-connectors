"""GitLab connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.gitlab.gitlab_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import as_iso8601, first_present

import requests


class GitlabLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        private_token = options.get("private_token")
        if not private_token:
            raise ValueError("gitlab requires private_token")

        self.base_url = options.get("base_url", "https://gitlab.example.com").rstrip("/")
        self.timeout_seconds = int(options.get("timeout_seconds", "30"))
        self.page_size = int(options.get("page_size", "100"))
        self.max_projects = int(options.get("max_projects", "200"))

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "PRIVATE-TOKEN": private_token,
            }
        )
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

        if table_name == "projects":
            rows = self._paged_get("/api/v4/projects", params={"membership": "true", "order_by": "last_activity_at", "sort": "desc"})
            records = [self._map_project(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "merge_requests":
            rows = self._paged_get("/api/v4/merge_requests", params={"scope": "all", "order_by": "updated_at", "sort": "desc"})
            records = [self._map_merge_request(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "issues":
            rows = self._paged_get("/api/v4/issues", params={"scope": "all", "order_by": "updated_at", "sort": "desc"})
            records = [self._map_issue(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "pipelines":
            records: list[dict[str, Any]] = []
            for project in self._projects_limited():
                project_id = project.get("id")
                if project_id is None:
                    continue
                rows = self._paged_get(f"/api/v4/projects/{project_id}/pipelines", params={"order_by": "updated_at", "sort": "desc"})
                records.extend(self._map_pipeline(r, str(project_id)) for r in rows)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "vulnerabilities":
            records = []
            for project in self._projects_limited():
                project_id = project.get("id")
                if project_id is None:
                    continue
                rows = self._paged_get(f"/api/v4/projects/{project_id}/vulnerabilities", params={"order_by": "updated_at", "sort": "desc"})
                records.extend(self._map_vulnerability(r, str(project_id)) for r in rows)
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _paged_get(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = dict(params or {})
        params.setdefault("per_page", str(self.page_size))
        page = 1
        out: list[dict[str, Any]] = []

        for _ in range(1000):
            params["page"] = str(page)
            response = self.session.request(
                "GET",
                f"{self.base_url}{path}",
                params=params,
                timeout=self.timeout_seconds,
            )
            if response.status_code != 200:
                raise RuntimeError(f"GitLab API error {response.status_code} for GET {path}: {response.text}")
            body = response.json()
            if isinstance(body, list):
                rows = [x for x in body if isinstance(x, dict)]
            else:
                rows = []
            out.extend(rows)

            next_page = response.headers.get("X-Next-Page")
            if next_page:
                page = int(next_page)
                continue
            break

        return out

    def _projects_limited(self) -> list[dict[str, Any]]:
        rows = self._paged_get("/api/v4/projects", params={"membership": "true", "order_by": "last_activity_at", "sort": "desc"})
        if self.max_projects > 0:
            return rows[: self.max_projects]
        return rows

    def _map_project(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "project_id": str(row.get("id") or ""),
            "path_with_namespace": row.get("path_with_namespace"),
            "name": row.get("name"),
            "default_branch": row.get("default_branch"),
            "visibility": row.get("visibility"),
            "web_url": row.get("web_url"),
            "created_at": as_iso8601(row.get("created_at")),
            "last_activity_at": as_iso8601(row.get("last_activity_at")),
            "updated_at": as_iso8601(first_present(row.get("last_activity_at"), row.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_merge_request(self, row: dict[str, Any]) -> dict[str, Any]:
        author = row.get("author") if isinstance(row.get("author"), dict) else {}
        return {
            "merge_request_id": str(row.get("id") or ""),
            "project_id": str(row.get("project_id")) if row.get("project_id") is not None else None,
            "iid": str(row.get("iid")) if row.get("iid") is not None else None,
            "title": row.get("title"),
            "state": row.get("state"),
            "author_username": author.get("username"),
            "source_branch": row.get("source_branch"),
            "target_branch": row.get("target_branch"),
            "created_at": as_iso8601(row.get("created_at")),
            "updated_at": as_iso8601(row.get("updated_at")),
            "merged_at": as_iso8601(row.get("merged_at")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_pipeline(self, row: dict[str, Any], project_id: str) -> dict[str, Any]:
        return {
            "pipeline_id": str(row.get("id") or ""),
            "project_id": project_id,
            "status": row.get("status"),
            "ref": row.get("ref"),
            "sha": row.get("sha"),
            "source": row.get("source"),
            "created_at": as_iso8601(row.get("created_at")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_issue(self, row: dict[str, Any]) -> dict[str, Any]:
        author = row.get("author") if isinstance(row.get("author"), dict) else {}
        severity = row.get("severity")
        return {
            "issue_id": str(row.get("id") or ""),
            "project_id": str(row.get("project_id")) if row.get("project_id") is not None else None,
            "iid": str(row.get("iid")) if row.get("iid") is not None else None,
            "title": row.get("title"),
            "state": row.get("state"),
            "author_username": author.get("username"),
            "severity": str(severity) if severity is not None else None,
            "created_at": as_iso8601(row.get("created_at")),
            "updated_at": as_iso8601(row.get("updated_at")),
            "closed_at": as_iso8601(row.get("closed_at")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_vulnerability(self, row: dict[str, Any], project_id: str) -> dict[str, Any]:
        identifiers = row.get("identifiers") if isinstance(row.get("identifiers"), list) else []
        cve = None
        for ident in identifiers:
            if not isinstance(ident, dict):
                continue
            if str(ident.get("external_type", "")).upper() == "CVE":
                cve = ident.get("name")
                break

        scanner = row.get("scanner") if isinstance(row.get("scanner"), dict) else {}
        return {
            "vulnerability_id": str(row.get("id") or ""),
            "project_id": project_id,
            "title": row.get("title"),
            "severity": row.get("severity"),
            "state": row.get("state"),
            "report_type": row.get("report_type"),
            "scanner": scanner.get("name"),
            "cve": cve,
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("created_at"))),
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
