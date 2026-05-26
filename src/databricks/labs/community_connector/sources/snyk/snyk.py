"""Snyk REST API connector."""

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
from databricks.labs.community_connector.sources.snyk.snyk_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class SnykLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_token = options.get("api_token")
        org_id = options.get("org_id")
        if not api_token or not org_id:
            raise ValueError("snyk requires api_token and org_id")

        self.org_id = org_id
        self.api_version = options.get("api_version", "2025-11-05")
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://api.snyk.io/rest"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/vnd.api+json",
                "Content-Type": "application/vnd.api+json",
                "Authorization": f"Token {api_token}",
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

        if table_name == "organizations":
            row = self._request_json("GET", f"/orgs/{self.org_id}")
            records = [self._map_org(self._single_data(row))] if self._single_data(row) else []
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "targets":
            rows = self._jsonapi_collection(f"/orgs/{self.org_id}/targets")
            records = [self._map_target(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "projects":
            rows = self._jsonapi_collection(f"/orgs/{self.org_id}/projects")
            records = [self._map_project(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "findings":
            rows = self._jsonapi_collection(f"/orgs/{self.org_id}/issues")
            records = [self._map_finding(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "vulnerabilities":
            rows = self._jsonapi_collection(f"/orgs/{self.org_id}/issues")
            records = [self._map_vulnerability(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _request_json(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        query = dict(params or {})
        query.setdefault("version", self.api_version)
        return self.client.request_json(method, path, params=query)

    def _single_data(self, body: Any) -> dict[str, Any] | None:
        if not isinstance(body, dict):
            return None
        data = body.get("data")
        if isinstance(data, dict):
            return data
        if "id" in body:
            return body
        return None

    def _jsonapi_collection(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = dict(params or {})
        params.setdefault("limit", self._page_size)
        out: list[dict[str, Any]] = []
        starting_after = params.get("starting_after")

        for _ in range(1000):
            if starting_after:
                params["starting_after"] = starting_after
            body = self._request_json("GET", path, params=params)
            rows = self._data_rows(body)
            out.extend(rows)

            next_cursor = self._next_cursor(body)
            if not next_cursor:
                break
            starting_after = next_cursor

        return out

    def _data_rows(self, body: Any) -> list[dict[str, Any]]:
        if not isinstance(body, dict):
            return []
        data = body.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            return [data]
        return []

    def _next_cursor(self, body: Any) -> str | None:
        if not isinstance(body, dict):
            return None
        links = body.get("links") if isinstance(body.get("links"), dict) else {}
        next_link = links.get("next")
        if isinstance(next_link, str) and next_link:
            marker = "starting_after="
            if marker in next_link:
                return next_link.split(marker, 1)[1].split("&", 1)[0]
        if isinstance(next_link, dict):
            href = next_link.get("href")
            if isinstance(href, str) and "starting_after=" in href:
                return href.split("starting_after=", 1)[1].split("&", 1)[0]
        return None

    def _attrs(self, row: dict[str, Any]) -> dict[str, Any]:
        attrs = row.get("attributes")
        return attrs if isinstance(attrs, dict) else {}

    def _rel_id(self, row: dict[str, Any], relation_name: str) -> str | None:
        relationships = row.get("relationships") if isinstance(row.get("relationships"), dict) else {}
        relation = relationships.get(relation_name) if isinstance(relationships.get(relation_name), dict) else {}
        data = relation.get("data") if isinstance(relation.get("data"), dict) else None
        if isinstance(data, dict):
            identifier = data.get("id")
            return str(identifier) if identifier is not None else None
        return None

    def _map_org(self, row: dict[str, Any]) -> dict[str, Any]:
        attrs = self._attrs(row)
        return {
            "org_id": str(row.get("id") or ""),
            "name": attrs.get("name"),
            "slug": attrs.get("slug"),
            "group_id": attrs.get("group_id"),
            "created_at": as_iso8601(first_present(attrs.get("created_at"), attrs.get("created"))),
            "updated_at": as_iso8601(first_present(attrs.get("updated_at"), attrs.get("modified"), attrs.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_target(self, row: dict[str, Any]) -> dict[str, Any]:
        attrs = self._attrs(row)
        return {
            "target_id": str(row.get("id") or ""),
            "display_name": first_present(attrs.get("display_name"), attrs.get("name")),
            "target_type": attrs.get("type"),
            "url": first_present(attrs.get("url"), attrs.get("target_reference")),
            "integration_id": attrs.get("integration_id"),
            "status": attrs.get("status"),
            "created_at": as_iso8601(attrs.get("created_at")),
            "updated_at": as_iso8601(first_present(attrs.get("updated_at"), attrs.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_project(self, row: dict[str, Any]) -> dict[str, Any]:
        attrs = self._attrs(row)
        return {
            "project_id": str(row.get("id") or ""),
            "target_id": self._rel_id(row, "target"),
            "name": attrs.get("name"),
            "project_type": first_present(attrs.get("type"), attrs.get("origin")),
            "status": attrs.get("status"),
            "lifecycle": attrs.get("lifecycle"),
            "origin": attrs.get("origin"),
            "last_tested_at": as_iso8601(first_present(attrs.get("last_tested_at"), attrs.get("last_tested_date"))),
            "updated_at": as_iso8601(first_present(attrs.get("updated_at"), attrs.get("last_tested_at"), attrs.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_finding(self, row: dict[str, Any]) -> dict[str, Any]:
        attrs = self._attrs(row)
        return {
            "finding_id": str(row.get("id") or ""),
            "issue_type": attrs.get("type"),
            "title": first_present(attrs.get("title"), attrs.get("key")),
            "severity": first_present(attrs.get("effective_severity_level"), attrs.get("severity")),
            "status": attrs.get("status"),
            "project_id": self._rel_id(row, "scan_item"),
            "target_id": self._rel_id(row, "target"),
            "introduced_at": as_iso8601(first_present(attrs.get("created_at"), attrs.get("introduced_date"))),
            "updated_at": as_iso8601(first_present(attrs.get("updated_at"), attrs.get("modified_at"), attrs.get("created_at"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_vulnerability(self, row: dict[str, Any]) -> dict[str, Any]:
        attrs = self._attrs(row)
        classes = attrs.get("classes") if isinstance(attrs.get("classes"), list) else []
        cve = None
        for cls in classes:
            if not isinstance(cls, dict):
                continue
            if str(cls.get("source", "")).upper() == "CVE":
                cve = cls.get("id")
                break

        coordinates = attrs.get("coordinates") if isinstance(attrs.get("coordinates"), list) else []
        return {
            "vulnerability_id": str(row.get("id") or ""),
            "cve": cve,
            "issue_type": attrs.get("type"),
            "severity": first_present(attrs.get("effective_severity_level"), attrs.get("severity")),
            "status": attrs.get("status"),
            "project_id": self._rel_id(row, "scan_item"),
            "coordinates": json.dumps(coordinates, separators=(",", ":"), default=str) if coordinates else None,
            "updated_at": as_iso8601(first_present(attrs.get("updated_at"), attrs.get("modified_at"), attrs.get("created_at"))),
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
