"""Burp Enterprise / Burp DAST connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.burp_enterprise.burp_enterprise_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
)


_ASSETS_QUERY = """
query AssetsTable {
  sites {
    nodes {
      id
      name
      url
      environment
      status
      updatedAt
    }
  }
}
""".strip()

_SCANS_QUERY = """
query ScansTable {
  scans {
    nodes {
      id
      site { id }
      status
      scheduledAt
      startedAt
      finishedAt
      updatedAt
    }
  }
}
""".strip()

_SCAN_RESULTS_QUERY = """
query ScanResultsTable {
  scanResults {
    nodes {
      id
      scan { id }
      site { id }
      status
      issueCount
      updatedAt
    }
  }
}
""".strip()

_FINDINGS_QUERY = """
query FindingsTable {
  findings {
    nodes {
      id
      scan { id }
      site { id }
      title
      severity
      confidence
      status
      path
      updatedAt
    }
  }
}
""".strip()

_SCAN_CONFIGURATIONS_QUERY = """
query ScanConfigurationsTable {
  scanConfigurations {
    nodes {
      id
      name
      crawlProfile
      auditProfile
      updatedAt
    }
  }
}
""".strip()


class BurpEnterpriseLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_token = options.get("api_token")
        if not api_token:
            raise ValueError("burp_enterprise requires api_token")

        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://burp-dast.example.local"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {api_token}",
            },
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

        if table_name == "assets":
            rows = self._graphql_nodes("AssetsTable", _ASSETS_QUERY, "sites")
            records = [self._map_asset(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "scans":
            rows = self._graphql_nodes("ScansTable", _SCANS_QUERY, "scans")
            records = [self._map_scan(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "scan_results":
            rows = self._graphql_nodes("ScanResultsTable", _SCAN_RESULTS_QUERY, "scanResults")
            records = [self._map_scan_result(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "findings":
            rows = self._graphql_nodes("FindingsTable", _FINDINGS_QUERY, "findings")
            records = [self._map_finding(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "scan_configurations":
            rows = self._graphql_nodes("ScanConfigurationsTable", _SCAN_CONFIGURATIONS_QUERY, "scanConfigurations")
            records = [self._map_scan_configuration(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _graphql_nodes(self, operation_name: str, query: str, root_key: str) -> list[dict[str, Any]]:
        payload = {"query": query, "variables": {}, "operationName": operation_name}
        body = self.client.request_json("POST", "/graphql", payload=payload)
        if isinstance(body, dict) and isinstance(body.get("errors"), list) and body["errors"]:
            raise RuntimeError(f"Burp GraphQL errors for {operation_name}: {body['errors']}")

        data = body.get("data") if isinstance(body, dict) else None
        root = data.get(root_key) if isinstance(data, dict) else None
        nodes = root.get("nodes") if isinstance(root, dict) else None
        if isinstance(nodes, list):
            return [x for x in nodes if isinstance(x, dict)]
        return []

    def _map_asset(self, row: dict[str, Any]) -> dict[str, Any]:
        site_id = str(row.get("id") or "")
        return {
            "asset_id": site_id,
            "site_id": site_id,
            "name": row.get("name"),
            "url": row.get("url"),
            "environment": row.get("environment"),
            "status": row.get("status"),
            "updated_at": as_iso8601(row.get("updatedAt")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_scan(self, row: dict[str, Any]) -> dict[str, Any]:
        site = row.get("site") if isinstance(row.get("site"), dict) else {}
        return {
            "scan_id": str(row.get("id") or ""),
            "site_id": site.get("id"),
            "status": row.get("status"),
            "scheduled_at": as_iso8601(row.get("scheduledAt")),
            "started_at": as_iso8601(row.get("startedAt")),
            "finished_at": as_iso8601(row.get("finishedAt")),
            "updated_at": as_iso8601(row.get("updatedAt")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_scan_result(self, row: dict[str, Any]) -> dict[str, Any]:
        scan = row.get("scan") if isinstance(row.get("scan"), dict) else {}
        site = row.get("site") if isinstance(row.get("site"), dict) else {}
        return {
            "scan_result_id": str(row.get("id") or ""),
            "scan_id": scan.get("id"),
            "site_id": site.get("id"),
            "status": row.get("status"),
            "issue_count": str(row.get("issueCount")) if row.get("issueCount") is not None else None,
            "updated_at": as_iso8601(row.get("updatedAt")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_finding(self, row: dict[str, Any]) -> dict[str, Any]:
        scan = row.get("scan") if isinstance(row.get("scan"), dict) else {}
        site = row.get("site") if isinstance(row.get("site"), dict) else {}
        return {
            "finding_id": str(row.get("id") or ""),
            "scan_id": scan.get("id"),
            "site_id": site.get("id"),
            "title": row.get("title"),
            "severity": row.get("severity"),
            "confidence": row.get("confidence"),
            "status": row.get("status"),
            "path": row.get("path"),
            "updated_at": as_iso8601(row.get("updatedAt")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_scan_configuration(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "scan_configuration_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "crawl_profile": row.get("crawlProfile"),
            "audit_profile": row.get("auditProfile"),
            "updated_at": as_iso8601(row.get("updatedAt")),
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
