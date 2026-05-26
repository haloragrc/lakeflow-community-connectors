"""Wiz security graph connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.high_value_common import (
    ClientCredentialsTokenProvider,
    HighValueApiClient,
    as_iso8601,
)
from databricks.labs.community_connector.sources.wiz.wiz_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


_ASSETS_QUERY = """
query AssetsTable {
  assets {
    nodes {
      id
      name
      type
      cloudProvider
      subscriptionId
      region
      status
      updatedAt
    }
  }
}
""".strip()

_FINDINGS_QUERY = """
query FindingsTable {
  issues {
    nodes {
      id
      title
      severity
      status
      entity { id }
      vulnerability { id }
      createdAt
      updatedAt
    }
  }
}
""".strip()

_VULNERABILITIES_QUERY = """
query VulnerabilitiesTable {
  vulnerabilities {
    nodes {
      id
      cve
      vendorSeverity
      cvssScore
      status
      asset { id }
      discoveredAt
      updatedAt
    }
  }
}
""".strip()

_INCIDENTS_QUERY = """
query IncidentsTable {
  incidents {
    nodes {
      id
      title
      severity
      status
      source
      createdAt
      updatedAt
    }
  }
}
""".strip()

_SCAN_RUNS_QUERY = """
query ScanRunsTable {
  scanRuns {
    nodes {
      id
      scanType
      status
      startedAt
      completedAt
      updatedAt
    }
  }
}
""".strip()

_PROJECTS_QUERY = """
query ProjectsTable {
  projects {
    nodes {
      id
      name
      slug
      businessUnit
      riskProfile
      updatedAt
    }
  }
}
""".strip()


class WizLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        client_id = options.get("client_id")
        client_secret = options.get("client_secret")
        if not client_id or not client_secret:
            raise ValueError("wiz requires client_id and client_secret")

        timeout_seconds = int(options.get("timeout_seconds", "30"))
        token_provider = ClientCredentialsTokenProvider(
            token_url=options.get("token_url", "https://auth.app.wiz.io/oauth/token"),
            client_id=client_id,
            client_secret=client_secret,
            audience=options.get("audience", "wiz-api"),
            timeout_seconds=timeout_seconds,
        )
        access_token = token_provider.get_token()

        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://api.wiz.io"),
            timeout_seconds=timeout_seconds,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}",
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
            rows = self._graphql_nodes("AssetsTable", _ASSETS_QUERY, "assets")
            records = [self._map_asset(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "findings":
            rows = self._graphql_nodes("FindingsTable", _FINDINGS_QUERY, "issues")
            records = [self._map_finding(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "vulnerabilities":
            rows = self._graphql_nodes("VulnerabilitiesTable", _VULNERABILITIES_QUERY, "vulnerabilities")
            records = [self._map_vulnerability(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "incidents":
            rows = self._graphql_nodes("IncidentsTable", _INCIDENTS_QUERY, "incidents")
            records = [self._map_incident(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "scan_runs":
            rows = self._graphql_nodes("ScanRunsTable", _SCAN_RUNS_QUERY, "scanRuns")
            records = [self._map_scan_run(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "projects":
            rows = self._graphql_nodes("ProjectsTable", _PROJECTS_QUERY, "projects")
            records = [self._map_project(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _graphql_nodes(self, operation_name: str, query: str, root_key: str) -> list[dict[str, Any]]:
        payload = {
            "query": query,
            "variables": {},
            "operationName": operation_name,
        }
        body = self.client.request_json("POST", "/graphql", payload=payload)
        if isinstance(body, dict) and isinstance(body.get("errors"), list) and body["errors"]:
            raise RuntimeError(f"Wiz GraphQL errors for {operation_name}: {body['errors']}")

        data = body.get("data") if isinstance(body, dict) else None
        root = data.get(root_key) if isinstance(data, dict) else None
        nodes = root.get("nodes") if isinstance(root, dict) else None
        if isinstance(nodes, list):
            return [x for x in nodes if isinstance(x, dict)]
        return []

    def _map_asset(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "asset_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "asset_type": row.get("type"),
            "cloud_provider": row.get("cloudProvider"),
            "subscription_id": row.get("subscriptionId"),
            "region": row.get("region"),
            "status": row.get("status"),
            "updated_at": as_iso8601(row.get("updatedAt")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_finding(self, row: dict[str, Any]) -> dict[str, Any]:
        entity = row.get("entity") if isinstance(row.get("entity"), dict) else {}
        vulnerability = row.get("vulnerability") if isinstance(row.get("vulnerability"), dict) else {}
        return {
            "finding_id": str(row.get("id") or ""),
            "title": row.get("title"),
            "severity": row.get("severity"),
            "status": row.get("status"),
            "asset_id": entity.get("id"),
            "vulnerability_id": vulnerability.get("id"),
            "created_at": as_iso8601(row.get("createdAt")),
            "updated_at": as_iso8601(row.get("updatedAt")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_vulnerability(self, row: dict[str, Any]) -> dict[str, Any]:
        asset = row.get("asset") if isinstance(row.get("asset"), dict) else {}
        return {
            "vulnerability_id": str(row.get("id") or ""),
            "cve": row.get("cve"),
            "vendor_severity": row.get("vendorSeverity"),
            "cvss_score": str(row.get("cvssScore")) if row.get("cvssScore") is not None else None,
            "status": row.get("status"),
            "asset_id": asset.get("id"),
            "discovered_at": as_iso8601(row.get("discoveredAt")),
            "updated_at": as_iso8601(row.get("updatedAt")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_incident(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "incident_id": str(row.get("id") or ""),
            "title": row.get("title"),
            "severity": row.get("severity"),
            "status": row.get("status"),
            "source": row.get("source"),
            "created_at": as_iso8601(row.get("createdAt")),
            "updated_at": as_iso8601(row.get("updatedAt")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_scan_run(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "scan_run_id": str(row.get("id") or ""),
            "scan_type": row.get("scanType"),
            "status": row.get("status"),
            "started_at": as_iso8601(row.get("startedAt")),
            "completed_at": as_iso8601(row.get("completedAt")),
            "updated_at": as_iso8601(row.get("updatedAt")),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_project(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "project_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "slug": row.get("slug"),
            "business_unit": row.get("businessUnit"),
            "risk_profile": row.get("riskProfile"),
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
