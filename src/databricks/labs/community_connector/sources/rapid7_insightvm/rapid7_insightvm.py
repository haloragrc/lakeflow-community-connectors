"""Rapid7 InsightVM connector."""

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
from databricks.labs.community_connector.sources.rapid7_insightvm.rapid7_insightvm_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class Rapid7InsightVmLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_key = options.get("api_key")
        if not api_key:
            raise ValueError("rapid7_insightvm requires api_key")

        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://example.insightvm.local"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "X-Api-Key": api_key,
            },
        )
        self._page_size = int(options.get("page_size", "200"))
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
            rows = self._paged_resources("/api/3/assets")
            records = [self._map_asset(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "vulnerabilities":
            rows = self._paged_resources("/api/3/vulnerabilities")
            records = [self._map_vulnerability(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "findings":
            rows = self._paged_resources("/api/3/vulnerability_findings")
            records = [self._map_finding(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "scans":
            rows = self._paged_resources("/api/3/scans")
            records = [self._map_scan(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "scan_results":
            rows = self._paged_resources("/api/3/scan_results")
            records = [self._map_scan_result(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")
        if table_name == "sites":
            rows = self._paged_resources("/api/3/sites")
            records = [self._map_site(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _paged_resources(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        page = int(params.get("page", 0))
        size = int(params.get("size", self._page_size))
        out: list[dict[str, Any]] = []

        for _ in range(1000):
            request_params = {**params, "page": page, "size": size}
            body = self.client.request_json("GET", path, params=request_params)
            rows = self._rows(body)
            out.extend(rows)

            page_info = body.get("page") if isinstance(body, dict) and isinstance(body.get("page"), dict) else {}
            total_pages = page_info.get("totalPages") if isinstance(page_info.get("totalPages"), int) else None
            if total_pages is not None:
                if page + 1 >= total_pages:
                    break
                page += 1
                continue

            if len(rows) < size:
                break
            page += 1

        return out

    def _rows(self, body: Any) -> list[dict[str, Any]]:
        if isinstance(body, dict):
            for key in ("resources", "data", "items"):
                value = body.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]
        if isinstance(body, list):
            return [x for x in body if isinstance(x, dict)]
        return []

    def _map_asset(self, row: dict[str, Any]) -> dict[str, Any]:
        host_name = row.get("hostName")
        addresses = row.get("ip") if isinstance(row.get("ip"), str) else None
        if addresses is None and isinstance(row.get("addresses"), list) and row["addresses"]:
            addresses = row["addresses"][0]

        return {
            "asset_id": str(first_present(row.get("id"), row.get("assetId"), "")),
            "host_name": host_name,
            "ip": addresses,
            "os": row.get("os"),
            "risk_score": str(row.get("riskScore")) if row.get("riskScore") is not None else None,
            "site_id": str(row.get("siteId")) if row.get("siteId") is not None else None,
            "last_assessed_at": as_iso8601(first_present(row.get("lastAssessedForVulnerabilities"), row.get("lastScanDate"))),
            "updated_at": as_iso8601(first_present(row.get("lastModified"), row.get("lastAssessedForVulnerabilities"), row.get("lastScanDate"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_vulnerability(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "vulnerability_id": str(first_present(row.get("id"), row.get("vulnerabilityId"), "")),
            "title": row.get("title"),
            "severity": str(first_present(row.get("severity"), row.get("severityScore"))),
            "cvss_score": str(first_present(row.get("cvssScore"), row.get("cvssV3Score"))) if first_present(row.get("cvssScore"), row.get("cvssV3Score")) is not None else None,
            "cve": first_present(row.get("cve"), row.get("cveId")),
            "published_at": as_iso8601(first_present(row.get("published"), row.get("publishedDate"))),
            "updated_at": as_iso8601(first_present(row.get("modified"), row.get("updatedAt"), row.get("published"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_finding(self, row: dict[str, Any]) -> dict[str, Any]:
        finding_id = first_present(row.get("id"), row.get("findingId"))
        if not finding_id:
            finding_id = f"{first_present(row.get('assetId'), 'asset')}:{first_present(row.get('vulnerabilityId'), 'vuln')}"

        return {
            "finding_id": str(finding_id),
            "asset_id": str(row.get("assetId")) if row.get("assetId") is not None else None,
            "vulnerability_id": str(row.get("vulnerabilityId")) if row.get("vulnerabilityId") is not None else None,
            "status": first_present(row.get("status"), row.get("state")),
            "proof": row.get("proof"),
            "first_discovered": as_iso8601(first_present(row.get("firstDiscovered"), row.get("firstFound"))),
            "last_discovered": as_iso8601(first_present(row.get("lastDiscovered"), row.get("lastFound"))),
            "updated_at": as_iso8601(first_present(row.get("lastDiscovered"), row.get("lastFound"), row.get("firstDiscovered"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_scan(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "scan_id": str(first_present(row.get("id"), row.get("scanId"), "")),
            "name": row.get("name"),
            "status": first_present(row.get("status"), row.get("state")),
            "engine_id": str(row.get("engineId")) if row.get("engineId") is not None else None,
            "started_at": as_iso8601(first_present(row.get("startTime"), row.get("startedAt"))),
            "finished_at": as_iso8601(first_present(row.get("endTime"), row.get("finishedAt"))),
            "updated_at": as_iso8601(first_present(row.get("endTime"), row.get("lastModified"), row.get("startTime"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_scan_result(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "scan_result_id": str(first_present(row.get("id"), row.get("scanResultId"), "")),
            "scan_id": str(row.get("scanId")) if row.get("scanId") is not None else None,
            "status": first_present(row.get("status"), row.get("state")),
            "asset_count": str(first_present(row.get("assetCount"), row.get("assets"))) if first_present(row.get("assetCount"), row.get("assets")) is not None else None,
            "vulnerability_count": str(first_present(row.get("vulnerabilityCount"), row.get("vulnerabilities"))) if first_present(row.get("vulnerabilityCount"), row.get("vulnerabilities")) is not None else None,
            "updated_at": as_iso8601(first_present(row.get("updatedAt"), row.get("endTime"), row.get("startTime"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_site(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "site_id": str(first_present(row.get("id"), row.get("siteId"), "")),
            "name": row.get("name"),
            "importance": str(row.get("importance")) if row.get("importance") is not None else None,
            "risk_score": str(row.get("riskScore")) if row.get("riskScore") is not None else None,
            "site_type": row.get("type"),
            "last_scan_time": as_iso8601(first_present(row.get("lastScanTime"), row.get("lastScanDate"))),
            "updated_at": as_iso8601(first_present(row.get("lastScanTime"), row.get("lastModified"), row.get("created"))),
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
