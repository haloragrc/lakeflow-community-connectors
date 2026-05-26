"""Microsoft Defender for Cloud connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.defender_for_cloud.defender_for_cloud_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
    first_present,
)


class DefenderForCloudLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_token = options.get("access_token")
        subscription_id = options.get("subscription_id")
        if not access_token or not subscription_id:
            raise ValueError("defender_for_cloud requires access_token and subscription_id")

        self.subscription_id = subscription_id
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://management.azure.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
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
            rows = self._paged_value(
                f"/subscriptions/{self.subscription_id}/resources",
                {"api-version": "2021-04-01"},
            )
            records = [self._map_asset(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "findings":
            rows = self._paged_value(
                f"/subscriptions/{self.subscription_id}/providers/Microsoft.Security/assessments",
                {"api-version": "2021-06-01"},
            )
            records = [self._map_finding(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "incidents":
            rows = self._paged_value(
                f"/subscriptions/{self.subscription_id}/providers/Microsoft.Security/alerts",
                {"api-version": "2022-01-01"},
            )
            records = [self._map_incident(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "secure_scores":
            rows = self._paged_value(
                f"/subscriptions/{self.subscription_id}/providers/Microsoft.Security/secureScores",
                {"api-version": "2020-01-01"},
            )
            records = [self._map_secure_score(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "recommendations":
            rows = self._paged_value(
                f"/subscriptions/{self.subscription_id}/providers/Microsoft.Security/recommendations",
                {"api-version": "2020-01-01"},
            )
            records = [self._map_recommendation(r) for r in rows]
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _paged_value(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        next_url: str | None = path
        next_params: dict[str, Any] | None = params

        for _ in range(1000):
            if next_url is None:
                break
            if next_url.startswith("http"):
                rel = next_url.replace(self.client.base_url, "")
                body = self.client.request_json("GET", rel)
            else:
                body = self.client.request_json("GET", next_url, params=next_params)
            rows = body.get("value") if isinstance(body, dict) else None
            if isinstance(rows, list):
                out.extend([x for x in rows if isinstance(x, dict)])
            next_url = body.get("nextLink") if isinstance(body, dict) else None
            next_params = None
            if not next_url:
                break

        return out

    def _map_asset(self, row: dict[str, Any]) -> dict[str, Any]:
        tags = row.get("tags") if isinstance(row.get("tags"), dict) else {}
        resource_group = None
        rid = row.get("id")
        if isinstance(rid, str) and "/resourceGroups/" in rid:
            resource_group = rid.split("/resourceGroups/", 1)[1].split("/", 1)[0]
        return {
            "asset_id": str(row.get("id") or ""),
            "name": row.get("name"),
            "type": row.get("type"),
            "location": row.get("location"),
            "resource_group": resource_group,
            "subscription_id": self.subscription_id,
            "tags": json.dumps(tags, separators=(",", ":")) if tags else None,
            "updated_at": as_iso8601(first_present((row.get("systemData") or {}).get("lastModifiedAt") if isinstance(row.get("systemData"), dict) else None, row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_finding(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        status = props.get("status") if isinstance(props.get("status"), dict) else {}
        metadata = props.get("metadata") if isinstance(props.get("metadata"), dict) else {}
        resource_details = props.get("resourceDetails") if isinstance(props.get("resourceDetails"), dict) else {}
        return {
            "finding_id": str(row.get("id") or ""),
            "assessment_key": row.get("name"),
            "display_name": props.get("displayName"),
            "status_code": status.get("code"),
            "severity": metadata.get("severity"),
            "resource_id": resource_details.get("id"),
            "remediation_description": metadata.get("description"),
            "updated_at": as_iso8601(first_present(status.get("firstEvaluationDate"), status.get("statusChangeDate"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_incident(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        entity = props.get("compromisedEntity")
        resource = props.get("resourceIdentifiers")
        rid = None
        if isinstance(resource, list) and resource and isinstance(resource[0], dict):
            rid = resource[0].get("azureResourceId")
        return {
            "incident_id": str(row.get("id") or ""),
            "alert_display_name": props.get("alertDisplayName"),
            "severity": props.get("severity"),
            "status": props.get("status"),
            "resource_id": rid,
            "time_generated": as_iso8601(props.get("timeGeneratedUtc")),
            "compromised_entity": entity,
            "updated_at": as_iso8601(first_present(props.get("timeGeneratedUtc"), props.get("statusChangedOn"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_secure_score(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        current = props.get("score", {}).get("current") if isinstance(props.get("score"), dict) else None
        max_val = props.get("score", {}).get("max") if isinstance(props.get("score"), dict) else None
        percentage = None
        if current is not None and max_val not in (None, 0):
            percentage = str(round((float(current) / float(max_val)) * 100, 2))
        return {
            "secure_score_id": str(row.get("id") or ""),
            "display_name": props.get("displayName"),
            "current": str(current) if current is not None else None,
            "max": str(max_val) if max_val is not None else None,
            "percentage": percentage,
            "weight": str(props.get("weight")) if props.get("weight") is not None else None,
            "updated_at": as_iso8601(first_present(props.get("updatedAt"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_recommendation(self, row: dict[str, Any]) -> dict[str, Any]:
        props = row.get("properties") if isinstance(row.get("properties"), dict) else {}
        return {
            "recommendation_id": str(row.get("id") or ""),
            "display_name": props.get("displayName"),
            "description": props.get("description"),
            "category": props.get("category"),
            "severity": props.get("severity"),
            "state": props.get("state"),
            "updated_at": as_iso8601(first_present(props.get("updatedOn"), row.get("id"))),
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
