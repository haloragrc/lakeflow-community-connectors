"""Dispatch Burp Enterprise GraphQL operations to deterministic corpus data."""

from __future__ import annotations

import json
from typing import Any

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import response_from_record


_OPERATION_MAP = {
    "AssetsTable": ("assets", "sites"),
    "ScansTable": ("scans", "scans"),
    "ScanResultsTable": ("scan_results", "scanResults"),
    "FindingsTable": ("findings", "findings"),
    "ScanConfigurationsTable": ("scan_configurations", "scanConfigurations"),
}


def _response(prep: PreparedRequest, status: int, body: dict[str, Any]) -> Response:
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=json.dumps(body),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)


def serve_graphql(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    raw_body = prep.body or b""
    if isinstance(raw_body, (bytes, bytearray)):
        raw_body = raw_body.decode("utf-8")
    try:
        payload = json.loads(raw_body) if raw_body else {}
    except json.JSONDecodeError:
        payload = {}

    operation_name = payload.get("operationName")
    mapping = _OPERATION_MAP.get(operation_name)
    if not mapping:
        return _response(
            prep,
            400,
            {"errors": [{"message": f"Unsupported operationName: {operation_name}"}]},
        )

    corpus_name, root_key = mapping
    rows = corpus.get(corpus_name)
    if not isinstance(rows, list):
        rows = []

    body = {
        "data": {
            root_key: {
                "nodes": rows,
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }
    }
    return _response(prep, 200, body)
