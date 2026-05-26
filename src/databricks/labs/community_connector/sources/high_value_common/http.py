"""Shared HTTP and normalization helpers for multiple connectors."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

import requests


class HighValueApiClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: int = 30,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        if headers:
            self.session.headers.update(dict(headers))

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        ok_statuses: tuple[int, ...] = (200,),
    ) -> Any:
        response = self.session.request(
            method=method,
            url=f"{self.base_url}{path}",
            params=params,
            json=payload,
            data=data,
            timeout=self.timeout_seconds,
        )
        if response.status_code not in ok_statuses:
            raise RuntimeError(
                f"API error {response.status_code} for {method} {path}: {response.text}"
            )
        if not response.content:
            return {}
        return response.json()


def first_present(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def as_iso8601(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    if text.endswith("Z"):
        return text

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return text
