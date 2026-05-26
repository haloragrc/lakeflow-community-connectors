"""Google Workspace HTTP and pagination helpers."""

from __future__ import annotations

from typing import Any

import requests


class GoogleWorkspaceApiClient:
    def __init__(
        self,
        *,
        base_url: str,
        access_token: str,
        timeout_seconds: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}",
            }
        )

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        ok_statuses: tuple[int, ...] = (200,),
    ) -> Any:
        response = self.session.request(
            method=method,
            url=f"{self.base_url}{path}",
            params=params,
            json=payload,
            timeout=self.timeout_seconds,
        )
        if response.status_code not in ok_statuses:
            raise RuntimeError(
                f"API error {response.status_code} for {method} {path}: {response.text}"
            )
        if not response.content:
            return {}
        return response.json()

    def paged_list(
        self,
        path: str,
        *,
        records_key: str,
        params: dict[str, Any] | None = None,
        page_token_param: str = "pageToken",
        next_page_token_key: str = "nextPageToken",
    ) -> list[dict[str, Any]]:
        params = dict(params or {})
        out: list[dict[str, Any]] = []
        page_token: str | None = None
        for _ in range(1000):
            if page_token:
                params[page_token_param] = page_token
            body = self.request_json("GET", path, params=params)
            rows = body.get(records_key) if isinstance(body, dict) else None
            if isinstance(rows, list):
                out.extend([x for x in rows if isinstance(x, dict)])
            page_token = body.get(next_page_token_key) if isinstance(body, dict) else None
            if not page_token:
                break
        return out
