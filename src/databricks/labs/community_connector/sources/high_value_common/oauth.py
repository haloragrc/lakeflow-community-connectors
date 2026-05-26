"""OAuth helpers for high-value connectors."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests


class ClientCredentialsTokenProvider:
    """Fetches and caches an OAuth client credentials access token."""

    def __init__(
        self,
        *,
        token_url: str,
        client_id: str,
        client_secret: str,
        timeout_seconds: int = 30,
        audience: str | None = None,
        scope: str | None = None,
    ) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout_seconds = timeout_seconds
        self.audience = audience
        self.scope = scope

        self._access_token: str | None = None
        self._expires_at: datetime | None = None

    def get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._access_token and self._expires_at and now < self._expires_at:
            return self._access_token

        payload: dict[str, Any] = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.audience:
            payload["audience"] = self.audience
        if self.scope:
            payload["scope"] = self.scope

        response = requests.post(
            self.token_url,
            data=payload,
            timeout=self.timeout_seconds,
        )
        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Token request failed ({response.status_code}): {response.text}"
            )

        body = response.json()
        token = body.get("access_token")
        if not token:
            raise RuntimeError("Token response missing access_token")

        expires_in = body.get("expires_in")
        try:
            expires_seconds = int(expires_in) if expires_in is not None else 300
        except (TypeError, ValueError):
            expires_seconds = 300
        expires_seconds = max(expires_seconds - 60, 30)

        self._access_token = str(token)
        self._expires_at = now + timedelta(seconds=expires_seconds)
        return self._access_token
