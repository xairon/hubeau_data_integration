"""HTTP client utilities for the generic dlt pipeline.

This module centralises retry/backoff logic and rate limiting based on a
simple token bucket. The implementation favours determinism so it can be
unit-tested without sleeping for long periods of time.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import httpx
from jsonpath_ng import parse


class TokenBucket:
    """Simple token bucket implementation used to cap request rate."""

    def __init__(self, rps: float, capacity: float = 1.0) -> None:
        self.capacity = max(capacity, 1.0)
        self.tokens = self.capacity
        self.fill_rate = max(rps, 0.01)
        self.timestamp = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.timestamp
        self.timestamp = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)

    def consume(self, tokens: float = 1.0) -> None:
        while True:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return
            missing = tokens - self.tokens
            sleep_time = missing / self.fill_rate if self.fill_rate else 1.0
            time.sleep(max(sleep_time, 0.01))


@dataclass
class RateLimitConfig:
    target_rps: float = 0.5
    max_concurrency: int = 1
    rps_decrease_on_429: bool = True
    rps_increase_cooldown_s: int = 60
    backoff: str = "exponential_jitter"


class HttpClient:
    """HTTP client with retry, backoff and rate-limit support."""

    def __init__(self, cfg: Dict[str, Any], *, client: Optional[httpx.Client] = None) -> None:
        self.base_url = cfg["base_url"].rstrip("/")
        rl_cfg = RateLimitConfig(**cfg.get("rate_limit", {}))
        self.bucket = TokenBucket(rl_cfg.target_rps)
        self.rate_limit_cfg = rl_cfg
        self.last_429_ts = 0.0
        self.session = client or httpx.Client(timeout=cfg.get("timeout", 60))
        self.default_headers = cfg.get("headers", {})
        self.backoff_initial = cfg.get("backoff_initial", 1.0)
        self.backoff_max = cfg.get("backoff_max", 120.0)
        self.max_attempts = max(int(cfg.get("max_attempts", 5)), 1)

    # --- context management --------------------------------------------
    def close(self) -> None:
        if hasattr(self.session, "close"):
            self.session.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # --- public API -----------------------------------------------------
    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Perform an HTTP request with retry/backoff support."""

        url = f"{self.base_url}/{path.lstrip('/')}"
        method = method.upper()
        backoff = self.backoff_initial
        attempts = 0
        header_payload = dict(self.default_headers)
        if headers:
            header_payload.update(headers)

        while attempts < self.max_attempts:
            attempts += 1
            self.bucket.consume(1.0)
            try:
                response = self.session.request(
                    method,
                    url,
                    params=params or None,
                    json=json_body,
                    data=data,
                    headers=header_payload or None,
                )
            except httpx.HTTPError:
                if attempts >= self.max_attempts:
                    raise
                time.sleep(min(backoff, self.backoff_max))
                backoff = min(self.backoff_max, backoff * 1.7)
                continue

            if response.status_code == 429:
                self._handle_429()
                if attempts >= self.max_attempts:
                    response.raise_for_status()
                time.sleep(min(backoff, self.backoff_max))
                backoff = min(self.backoff_max, backoff * 1.7)
                continue

            if response.status_code >= 500:
                if attempts >= self.max_attempts:
                    response.raise_for_status()
                time.sleep(min(backoff, self.backoff_max))
                backoff = min(self.backoff_max, backoff * 1.7)
                continue

            response.raise_for_status()
            self._maybe_increase_rps()
            return self._decode_response(response)

        raise RuntimeError("HTTP request failed after exhausting retry attempts")

    def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Backward compatible helper for GET requests."""

        return self.request("GET", path, params=params)

    def extract_records(self, payload: Dict[str, Any], records_path: Optional[str]) -> Iterable[Dict[str, Any]]:
        """Extract records from the JSON payload using JSONPath.

        If ``records_path`` is ``None`` or empty, the payload itself is
        returned. Otherwise the first match of the JSONPath expression is
        assumed to contain the list of records.
        """

        if not records_path:
            if isinstance(payload, list):
                return payload
            return [payload]

        matches = parse(records_path).find(payload)
        if not matches:
            return []
        aggregated = []
        for match in matches:
            value = match.value
            if value is None:
                continue
            if isinstance(value, list):
                aggregated.extend(value)
            else:
                aggregated.append(value)
        return aggregated

    # --- helpers --------------------------------------------------------
    def _handle_429(self) -> None:
        self.last_429_ts = time.monotonic()
        if not self.rate_limit_cfg.rps_decrease_on_429:
            return
        self.bucket.fill_rate = max(0.1, self.bucket.fill_rate * 0.7)

    def _maybe_increase_rps(self) -> None:
        if not self.rate_limit_cfg.rps_decrease_on_429:
            return
        now = time.monotonic()
        if now - self.last_429_ts > self.rate_limit_cfg.rps_increase_cooldown_s:
            self.bucket.fill_rate = min(self.bucket.fill_rate * 1.05, 5.0)

    def _decode_response(self, response: httpx.Response) -> Dict[str, Any]:
        try:
            return response.json()
        except ValueError as exc:  # pragma: no cover - defensive branch
            raise ValueError("Expected JSON response from API") from exc


__all__ = ["HttpClient", "TokenBucket"]
