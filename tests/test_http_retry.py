from unittest.mock import MagicMock

import httpx
import pytest

pytest.importorskip("jsonpath_ng", reason="jsonpath-ng dependency missing")

from pipelines.dlt.http_client import HttpClient


class DummyResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {"data": []}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("error", request=MagicMock(), response=MagicMock(status_code=self.status_code))


class DummyClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = 0

    def get(self, *args, **kwargs):
        response = self.responses[self.calls]
        self.calls += 1
        return response


def test_retry_on_429(monkeypatch):
    responses = [DummyResponse(429), DummyResponse(200)]
    client = DummyClient(responses)
    cfg = {"base_url": "https://example.com"}
    http_client = HttpClient(cfg, client=client)
    http_client.bucket.consume = MagicMock()  # avoid sleeping
    http_client.bucket.consume.side_effect = lambda *args, **kwargs: None
    data = http_client.get("/endpoint", params={})
    assert client.calls == 2
    assert data == {"data": []}


def test_extract_records_jsonpath():
    http_client = HttpClient({"base_url": "https://example.com"})
    payload = {"data": [{"id": 1}, {"id": 2}]}
    records = list(http_client.extract_records(payload, "$.data"))
    assert len(records) == 2
