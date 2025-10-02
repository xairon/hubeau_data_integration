from pathlib import Path
from typing import Any, Dict

import pytest

pytest.importorskip("dlt", reason="dlt dependency missing")

try:
    from pipelines.dlt.hubeau_generic import run_pipeline
except NotImplementedError as exc:  # pragma: no cover - dependency issue
    pytest.skip(f"dlt import failed: {exc}", allow_module_level=True)


class DummyHttpClient:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.calls = []

    def get(self, path: str, params: Dict[str, Any]):
        self.calls.append((path, params))
        return {"data": [{"id_taxon": 1, "code_station": "A", "date_prelevement": "2024-01-01"}]}

    def extract_records(self, payload: Dict[str, Any], records_path: str):
        return payload["data"]

def test_pipeline_runs(tmp_path, monkeypatch):
    monkeypatch.setenv("DESTINATION__FILESYSTEM__PATH", str(tmp_path))
    monkeypatch.setattr("pipelines.dlt.hubeau_generic.HttpClient", DummyHttpClient)
    cfg = {
        "name": "hydrobio_taxons",
        "source": "hubeau",
        "base_url": "https://example.com",
        "path": "/hydrobio/taxons",
        "primary_keys": ["id_taxon"],
        "records_path": "$.data",
        "params_default": {},
        "pagination": {
            "type": "page",
            "page_param": "page",
            "page_size_param": "size",
            "page_size": 500,
        },
        "slicer": {
            "mode": "datetime",
            "start_param": "start",
            "end_param": "end",
            "window_days": 1,
            "start_date": "2024-01-01",
            "end_offset_days": 0,
        },
    }
    load_info = run_pipeline(cfg)
    assert load_info is not None
    output_dir = Path(tmp_path) / "bronze" / "hydrobio_taxons"
    assert output_dir.exists()
