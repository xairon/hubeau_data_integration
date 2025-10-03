from datetime import date

import pytest

from pipelines.dlt.slicing import build_slices, generate_fallback_slices, needs_truncation


@pytest.fixture
def datetime_cfg():
    return {
        "slicer": {
            "mode": "datetime",
            "start_param": "start",
            "end_param": "end",
            "window_days": 1,
            "start_date": "2024-01-01",
            "end_offset_days": 0,
        }
    }


class _MockDate(date):
    @classmethod
    def today(cls):
        return cls(2024, 1, 3)


def test_build_datetime_slices(monkeypatch, datetime_cfg):
    monkeypatch.setattr("pipelines.dlt.slicing.date", _MockDate)
    slices = list(build_slices(datetime_cfg))
    assert len(slices) == 3
    assert slices[0].params == {"start": "2024-01-01", "end": "2024-01-01"}
    assert slices[0].metadata["start"] == "2024-01-01"
    assert slices[0].metadata["end"] == "2024-01-01"


def test_needs_truncation_default():
    assert not needs_truncation(25_000, {})


def test_needs_truncation_with_threshold():
    cfg = {"fallbacks": {"truncation_threshold": 10_000}}
    assert needs_truncation(10_000, cfg)
    assert needs_truncation(25_000, cfg)
    assert not needs_truncation(9_999, cfg)


def test_fallback_day(monkeypatch, datetime_cfg):
    monkeypatch.setattr("pipelines.dlt.slicing.date", _MockDate)
    cfg = {**datetime_cfg, "fallbacks": {"split_chain": ["day"]}}
    root_slice = next(iter(build_slices(cfg)))
    fallbacks = generate_fallback_slices(root_slice, cfg, root_slice.level)
    assert len(fallbacks) == 1  # already daily windows
    assert fallbacks[0].metadata["start"] == "2024-01-01"


def test_fallback_station_month(monkeypatch, datetime_cfg):
    monkeypatch.setattr("pipelines.dlt.slicing.date", _MockDate)
    cfg = {
        **datetime_cfg,
        "fallbacks": {"split_chain": ["station_month"]},
        "pre_scan": {"stations": {"enabled": True, "values": ["A", "B"]}},
    }
    root_slice = next(iter(build_slices(cfg)))
    fallbacks = generate_fallback_slices(root_slice, cfg, root_slice.level)
    assert any(f.scope == "station-A" for f in fallbacks)
    assert any(f.scope == "station-B" for f in fallbacks)
