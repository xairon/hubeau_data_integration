"""Tests for Pastas groundwater signatures computation."""

import numpy as np
import pandas as pd

from hubeau_pipeline.ml.pastas_signatures import compute_signatures_single


def _make_synthetic_gwl(n_days: int = 3000, seed: int = 42) -> pd.Series:
    """Synthetic daily GWL: sinusoidal annual cycle + noise."""
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2015-01-01", periods=n_days, freq="D")
    t = np.arange(n_days)
    gwl = 50.0 + 2.0 * np.sin(2 * np.pi * t / 365) + rng.normal(0, 0.3, n_days)
    return pd.Series(gwl, index=dates, name="gwl")


class TestComputeSignaturesSingle:
    def test_success_on_long_series(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        result = compute_signatures_single("TEST_001", gwl)
        assert result["code_bss"] == "TEST_001"
        assert result["success"] is True
        assert "error" not in result

    def test_30_signatures_present(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        result = compute_signatures_single("TEST_001", gwl)
        assert result["success"] is True
        assert result["n_signatures_computed"] >= 25
        assert "magnitude" in result
        assert "recession_constant" in result
        assert "autocorr_time" in result

    def test_7_dutch_stats_present(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        result = compute_signatures_single("TEST_001", gwl)
        for key in ["gg", "ghg", "glg", "gvg", "q_ghg", "q_glg", "q_gvg"]:
            assert key in result

    def test_metadata_fields(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        result = compute_signatures_single("TEST_001", gwl)
        assert result["series_start"] is not None
        assert result["series_end"] is not None
        assert result["series_length_days"] > 0
        assert result["n_valid_days"] > 0

    def test_short_series_does_not_crash(self):
        gwl = _make_synthetic_gwl(n_days=300)
        result = compute_signatures_single("SHORT", gwl)
        assert result["code_bss"] == "SHORT"

    def test_full_nan_does_not_crash(self):
        dates = pd.date_range("2020-01-01", periods=1000, freq="D")
        gwl = pd.Series(np.nan, index=dates, name="gwl")
        result = compute_signatures_single("NAN", gwl)
        assert result["code_bss"] == "NAN"
        assert result["success"] is False

    def test_empty_series_does_not_crash(self):
        gwl = pd.Series(dtype=float, index=pd.DatetimeIndex([]), name="gwl")
        result = compute_signatures_single("EMPTY", gwl)
        assert result["code_bss"] == "EMPTY"
        assert result["success"] is False
