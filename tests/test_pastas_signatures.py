"""Tests for Pastas groundwater signatures computation."""

import numpy as np
import pandas as pd

from hubeau_pipeline.assets.pastas_signatures_asset import (
    SIGNATURE_NAMES,
    compute_signatures,
)


def _make_synthetic_gwl(n_days: int = 3000, seed: int = 42) -> pd.Series:
    """Synthetic daily GWL: sinusoidal annual cycle + noise."""
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2015-01-01", periods=n_days, freq="D")
    t = np.arange(n_days)
    gwl = 50.0 + 2.0 * np.sin(2 * np.pi * t / 365) + rng.normal(0, 0.3, n_days)
    return pd.Series(gwl, index=dates, name="gwl")


class TestComputeSignatures:
    def test_success_on_long_series(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        result = compute_signatures("TEST_001", gwl)
        assert result["code_bss"] == "TEST_001"
        assert result["success"] is True
        assert "error" not in result

    def test_29_signatures_present(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        result = compute_signatures("TEST_001", gwl)
        assert result["success"] is True
        for name in SIGNATURE_NAMES:
            assert name in result, f"Missing signature: {name}"

    def test_most_signatures_non_null(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        result = compute_signatures("TEST_001", gwl)
        assert result["success"] is True
        assert result["n_signatures_computed"] >= 25

    def test_no_dutch_stats(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        result = compute_signatures("TEST_001", gwl)
        for key in ["gg", "ghg", "glg", "gvg", "q_ghg", "q_glg", "q_gvg"]:
            assert key not in result, f"Dutch stat {key} should not be present"

    def test_metadata_fields(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        result = compute_signatures("TEST_001", gwl)
        assert result["series_start"] is not None
        assert result["series_end"] is not None
        assert result["series_length_days"] > 0
        assert result["n_valid_days"] > 0
        assert result["pastas_version"] is not None

    def test_inf_sanitized(self):
        """Signatures producing inf (e.g., magnitude when min~0) are replaced with None."""
        gwl = _make_synthetic_gwl(n_days=3000)
        result = compute_signatures("TEST_001", gwl)
        for name in SIGNATURE_NAMES:
            val = result[name]
            if val is not None:
                assert not np.isinf(val), f"{name} is inf"

    def test_full_nan_returns_failure(self):
        dates = pd.date_range("2020-01-01", periods=1000, freq="D")
        gwl = pd.Series(np.nan, index=dates, name="gwl")
        result = compute_signatures("NAN", gwl)
        assert result["code_bss"] == "NAN"
        assert result["success"] is False

    def test_empty_series_returns_failure(self):
        gwl = pd.Series(dtype=float, index=pd.DatetimeIndex([]), name="gwl")
        result = compute_signatures("EMPTY", gwl)
        assert result["code_bss"] == "EMPTY"
        assert result["success"] is False

    def test_short_series_does_not_crash(self):
        gwl = _make_synthetic_gwl(n_days=300)
        result = compute_signatures("SHORT", gwl)
        assert result["code_bss"] == "SHORT"
        # May succeed or fail depending on Pastas, but must not raise
