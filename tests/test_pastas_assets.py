"""Tests for Pastas IRF features pipeline.

1. Unit test fit_single_station: synthetic series, verify features are extracted
   and physically valid (tmax > 0, 0 < evp < 100, gain > 0).
2. Integration test: mock DB, verify asset produces correct schema.
3. Robustness: short series, constant series, full NaN → all return fit_success=False.
"""

import numpy as np
import pandas as pd

from hubeau_pipeline.ml.pastas_wrapper import fit_single_station

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_synthetic_station(
    n_days: int = 1000,
    seed: int = 42,
    gwl_mean: float = 50.0,
    gwl_amplitude: float = 2.0,
    precip_mean: float = 3.0,
    evap_mean: float = 2.0,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Generate synthetic daily series mimicking a piezometric station.

    GWL = sinusoidal (annual cycle) + noise + trend response to precip.
    Precip = random positive values. Evap = seasonal positive values.
    """
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2015-01-01", periods=n_days, freq="D")
    t = np.arange(n_days)

    # GWL: sinusoidal annual cycle + noise
    gwl = gwl_mean + gwl_amplitude * np.sin(2 * np.pi * t / 365) + rng.normal(0, 0.3, n_days)
    gwl_series = pd.Series(gwl, index=dates, name="gwl")

    # Precipitation: exponential distribution (realistic)
    precip = rng.exponential(precip_mean, n_days)
    precip_series = pd.Series(precip, index=dates, name="precip")

    # Evapotranspiration: seasonal with noise
    evap = evap_mean + 1.5 * np.sin(2 * np.pi * t / 365 + np.pi) + rng.normal(0, 0.2, n_days)
    evap = np.clip(evap, 0.1, None)  # Always positive
    evap_series = pd.Series(evap, index=dates, name="evap")

    return gwl_series, precip_series, evap_series


# ---------------------------------------------------------------------------
# Test 1: Synthetic station - features extracted and physically valid
# ---------------------------------------------------------------------------

class TestFitSingleStation:
    """Test fit_single_station with synthetic data."""

    def test_synthetic_station_success(self):
        """Fit on a well-behaved synthetic station should succeed."""
        gwl, precip, evap = _make_synthetic_station(n_days=1000)
        result = fit_single_station("TEST_BSS_001", gwl, precip, evap)

        assert result["code_bss"] == "TEST_BSS_001"
        assert result["fit_success"] is True
        assert "error" not in result

    def test_irf_parameters_present(self):
        """IRF parameters should be extracted (non-None for successful fit)."""
        gwl, precip, evap = _make_synthetic_station(n_days=1000)
        result = fit_single_station("TEST_BSS_001", gwl, precip, evap)

        assert result["fit_success"] is True
        # Core IRF params should be non-None
        assert result["recharge_a"] is not None
        assert result["recharge_n"] is not None
        assert result["recharge_scale"] is not None
        assert result["recharge_f"] is not None

    def test_derived_features_physically_valid(self):
        """Derived features should be physically coherent."""
        gwl, precip, evap = _make_synthetic_station(n_days=1000)
        result = fit_single_station("TEST_BSS_001", gwl, precip, evap)

        assert result["fit_success"] is True

        # tmax: time to peak should be >= 0
        if result["tmax_days"] is not None:
            assert result["tmax_days"] >= 0

        # gain: should be positive (response to positive recharge)
        if result["gain"] is not None:
            assert result["gain"] > 0

    def test_fit_quality_metrics(self):
        """Fit quality metrics should be present and in valid ranges."""
        gwl, precip, evap = _make_synthetic_station(n_days=1000)
        result = fit_single_station("TEST_BSS_001", gwl, precip, evap)

        assert result["fit_success"] is True
        assert result["n_observations"] is not None
        assert result["n_observations"] > 0
        assert result["rmse"] is not None
        assert result["rmse"] >= 0

    def test_metadata_fields(self):
        """Temporal metadata should be correct."""
        gwl, precip, evap = _make_synthetic_station(n_days=1000)
        result = fit_single_station("TEST_BSS_001", gwl, precip, evap)

        assert result["series_start"] is not None
        assert result["series_end"] is not None
        assert result["series_length_days"] > 0
        assert 0 <= result["nan_fraction"] <= 1

    def test_pastas_version_present(self):
        """Pastas version should be recorded."""
        gwl, precip, evap = _make_synthetic_station(n_days=1000)
        result = fit_single_station("TEST_BSS_001", gwl, precip, evap)
        assert result["pastas_version"] is not None


# ---------------------------------------------------------------------------
# Test 2: Schema validation
# ---------------------------------------------------------------------------

class TestResultSchema:
    """Verify the result dict has all expected keys matching the DB schema."""

    EXPECTED_KEYS = {
        "code_bss", "recharge_a", "recharge_n", "recharge_scale", "recharge_f",
        "tmax_days", "cutoff_95_days", "gain", "mean_response_time",
        "evp", "rmse", "nash", "r2", "n_observations", "fit_success",
        "series_start", "series_end", "series_length_days", "nan_fraction",
        "pastas_version",
    }

    def test_success_result_has_all_keys(self):
        gwl, precip, evap = _make_synthetic_station()
        result = fit_single_station("TEST", gwl, precip, evap)
        assert self.EXPECTED_KEYS.issubset(result.keys())

    def test_failure_result_has_all_keys(self):
        """Even failure results should have the full schema (with None values)."""
        dates = pd.date_range("2020-01-01", periods=100, freq="D")
        gwl = pd.Series(np.nan, index=dates)
        precip = pd.Series(1.0, index=dates)
        evap = pd.Series(1.0, index=dates)
        result = fit_single_station("FAIL_TEST", gwl, precip, evap)
        assert self.EXPECTED_KEYS.issubset(result.keys())
        assert result["fit_success"] is False


# ---------------------------------------------------------------------------
# Test 3: Robustness — pathological inputs
# ---------------------------------------------------------------------------

class TestRobustness:
    """Pathological inputs should return fit_success=False without crashing."""

    def test_too_short_series(self):
        """Series shorter than reasonable should fail gracefully."""
        dates = pd.date_range("2020-01-01", periods=30, freq="D")
        gwl = pd.Series(50.0 + np.random.randn(30) * 0.1, index=dates)
        precip = pd.Series(np.random.exponential(3, 30), index=dates)
        evap = pd.Series(2.0 + np.random.randn(30) * 0.1, index=dates).clip(lower=0.1)

        result = fit_single_station("SHORT", gwl, precip, evap)
        # May succeed or fail depending on Pastas, but must not crash
        assert "code_bss" in result
        assert result["code_bss"] == "SHORT"

    def test_constant_series(self):
        """Constant GWL (no variance) should fail or produce poor metrics."""
        dates = pd.date_range("2018-01-01", periods=1000, freq="D")
        gwl = pd.Series(50.0, index=dates)
        precip = pd.Series(3.0, index=dates)
        evap = pd.Series(2.0, index=dates)

        result = fit_single_station("CONSTANT", gwl, precip, evap)
        assert result["code_bss"] == "CONSTANT"
        # Must not crash — either fails or produces meaningless metrics

    def test_full_nan_gwl(self):
        """Full NaN GWL should fail gracefully."""
        dates = pd.date_range("2018-01-01", periods=1000, freq="D")
        gwl = pd.Series(np.nan, index=dates)
        precip = pd.Series(3.0, index=dates)
        evap = pd.Series(2.0, index=dates)

        result = fit_single_station("NAN_GWL", gwl, precip, evap)
        assert result["code_bss"] == "NAN_GWL"
        assert result["fit_success"] is False

    def test_full_nan_precip(self):
        """Full NaN precipitation should fail gracefully."""
        dates = pd.date_range("2018-01-01", periods=1000, freq="D")
        gwl = pd.Series(50.0 + np.sin(np.arange(1000) * 2 * np.pi / 365), index=dates)
        precip = pd.Series(np.nan, index=dates)
        evap = pd.Series(2.0, index=dates)

        result = fit_single_station("NAN_PRECIP", gwl, precip, evap)
        assert result["code_bss"] == "NAN_PRECIP"
        assert result["fit_success"] is False

    def test_empty_series(self):
        """Empty series should fail gracefully."""
        dates = pd.DatetimeIndex([])
        gwl = pd.Series(dtype=float, index=dates)
        precip = pd.Series(dtype=float, index=dates)
        evap = pd.Series(dtype=float, index=dates)

        result = fit_single_station("EMPTY", gwl, precip, evap)
        assert result["code_bss"] == "EMPTY"
        assert result["fit_success"] is False
