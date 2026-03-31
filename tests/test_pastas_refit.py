"""Tests for Pastas full re-fit: decomposition + water balance + enriched metrics."""

import numpy as np
import pandas as pd

from hubeau_pipeline.ml.pastas_wrapper import fit_and_extract_all


def _make_synthetic_station(
    n_days: int = 3000, seed: int = 42,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2015-01-01", periods=n_days, freq="D")
    t = np.arange(n_days)
    gwl = 50.0 + 2.0 * np.sin(2 * np.pi * t / 365) + rng.normal(0, 0.3, n_days)
    precip = rng.exponential(3.0, n_days)
    evap = np.clip(2.0 + 1.5 * np.sin(2 * np.pi * t / 365 + np.pi) + rng.normal(0, 0.2, n_days), 0.1, None)
    return (
        pd.Series(gwl, index=dates, name="gwl"),
        pd.Series(precip, index=dates, name="precip"),
        pd.Series(evap, index=dates, name="evap"),
    )


class TestFitAndExtractAll:
    def test_success_returns_scalars_and_timeseries(self):
        gwl, precip, evap = _make_synthetic_station(n_days=3000)
        result = fit_and_extract_all("TEST_001", gwl, precip, evap)

        assert result["fit_success"] is True
        assert "scalars" in result
        assert "timeseries" in result
        assert result["error"] is None

    def test_enriched_metrics_present(self):
        gwl, precip, evap = _make_synthetic_station(n_days=3000)
        result = fit_and_extract_all("TEST_001", gwl, precip, evap)
        s = result["scalars"]
        assert s["kge"] is not None
        assert s["mae"] is not None
        assert s["mae"] >= 0
        assert s["aic"] is not None
        assert s["bic"] is not None
        assert s["pearsonr"] is not None

    def test_diagnostics_present(self):
        gwl, precip, evap = _make_synthetic_station(n_days=3000)
        result = fit_and_extract_all("TEST_001", gwl, precip, evap)
        s = result["scalars"]
        assert s["shapiro_pvalue"] is not None
        assert 0 <= s["shapiro_pvalue"] <= 1
        assert s["dagostino_pvalue"] is not None
        assert s["durbin_watson_stat"] is not None

    def test_block_response_is_list(self):
        gwl, precip, evap = _make_synthetic_station(n_days=3000)
        result = fit_and_extract_all("TEST_001", gwl, precip, evap)
        s = result["scalars"]
        assert isinstance(s["block_response"], list)
        assert len(s["block_response"]) > 0
        assert s["block_response_length"] == len(s["block_response"])

    def test_timeseries_decomposition_columns(self):
        gwl, precip, evap = _make_synthetic_station(n_days=3000)
        result = fit_and_extract_all("TEST_001", gwl, precip, evap)
        ts = result["timeseries"]
        assert "simulated" in ts.columns
        assert "residuals" in ts.columns
        assert "recharge_contribution" in ts.columns
        assert len(ts) > 0

    def test_timeseries_water_balance_columns(self):
        gwl, precip, evap = _make_synthetic_station(n_days=3000)
        result = fit_and_extract_all("TEST_001", gwl, precip, evap)
        ts = result["timeseries"]
        wb_cols = [c for c in ts.columns if c.startswith("wb_")]
        assert len(wb_cols) >= 5

    def test_nan_gwl_fails_gracefully(self):
        dates = pd.date_range("2018-01-01", periods=1000, freq="D")
        gwl = pd.Series(np.nan, index=dates)
        precip = pd.Series(3.0, index=dates)
        evap = pd.Series(2.0, index=dates)
        result = fit_and_extract_all("NAN", gwl, precip, evap)
        assert result["fit_success"] is False
        assert result["error"] is not None

    def test_empty_series_fails_gracefully(self):
        dates = pd.DatetimeIndex([])
        gwl = pd.Series(dtype=float, index=dates)
        precip = pd.Series(dtype=float, index=dates)
        evap = pd.Series(dtype=float, index=dates)
        result = fit_and_extract_all("EMPTY", gwl, precip, evap)
        assert result["fit_success"] is False
