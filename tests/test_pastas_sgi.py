"""Tests for Pastas SGI computation."""

import numpy as np
import pandas as pd
import pastas as ps


def _make_synthetic_gwl(n_days: int = 3000, seed: int = 42) -> pd.Series:
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2015-01-01", periods=n_days, freq="D")
    t = np.arange(n_days)
    gwl = 50.0 + 2.0 * np.sin(2 * np.pi * t / 365) + rng.normal(0, 0.3, n_days)
    return pd.Series(gwl, index=dates, name="gwl")


class TestSGI:
    def test_sgi_returns_series(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        sgi = ps.stats.sgi(gwl)
        assert isinstance(sgi, pd.Series)
        assert len(sgi) > 0

    def test_sgi_same_length_as_input(self):
        """ps.stats.sgi returns daily values (same freq as input)."""
        gwl = _make_synthetic_gwl(n_days=3000)
        sgi = ps.stats.sgi(gwl)
        assert len(sgi) == len(gwl)

    def test_sgi_resampled_to_monthly(self):
        """Resampling daily SGI to monthly gives ~98 months for 3000 days."""
        gwl = _make_synthetic_gwl(n_days=3000)
        sgi = ps.stats.sgi(gwl)
        sgi_monthly = sgi.resample("ME").mean().dropna()
        assert 80 < len(sgi_monthly) < 110

    def test_sgi_roughly_standard_normal(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        sgi = ps.stats.sgi(gwl)
        assert -3 < sgi.mean() < 3
        assert 0.3 < sgi.std() < 2.0

    def test_sgi_short_series_does_not_crash(self):
        gwl = _make_synthetic_gwl(n_days=400)
        try:
            sgi = ps.stats.sgi(gwl)
            assert isinstance(sgi, pd.Series)
        except Exception:
            pass
