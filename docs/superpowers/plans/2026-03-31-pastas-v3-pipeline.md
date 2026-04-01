# Pastas v3 Pipeline Extension — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 3 new Dagster assets to extract groundwater signatures, SGI time series, and full model decomposition+water balance from Pastas, extending the existing `ml.pastas_irf_features` pipeline.

**Architecture:** 3 independent Dagster assets (signatures, SGI, full-refit) following the batch+joblib+UPSERT pattern from `pastas_assets.py`. Signatures and SGI operate on raw GWL series (~15K stations, no Pastas model fit). Full-refit re-fits Pastas on the ~5.4K already-fitted stations to extract decomposition, water balance, enriched metrics, and block response in a single pass.

**Tech Stack:** Pastas 1.10.1 (`ps.stats.signatures`, `ps.stats.dutch`, `ps.stats.sgi`, `model.stats.*`, `RechargeModel.get_water_balance`), Dagster assets, joblib Parallel, PostgreSQL COPY, psycopg2.

**Spec:** `docs/cahier_des_charges_pastas_v3.md` (in `aida_embedding_benchmark`)

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `src/hubeau_pipeline/ml/pastas_signatures.py` | Wrapper: `compute_signatures_single()` (30 sigs + 7 dutch) |
| Create | `src/hubeau_pipeline/assets/pastas_signatures_asset.py` | Dagster asset: batch load, parallel compute, UPSERT |
| Create | `src/hubeau_pipeline/assets/pastas_sgi_asset.py` | Dagster asset: batch load, SGI compute, UPSERT |
| Create | `src/hubeau_pipeline/assets/pastas_refit_asset.py` | Dagster asset: full refit, COPY timeseries, UPDATE scalars |
| Modify | `src/hubeau_pipeline/ml/pastas_wrapper.py` | Add `fit_and_extract_all()`, diagnostic helpers |
| Modify | `src/hubeau_pipeline/assets/__init__.py` | Register 3 new assets in `all_ml_assets` |
| Modify | `src/hubeau_pipeline/jobs/ml_jobs.py` | Add 3 new jobs |
| Modify | `src/hubeau_pipeline/jobs/__init__.py` | Import + register 3 new jobs |
| Create | `tests/test_pastas_signatures.py` | Unit tests for signatures wrapper |
| Create | `tests/test_pastas_sgi.py` | Unit tests for SGI computation |
| Create | `tests/test_pastas_refit.py` | Unit tests for full refit wrapper |

---

## Task 1: Signatures Wrapper (`pastas_signatures.py`)

**Files:**
- Create: `src/hubeau_pipeline/ml/pastas_signatures.py`
- Create: `tests/test_pastas_signatures.py`

- [ ] **Step 1: Write failing tests for `compute_signatures_single`**

Create `tests/test_pastas_signatures.py`:

```python
"""Tests for Pastas groundwater signatures computation."""

import numpy as np
import pandas as pd
import pytest

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
        # Check a few known signatures
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
        # May succeed or fail, but must not crash

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/ringuet/hubeau_data_integration && python -m pytest tests/test_pastas_signatures.py -v`
Expected: `ModuleNotFoundError: No module named 'hubeau_pipeline.ml.pastas_signatures'`

- [ ] **Step 3: Implement `compute_signatures_single`**

Create `src/hubeau_pipeline/ml/pastas_signatures.py`:

```python
"""Pastas groundwater signatures and Dutch statistics computation.

Computes 30 groundwater signatures via ps.stats.signatures.summary()
and 7 Dutch statistics (GHG/GLG/GVG) via ps.stats.dutch.*.

No Pastas model fitting required — operates on raw GWL series.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    import pastas as ps

    PASTAS_AVAILABLE = True
except ImportError:
    PASTAS_AVAILABLE = False

# The 30 signature names from ps.stats.signatures.__all__
SIGNATURE_NAMES = [
    "cv_period_mean", "cv_date_min", "cv_date_max", "cv_fall_rate",
    "cv_rise_rate", "parde_seasonality", "avg_seasonal_fluctuation",
    "interannual_variation", "low_pulse_count", "high_pulse_count",
    "low_pulse_duration", "high_pulse_duration", "bimodality_coefficient",
    "mean_annual_maximum", "rise_rate", "fall_rate", "reversals_avg",
    "reversals_cv", "colwell_contingency", "colwell_constancy",
    "recession_constant", "recovery_constant", "duration_curve_slope",
    "duration_curve_ratio", "richards_pathlength", "baselevel_index",
    "baselevel_stability", "magnitude", "autocorr_time", "date_min", "date_max",
]

DUTCH_STATS = [
    ("gg", "gg"), ("ghg", "ghg"), ("glg", "glg"), ("gvg", "gvg"),
    ("q_ghg", "q_ghg"), ("q_glg", "q_glg"), ("q_gvg", "q_gvg"),
]


def compute_signatures_single(code_bss: str, gwl: pd.Series) -> dict:
    """Compute 30 groundwater signatures + 7 Dutch stats for one station.

    Args:
        code_bss: Station identifier.
        gwl: Groundwater level series with DatetimeIndex.

    Returns:
        Dict with code_bss, 30 signature values, 7 dutch stats, metadata.
        Always returns a dict (success=False on error).
    """
    if not PASTAS_AVAILABLE:
        return _failure_result(code_bss, gwl, "pastas not installed")

    if len(gwl) == 0 or gwl.isna().all():
        return _failure_result(code_bss, gwl, "empty or all-NaN series")

    # Metadata
    valid = gwl.dropna()
    base_meta = {
        "code_bss": code_bss,
        "series_start": gwl.index.min().date(),
        "series_end": gwl.index.max().date(),
        "series_length_days": (gwl.index.max() - gwl.index.min()).days,
        "n_valid_days": len(valid),
    }

    result = {**base_meta}

    try:
        # 30 signatures via ps.stats.signatures.summary()
        sigs_df = ps.stats.signatures.summary(gwl)
        sigs = sigs_df.iloc[:, 0].to_dict()
        result.update(sigs)
        result["n_signatures_computed"] = sum(
            1 for v in sigs.values() if pd.notna(v)
        )
    except Exception as e:
        # Fill all signature keys with None
        for name in SIGNATURE_NAMES:
            result[name] = None
        result["n_signatures_computed"] = 0
        result["success"] = False
        result["error"] = f"signatures failed: {e}"
        return result

    # 7 Dutch stats (each computed independently — partial failure OK)
    for col_name, func_name in DUTCH_STATS:
        try:
            func = getattr(ps.stats, func_name)
            result[col_name] = float(func(gwl))
        except Exception:
            result[col_name] = None

    result["success"] = True
    return result


def _failure_result(code_bss: str, gwl: pd.Series, error: str) -> dict:
    """Build a failure result with metadata."""
    result = {"code_bss": code_bss, "success": False, "error": error}
    if len(gwl) > 0 and not gwl.index.empty:
        result["series_start"] = gwl.index.min().date()
        result["series_end"] = gwl.index.max().date()
        result["series_length_days"] = (gwl.index.max() - gwl.index.min()).days
        result["n_valid_days"] = int(gwl.notna().sum())
    else:
        result["series_start"] = None
        result["series_end"] = None
        result["series_length_days"] = 0
        result["n_valid_days"] = 0
    result["n_signatures_computed"] = 0
    for name in SIGNATURE_NAMES:
        result[name] = None
    for col_name, _ in DUTCH_STATS:
        result[col_name] = None
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/ringuet/hubeau_data_integration && python -m pytest tests/test_pastas_signatures.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/hubeau_pipeline/ml/pastas_signatures.py tests/test_pastas_signatures.py
git commit -m "feat(ml): add Pastas groundwater signatures wrapper (30 sigs + 7 dutch stats)"
```

---

## Task 2: Signatures Dagster Asset (`pastas_signatures_asset.py`)

**Files:**
- Create: `src/hubeau_pipeline/assets/pastas_signatures_asset.py`

- [ ] **Step 1: Create the signatures asset**

Create `src/hubeau_pipeline/assets/pastas_signatures_asset.py`:

```python
"""Dagster Asset — Pastas Groundwater Signatures.

Computes 30 groundwater signatures + 7 Dutch statistics for all eligible
piezometric stations (>= 2 years of data) from gold.hubeau_daily_chroniques.

No Pastas model fitting required — operates directly on raw GWL series.
"""

import logging

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset
from joblib import Parallel, delayed

from ..ml.pastas_signatures import DUTCH_STATS, SIGNATURE_NAMES, compute_signatures_single
from ..resources import PostgreSQLResource

logger = logging.getLogger(__name__)

MIN_DAYS = 730  # >= 2 years of observations
BATCH_SIZE = 500  # stations per batch
N_JOBS = 8  # parallel workers

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ml.pastas_groundwater_signatures (
    code_bss                TEXT PRIMARY KEY,
    cv_period_mean          DOUBLE PRECISION,
    cv_date_min             DOUBLE PRECISION,
    cv_date_max             DOUBLE PRECISION,
    cv_fall_rate            DOUBLE PRECISION,
    cv_rise_rate            DOUBLE PRECISION,
    parde_seasonality       DOUBLE PRECISION,
    avg_seasonal_fluctuation DOUBLE PRECISION,
    interannual_variation   DOUBLE PRECISION,
    low_pulse_count         DOUBLE PRECISION,
    high_pulse_count        DOUBLE PRECISION,
    low_pulse_duration      DOUBLE PRECISION,
    high_pulse_duration     DOUBLE PRECISION,
    bimodality_coefficient  DOUBLE PRECISION,
    mean_annual_maximum     DOUBLE PRECISION,
    rise_rate               DOUBLE PRECISION,
    fall_rate               DOUBLE PRECISION,
    reversals_avg           DOUBLE PRECISION,
    reversals_cv            DOUBLE PRECISION,
    colwell_contingency     DOUBLE PRECISION,
    colwell_constancy       DOUBLE PRECISION,
    recession_constant      DOUBLE PRECISION,
    recovery_constant       DOUBLE PRECISION,
    duration_curve_slope    DOUBLE PRECISION,
    duration_curve_ratio    DOUBLE PRECISION,
    richards_pathlength     DOUBLE PRECISION,
    baselevel_index         DOUBLE PRECISION,
    baselevel_stability     DOUBLE PRECISION,
    magnitude               DOUBLE PRECISION,
    autocorr_time           DOUBLE PRECISION,
    date_min                DOUBLE PRECISION,
    date_max                DOUBLE PRECISION,
    gg                      DOUBLE PRECISION,
    ghg                     DOUBLE PRECISION,
    glg                     DOUBLE PRECISION,
    gvg                     DOUBLE PRECISION,
    q_ghg                   DOUBLE PRECISION,
    q_glg                   DOUBLE PRECISION,
    q_gvg                   DOUBLE PRECISION,
    series_start            DATE,
    series_end              DATE,
    series_length_days      INTEGER,
    n_valid_days            INTEGER,
    n_signatures_computed   INTEGER,
    computed_at             TIMESTAMP DEFAULT NOW()
)
"""

# Column names for UPSERT (excluding code_bss PK)
_VALUE_COLS = (
    SIGNATURE_NAMES
    + [col for col, _ in DUTCH_STATS]
    + ["series_start", "series_end", "series_length_days", "n_valid_days", "n_signatures_computed"]
)

_UPSERT_COLS = ", ".join(["code_bss"] + _VALUE_COLS + ["computed_at"])
_UPSERT_VALS = ", ".join([f"%({c})s" for c in ["code_bss"] + _VALUE_COLS] + ["NOW()"])
_UPSERT_SET = ", ".join([f"{c} = EXCLUDED.{c}" for c in _VALUE_COLS] + ["computed_at = NOW()"])

_UPSERT = f"""
INSERT INTO ml.pastas_groundwater_signatures ({_UPSERT_COLS})
VALUES ({_UPSERT_VALS})
ON CONFLICT (code_bss) DO UPDATE SET {_UPSERT_SET}
"""


def _get_eligible_station_ids(pg: PostgreSQLResource) -> list[str]:
    """Stations with >= MIN_DAYS observations of niveau_nappe_eau."""
    query = f"""
    SELECT code_bss
    FROM gold.hubeau_daily_chroniques
    GROUP BY code_bss
    HAVING COUNT(niveau_nappe_eau) >= {MIN_DAYS}
    ORDER BY code_bss
    """
    with pg.get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df["code_bss"].tolist()


def _load_gwl_batch(pg: PostgreSQLResource, station_ids: list[str]) -> dict[str, pd.Series]:
    """Load GWL series for a batch of stations."""
    placeholders = ",".join(["%s"] * len(station_ids))
    query = f"""
    SELECT code_bss, date, niveau_nappe_eau
    FROM gold.hubeau_daily_chroniques
    WHERE code_bss IN ({placeholders}) AND niveau_nappe_eau IS NOT NULL
    ORDER BY code_bss, date
    """
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, station_ids)
        rows = cur.fetchall()

    if not rows:
        return {}

    df = pd.DataFrame(rows, columns=["code_bss", "date", "niveau_nappe_eau"])
    df["date"] = pd.to_datetime(df["date"])
    df["niveau_nappe_eau"] = pd.to_numeric(df["niveau_nappe_eau"], errors="coerce")

    stations = {}
    for code_bss, group in df.groupby("code_bss"):
        gwl = group.set_index("date")["niveau_nappe_eau"].sort_index()
        stations[code_bss] = gwl

    return stations


def _persist_results(pg: PostgreSQLResource, results: list[dict]) -> int:
    """UPSERT results into ml.pastas_groundwater_signatures."""
    if not results:
        return 0
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS ml")
        cur.execute(_CREATE_TABLE)
        count = 0
        for row in results:
            if not row.get("success"):
                continue
            params = {k: v for k, v in row.items() if k not in ("success", "error")}
            cur.execute(_UPSERT, params)
            count += 1
        conn.commit()
    return count


@asset(
    group_name="ml_piezo",
    deps=["hubeau_daily_chroniques"],
    description="Compute 30 Pastas groundwater signatures + 7 Dutch stats for all eligible stations (>= 2y)",
)
def ml_piezo_groundwater_signatures(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
):
    """Compute signatures in batches and persist to ml.pastas_groundwater_signatures."""
    context.log.info(f"Identifying eligible stations (>= {MIN_DAYS} days)...")
    all_ids = _get_eligible_station_ids(pg)
    context.log.info(f"Found {len(all_ids)} eligible stations")

    if not all_ids:
        context.log.warning("No eligible stations found.")
        return

    # Ensure table exists
    _persist_results(pg, [])

    n_batches = (len(all_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    total_success = 0
    total_fail = 0
    total_persisted = 0

    for batch_idx in range(n_batches):
        batch_ids = all_ids[batch_idx * BATCH_SIZE : (batch_idx + 1) * BATCH_SIZE]
        stations = _load_gwl_batch(pg, batch_ids)

        if not stations:
            context.log.info(f"Batch {batch_idx + 1}/{n_batches}: 0 stations loaded, skipping")
            continue

        results = Parallel(n_jobs=N_JOBS, backend="loky")(
            delayed(compute_signatures_single)(code_bss, gwl)
            for code_bss, gwl in stations.items()
        )

        n_ok = sum(1 for r in results if r.get("success"))
        total_success += n_ok
        total_fail += len(results) - n_ok

        n_persisted = _persist_results(pg, results)
        total_persisted += n_persisted

        context.log.info(
            f"Batch {batch_idx + 1}/{n_batches}: "
            f"{n_ok}/{len(results)} success, {n_persisted} persisted"
        )
        del stations, results

    context.log.info(
        f"Done: {total_success} success, {total_fail} failures, {total_persisted} persisted"
    )
    context.add_output_metadata({
        "n_eligible": MetadataValue.int(len(all_ids)),
        "n_success": MetadataValue.int(total_success),
        "n_failure": MetadataValue.int(total_fail),
        "n_persisted": MetadataValue.int(total_persisted),
    })
```

- [ ] **Step 2: Run linting**

Run: `cd /home/ringuet/hubeau_data_integration && ruff check src/hubeau_pipeline/assets/pastas_signatures_asset.py src/hubeau_pipeline/ml/pastas_signatures.py`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/hubeau_pipeline/assets/pastas_signatures_asset.py
git commit -m "feat(ml): add Dagster asset for groundwater signatures (batch + joblib)"
```

---

## Task 3: SGI Asset (`pastas_sgi_asset.py`)

**Files:**
- Create: `src/hubeau_pipeline/assets/pastas_sgi_asset.py`
- Create: `tests/test_pastas_sgi.py`

- [ ] **Step 1: Write failing SGI tests**

Create `tests/test_pastas_sgi.py`:

```python
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

    def test_sgi_is_monthly(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        sgi = ps.stats.sgi(gwl)
        # ~8 years of data → ~96 monthly values
        assert 80 < len(sgi) < 110

    def test_sgi_roughly_standard_normal(self):
        gwl = _make_synthetic_gwl(n_days=3000)
        sgi = ps.stats.sgi(gwl)
        assert -3 < sgi.mean() < 3
        assert 0.3 < sgi.std() < 2.0

    def test_sgi_short_series_does_not_crash(self):
        gwl = _make_synthetic_gwl(n_days=400)
        # May succeed or raise — we just verify no unhandled crash
        try:
            sgi = ps.stats.sgi(gwl)
            assert isinstance(sgi, pd.Series)
        except Exception:
            pass  # Acceptable to fail on short series
```

- [ ] **Step 2: Run tests to verify they fail or pass (SGI is a Pastas function, no wrapper needed)**

Run: `cd /home/ringuet/hubeau_data_integration && python -m pytest tests/test_pastas_sgi.py -v`
Expected: All PASS (these test `ps.stats.sgi` directly)

- [ ] **Step 3: Create the SGI asset**

Create `src/hubeau_pipeline/assets/pastas_sgi_asset.py`:

```python
"""Dagster Asset — Pastas SGI (Standardized Groundwater Index).

Computes monthly SGI for all eligible piezometric stations (>= 5 years)
from gold.hubeau_daily_chroniques. No Pastas model fitting required.

SGI is a monthly time series (not daily) — normalized to N(0,1).
"""

import logging

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset
from joblib import Parallel, delayed

from ..resources import PostgreSQLResource

logger = logging.getLogger(__name__)

try:
    import pastas as ps

    PASTAS_AVAILABLE = True
except ImportError:
    PASTAS_AVAILABLE = False

MIN_DAYS = 1825  # >= 5 years for meaningful SGI
BATCH_SIZE = 500
N_JOBS = 8

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ml.pastas_sgi (
    code_bss    TEXT NOT NULL,
    date        DATE NOT NULL,
    sgi         DOUBLE PRECISION,
    PRIMARY KEY (code_bss, date)
)
"""

_CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_pastas_sgi_date_brin
    ON ml.pastas_sgi USING BRIN (date);
CREATE INDEX IF NOT EXISTS idx_pastas_sgi_bss
    ON ml.pastas_sgi (code_bss);
"""


def _compute_sgi_single(code_bss: str, gwl: pd.Series) -> dict:
    """Compute SGI for one station.

    Returns dict with code_bss, sgi_series (pd.Series), success, error.
    """
    try:
        sgi = ps.stats.sgi(gwl, timescale_months=1)
        sgi = sgi.dropna()
        if len(sgi) == 0:
            return {"code_bss": code_bss, "sgi_series": None, "success": False, "error": "SGI all NaN"}
        return {"code_bss": code_bss, "sgi_series": sgi, "success": True}
    except Exception as e:
        return {"code_bss": code_bss, "sgi_series": None, "success": False, "error": str(e)}


def _get_eligible_station_ids(pg: PostgreSQLResource) -> list[str]:
    query = f"""
    SELECT code_bss
    FROM gold.hubeau_daily_chroniques
    GROUP BY code_bss
    HAVING COUNT(niveau_nappe_eau) >= {MIN_DAYS}
    ORDER BY code_bss
    """
    with pg.get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df["code_bss"].tolist()


def _load_gwl_batch(pg: PostgreSQLResource, station_ids: list[str]) -> dict[str, pd.Series]:
    placeholders = ",".join(["%s"] * len(station_ids))
    query = f"""
    SELECT code_bss, date, niveau_nappe_eau
    FROM gold.hubeau_daily_chroniques
    WHERE code_bss IN ({placeholders}) AND niveau_nappe_eau IS NOT NULL
    ORDER BY code_bss, date
    """
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, station_ids)
        rows = cur.fetchall()

    if not rows:
        return {}

    df = pd.DataFrame(rows, columns=["code_bss", "date", "niveau_nappe_eau"])
    df["date"] = pd.to_datetime(df["date"])
    df["niveau_nappe_eau"] = pd.to_numeric(df["niveau_nappe_eau"], errors="coerce")

    stations = {}
    for code_bss, group in df.groupby("code_bss"):
        gwl = group.set_index("date")["niveau_nappe_eau"].sort_index()
        stations[code_bss] = gwl
    return stations


def _persist_sgi_batch(pg: PostgreSQLResource, results: list[dict]) -> int:
    """UPSERT SGI time series for a batch of stations."""
    rows_to_insert = []
    for r in results:
        if not r.get("success") or r["sgi_series"] is None:
            continue
        code_bss = r["code_bss"]
        sgi = r["sgi_series"]
        for date_val, sgi_val in sgi.items():
            rows_to_insert.append((code_bss, date_val.date(), float(sgi_val)))

    if not rows_to_insert:
        return 0

    with pg.get_connection() as conn:
        cur = conn.cursor()
        # Use executemany for moderate volume (~180 rows per station x 500 stations = ~90K per batch)
        cur.executemany(
            """INSERT INTO ml.pastas_sgi (code_bss, date, sgi)
               VALUES (%s, %s, %s)
               ON CONFLICT (code_bss, date) DO UPDATE SET sgi = EXCLUDED.sgi""",
            rows_to_insert,
        )
        conn.commit()
    return len(rows_to_insert)


@asset(
    group_name="ml_piezo",
    deps=["hubeau_daily_chroniques"],
    description="Compute monthly SGI (Standardized Groundwater Index) for all eligible stations (>= 5y)",
)
def ml_piezo_sgi(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
):
    """Compute SGI in batches and persist to ml.pastas_sgi."""
    if not PASTAS_AVAILABLE:
        context.log.error("pastas not installed")
        return

    context.log.info(f"Identifying eligible stations (>= {MIN_DAYS} days)...")
    all_ids = _get_eligible_station_ids(pg)
    context.log.info(f"Found {len(all_ids)} eligible stations")

    if not all_ids:
        context.log.warning("No eligible stations found.")
        return

    # Ensure table + indexes exist
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS ml")
        cur.execute(_CREATE_TABLE)
        cur.execute(_CREATE_INDEXES)
        conn.commit()

    n_batches = (len(all_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    total_success = 0
    total_fail = 0
    total_rows = 0

    for batch_idx in range(n_batches):
        batch_ids = all_ids[batch_idx * BATCH_SIZE : (batch_idx + 1) * BATCH_SIZE]
        stations = _load_gwl_batch(pg, batch_ids)

        if not stations:
            continue

        results = Parallel(n_jobs=N_JOBS, backend="loky")(
            delayed(_compute_sgi_single)(code_bss, gwl)
            for code_bss, gwl in stations.items()
        )

        n_ok = sum(1 for r in results if r.get("success"))
        total_success += n_ok
        total_fail += len(results) - n_ok

        n_rows = _persist_sgi_batch(pg, results)
        total_rows += n_rows

        context.log.info(
            f"Batch {batch_idx + 1}/{n_batches}: "
            f"{n_ok}/{len(results)} success, {n_rows} rows inserted"
        )
        del stations, results

    context.log.info(f"Done: {total_success} stations, {total_rows} total SGI rows")
    context.add_output_metadata({
        "n_eligible": MetadataValue.int(len(all_ids)),
        "n_success": MetadataValue.int(total_success),
        "n_failure": MetadataValue.int(total_fail),
        "n_sgi_rows": MetadataValue.int(total_rows),
    })
```

- [ ] **Step 4: Run linting**

Run: `cd /home/ringuet/hubeau_data_integration && ruff check src/hubeau_pipeline/assets/pastas_sgi_asset.py`
Expected: No errors

- [ ] **Step 5: Commit**

```bash
git add src/hubeau_pipeline/assets/pastas_sgi_asset.py tests/test_pastas_sgi.py
git commit -m "feat(ml): add SGI asset (monthly Standardized Groundwater Index)"
```

---

## Task 4: Full Re-fit Wrapper (`pastas_wrapper.py` extension)

**Files:**
- Modify: `src/hubeau_pipeline/ml/pastas_wrapper.py` (add `fit_and_extract_all` + helpers)
- Create: `tests/test_pastas_refit.py`

- [ ] **Step 1: Write failing tests for `fit_and_extract_all`**

Create `tests/test_pastas_refit.py`:

```python
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
        # FlexModel default: 5 components (Sr, R, Ea, Q, Pe)
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/ringuet/hubeau_data_integration && python -m pytest tests/test_pastas_refit.py -v`
Expected: `AttributeError: module 'hubeau_pipeline.ml.pastas_wrapper' has no attribute 'fit_and_extract_all'`

- [ ] **Step 3: Add `fit_and_extract_all` and helpers to `pastas_wrapper.py`**

Append to `src/hubeau_pipeline/ml/pastas_wrapper.py` (after the existing `_failure_result` function at the end of the file):

```python
# ---------------------------------------------------------------------------
# Full re-fit: decomposition + water balance + enriched metrics
# ---------------------------------------------------------------------------


def _diag_pvalue(diag_df: "pd.DataFrame", test_name: str) -> float | None:
    """Extract p-value for a named test from model.stats.diagnostics() DataFrame."""
    try:
        row = diag_df.loc[diag_df["Checks"] == test_name]
        if row.empty:
            return None
        val = row["P-value"].iloc[0]
        return float(val) if pd.notna(val) else None
    except Exception:
        return None


def _diag_stat(diag_df: "pd.DataFrame", test_name: str) -> float | None:
    """Extract statistic value for a named test from diagnostics DataFrame."""
    try:
        row = diag_df.loc[diag_df["Checks"] == test_name]
        if row.empty:
            return None
        val = row["Statistic"].iloc[0]
        return float(val) if pd.notna(val) else None
    except Exception:
        return None


def fit_and_extract_all(
    code_bss: str,
    gwl: pd.Series,
    precip: pd.Series,
    evap: pd.Series,
) -> dict:
    """Fit Pastas model once and extract everything: metrics, diagnostics,
    block response, decomposition time series, and water balance.

    Args:
        code_bss: Station identifier.
        gwl: Groundwater level (m NGF), DatetimeIndex.
        precip: Precipitation (mm/d), DatetimeIndex.
        evap: Evapotranspiration (mm/d, positive), DatetimeIndex.

    Returns:
        {
            "code_bss": str,
            "scalars": dict (kge, mae, aic, bic, pearsonr, diagnostics, block_response),
            "timeseries": pd.DataFrame (simulated, residuals, recharge_contribution, wb_*),
            "fit_success": bool,
            "error": str | None,
        }
    """
    if not PASTAS_AVAILABLE:
        return {"code_bss": code_bss, "scalars": {}, "timeseries": pd.DataFrame(),
                "fit_success": False, "error": "pastas not installed"}

    if len(gwl) == 0 or gwl.dropna().empty:
        return {"code_bss": code_bss, "scalars": {}, "timeseries": pd.DataFrame(),
                "fit_success": False, "error": "empty or all-NaN GWL series"}

    try:
        # Ensure daily frequency
        gwl = gwl.asfreq("D")
        precip = precip.asfreq("D")
        evap = evap.asfreq("D")

        ps.logger.setLevel(logging.ERROR)

        # 1. Build and solve model
        model = ps.Model(gwl.dropna(), name="gwl")
        recharge = ps.RechargeModel(
            precip, evap, rfunc=ps.Gamma(),
            name="recharge", recharge=ps.rch.FlexModel(),
        )
        model.add_stressmodel(recharge)
        model.solve(solver=ps.LeastSquares(), report=False)

        # 2. Enriched fit metrics
        scalars = {}
        for metric in ["kge", "mae", "aic", "bic"]:
            try:
                scalars[metric] = float(getattr(model.stats, metric)())
            except Exception:
                scalars[metric] = None
        try:
            scalars["pearsonr"] = float(model.stats.pearsonr())
        except Exception:
            scalars["pearsonr"] = None

        # 3. Diagnostics
        try:
            diag = model.stats.diagnostics()
            scalars["shapiro_pvalue"] = _diag_pvalue(diag, "Shapiro-Wilk")
            scalars["dagostino_pvalue"] = _diag_pvalue(diag, "D'Agostino")
            scalars["runs_test_pvalue"] = _diag_pvalue(diag, "Runs test")
            scalars["ljung_box_pvalue"] = _diag_pvalue(diag, "Ljung-Box")
            scalars["durbin_watson_stat"] = _diag_stat(diag, "Durbin-Watson")
        except Exception:
            for k in ["shapiro_pvalue", "dagostino_pvalue", "runs_test_pvalue",
                       "ljung_box_pvalue", "durbin_watson_stat"]:
                scalars[k] = None

        # 4. Block response
        try:
            br = model.get_block_response("recharge")
            if br is not None and len(br) > 0:
                scalars["block_response"] = br.values.flatten().tolist()
                scalars["block_response_length"] = len(br)
            else:
                scalars["block_response"] = None
                scalars["block_response_length"] = 0
        except Exception:
            scalars["block_response"] = None
            scalars["block_response_length"] = 0

        # 5. Decomposition time series
        sim = model.simulate()
        res = model.residuals()
        contrib = model.get_contribution("recharge")

        ts = pd.DataFrame({
            "simulated": sim,
            "residuals": res,
            "recharge_contribution": contrib,
        })

        # 6. Water balance (FlexModel default: 5 components)
        try:
            rm = model.stressmodels["recharge"]
            wb = rm.get_water_balance(model.get_parameters("recharge"))
            for col in wb.columns:
                safe_name = "wb_" + col.lower().replace(" ", "_").replace("(", "").replace(")", "")
                ts[safe_name] = wb[col]
        except Exception as e:
            logger.warning(f"{code_bss}: water balance extraction failed: {e}")

        return {
            "code_bss": code_bss,
            "scalars": scalars,
            "timeseries": ts,
            "fit_success": True,
            "error": None,
        }

    except Exception as e:
        return {
            "code_bss": code_bss,
            "scalars": {},
            "timeseries": pd.DataFrame(),
            "fit_success": False,
            "error": str(e),
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/ringuet/hubeau_data_integration && python -m pytest tests/test_pastas_refit.py -v`
Expected: All 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/hubeau_pipeline/ml/pastas_wrapper.py tests/test_pastas_refit.py
git commit -m "feat(ml): add fit_and_extract_all for full Pastas decomposition + water balance"
```

---

## Task 5: Full Re-fit Dagster Asset (`pastas_refit_asset.py`)

**Files:**
- Create: `src/hubeau_pipeline/assets/pastas_refit_asset.py`

- [ ] **Step 1: Create the full re-fit asset**

Create `src/hubeau_pipeline/assets/pastas_refit_asset.py`:

```python
"""Dagster Asset — Pastas Full Re-fit.

Re-fits Pastas TFN model on all successfully-fitted stations from
ml.pastas_irf_features, extracting in a single pass:
- Enriched metrics (KGE, MAE, AIC, BIC, Pearson, diagnostics, block response)
  → UPDATE ml.pastas_irf_features
- Decomposition + water balance time series
  → INSERT ml.pastas_model_timeseries via COPY
"""

import io
import logging

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset
from joblib import Parallel, delayed

from ..ml.pastas_wrapper import fit_and_extract_all
from ..resources import PostgreSQLResource

logger = logging.getLogger(__name__)

BATCH_SIZE = 200
N_JOBS = 8

_ALTER_IRF = """
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS kge DOUBLE PRECISION;
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS mae DOUBLE PRECISION;
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS aic DOUBLE PRECISION;
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS bic DOUBLE PRECISION;
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS pearsonr DOUBLE PRECISION;
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS shapiro_pvalue DOUBLE PRECISION;
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS dagostino_pvalue DOUBLE PRECISION;
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS runs_test_pvalue DOUBLE PRECISION;
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS ljung_box_pvalue DOUBLE PRECISION;
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS durbin_watson_stat DOUBLE PRECISION;
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS block_response DOUBLE PRECISION[];
ALTER TABLE ml.pastas_irf_features ADD COLUMN IF NOT EXISTS block_response_length INTEGER;
ALTER TABLE ml.pastas_irf_features DROP COLUMN IF EXISTS recharge_f;
"""

_CREATE_TIMESERIES = """
CREATE TABLE IF NOT EXISTS ml.pastas_model_timeseries (
    code_bss                TEXT NOT NULL,
    date                    DATE NOT NULL,
    simulated               DOUBLE PRECISION,
    residuals               DOUBLE PRECISION,
    recharge_contribution   DOUBLE PRECISION,
    wb_recharge             DOUBLE PRECISION,
    wb_actual_evaporation   DOUBLE PRECISION,
    wb_surface_runoff       DOUBLE PRECISION,
    wb_effective_precip     DOUBLE PRECISION,
    wb_root_zone_storage    DOUBLE PRECISION,
    PRIMARY KEY (code_bss, date)
)
"""

_CREATE_TS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_pastas_ts_date_brin
    ON ml.pastas_model_timeseries USING BRIN (date);
CREATE INDEX IF NOT EXISTS idx_pastas_ts_bss
    ON ml.pastas_model_timeseries (code_bss);
"""

_UPDATE_IRF = """
UPDATE ml.pastas_irf_features SET
    kge = %(kge)s, mae = %(mae)s, aic = %(aic)s, bic = %(bic)s,
    pearsonr = %(pearsonr)s,
    shapiro_pvalue = %(shapiro_pvalue)s, dagostino_pvalue = %(dagostino_pvalue)s,
    runs_test_pvalue = %(runs_test_pvalue)s, ljung_box_pvalue = %(ljung_box_pvalue)s,
    durbin_watson_stat = %(durbin_watson_stat)s,
    block_response = %(block_response)s, block_response_length = %(block_response_length)s,
    fitted_at = NOW()
WHERE code_bss = %(code_bss)s
"""

# Columns in the timeseries table (order matters for COPY)
_TS_COLS = [
    "code_bss", "date", "simulated", "residuals", "recharge_contribution",
    "wb_recharge", "wb_actual_evaporation", "wb_surface_runoff",
    "wb_effective_precip", "wb_root_zone_storage",
]


def _get_fitted_station_ids(pg: PostgreSQLResource) -> list[str]:
    """Get station IDs that were successfully fitted in irf_features."""
    query = """
    SELECT code_bss FROM ml.pastas_irf_features
    WHERE fit_success = true
    ORDER BY code_bss
    """
    with pg.get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df["code_bss"].tolist()


def _load_station_batch(pg: PostgreSQLResource, station_ids: list[str]) -> dict[str, dict[str, pd.Series]]:
    """Load GWL + precip + evap for a batch of stations."""
    placeholders = ",".join(["%s"] * len(station_ids))
    query = f"""
    SELECT code_bss, date, niveau_nappe_eau, total_precipitation, potential_evaporation
    FROM gold.hubeau_daily_chroniques
    WHERE code_bss IN ({placeholders})
    ORDER BY code_bss, date
    """
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, station_ids)
        rows = cur.fetchall()

    if not rows:
        return {}

    df = pd.DataFrame(rows, columns=["code_bss", "date", "niveau_nappe_eau",
                                      "total_precipitation", "potential_evaporation"])
    df["date"] = pd.to_datetime(df["date"])
    for col in ["niveau_nappe_eau", "total_precipitation", "potential_evaporation"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["potential_evaporation"] = df["potential_evaporation"].abs()

    stations = {}
    for code_bss, group in df.groupby("code_bss"):
        group = group.set_index("date").sort_index()
        stations[code_bss] = {
            "gwl": group["niveau_nappe_eau"],
            "precip": group["total_precipitation"],
            "evap": group["potential_evaporation"],
        }
    return stations


def _update_irf_scalars(pg: PostgreSQLResource, results: list[dict]) -> int:
    """UPDATE enriched metrics in ml.pastas_irf_features."""
    count = 0
    with pg.get_connection() as conn:
        cur = conn.cursor()
        for r in results:
            if not r["fit_success"]:
                continue
            params = {"code_bss": r["code_bss"], **r["scalars"]}
            cur.execute(_UPDATE_IRF, params)
            count += 1
        conn.commit()
    return count


def _normalize_wb_columns(ts: pd.DataFrame) -> pd.DataFrame:
    """Ensure water balance columns match the expected schema names."""
    # FlexModel column names vary — normalize to our schema
    rename_map = {}
    for col in ts.columns:
        if not col.startswith("wb_"):
            continue
        # Map common FlexModel output names to schema
        lower = col.lower()
        if "recharge" in lower and "wb_recharge" not in rename_map.values():
            rename_map[col] = "wb_recharge"
        elif "actual" in lower and "evap" in lower:
            rename_map[col] = "wb_actual_evaporation"
        elif "runoff" in lower or "surface" in lower:
            rename_map[col] = "wb_surface_runoff"
        elif "effective" in lower and "precip" in lower:
            rename_map[col] = "wb_effective_precip"
        elif "root" in lower or ("state" in lower and "sr" in lower.replace("state", "")):
            rename_map[col] = "wb_root_zone_storage"

    if rename_map:
        ts = ts.rename(columns=rename_map)

    # Ensure all expected columns exist
    for expected in ["wb_recharge", "wb_actual_evaporation", "wb_surface_runoff",
                     "wb_effective_precip", "wb_root_zone_storage"]:
        if expected not in ts.columns:
            ts[expected] = np.nan

    return ts


def _copy_timeseries_batch(pg: PostgreSQLResource, results: list[dict]) -> int:
    """Bulk insert time series via COPY for performance."""
    buf = io.StringIO()
    total_rows = 0

    for r in results:
        if not r["fit_success"] or r["timeseries"].empty:
            continue

        code_bss = r["code_bss"]
        ts = _normalize_wb_columns(r["timeseries"].copy())

        for date_val, row in ts.iterrows():
            vals = [code_bss, date_val.strftime("%Y-%m-%d")]
            for col in _TS_COLS[2:]:  # skip code_bss, date
                v = row.get(col)
                vals.append("\\N" if v is None or (isinstance(v, float) and np.isnan(v)) else str(v))
            buf.write("\t".join(vals) + "\n")
            total_rows += 1

    if total_rows == 0:
        return 0

    buf.seek(0)
    with pg.get_connection() as conn:
        cur = conn.cursor()
        # Delete existing rows for these stations first (idempotent re-run)
        station_ids = [r["code_bss"] for r in results if r["fit_success"]]
        if station_ids:
            placeholders = ",".join(["%s"] * len(station_ids))
            cur.execute(f"DELETE FROM ml.pastas_model_timeseries WHERE code_bss IN ({placeholders})",
                        station_ids)
        cur.copy_expert(
            f"COPY ml.pastas_model_timeseries ({', '.join(_TS_COLS)}) FROM STDIN WITH (FORMAT text, NULL '\\N')",
            buf,
        )
        conn.commit()
    return total_rows


@asset(
    group_name="ml_piezo",
    deps=["ml_piezo_pastas_irf_features"],
    description="Re-fit Pastas models: extract decomposition, water balance, enriched metrics (~45-75min)",
)
def ml_piezo_pastas_full_refit(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
):
    """Full re-fit: decomposition + water balance + enriched metrics."""
    # Setup: ALTER TABLE + CREATE TABLE
    context.log.info("Setting up schema (ALTER irf_features + CREATE model_timeseries)...")
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS ml")
        for stmt in _ALTER_IRF.strip().split("\n"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        cur.execute(_CREATE_TIMESERIES)
        cur.execute(_CREATE_TS_INDEXES)
        conn.commit()

    all_ids = _get_fitted_station_ids(pg)
    context.log.info(f"Found {len(all_ids)} successfully-fitted stations for re-fit")

    if not all_ids:
        context.log.warning("No fitted stations found.")
        return

    n_batches = (len(all_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    total_success = 0
    total_fail = 0
    total_irf_updated = 0
    total_ts_rows = 0

    for batch_idx in range(n_batches):
        batch_ids = all_ids[batch_idx * BATCH_SIZE : (batch_idx + 1) * BATCH_SIZE]
        stations = _load_station_batch(pg, batch_ids)

        if not stations:
            continue

        results = Parallel(n_jobs=N_JOBS, backend="loky")(
            delayed(fit_and_extract_all)(code_bss, data["gwl"], data["precip"], data["evap"])
            for code_bss, data in stations.items()
        )

        n_ok = sum(1 for r in results if r["fit_success"])
        total_success += n_ok
        total_fail += len(results) - n_ok

        # Persist scalars (UPDATE irf_features)
        n_updated = _update_irf_scalars(pg, results)
        total_irf_updated += n_updated

        # Persist time series (COPY)
        n_rows = _copy_timeseries_batch(pg, results)
        total_ts_rows += n_rows

        context.log.info(
            f"Batch {batch_idx + 1}/{n_batches}: "
            f"{n_ok}/{len(results)} fit success, "
            f"{n_updated} irf updated, {n_rows} ts rows"
        )

        # Log failures
        for r in results:
            if not r["fit_success"]:
                context.log.warning(f"  FAIL {r['code_bss']}: {r.get('error', 'unknown')}")

        del stations, results

    context.log.info(
        f"Done: {total_success} success, {total_fail} fail, "
        f"{total_irf_updated} irf updated, {total_ts_rows} ts rows"
    )
    context.add_output_metadata({
        "n_stations": MetadataValue.int(len(all_ids)),
        "n_fit_success": MetadataValue.int(total_success),
        "n_fit_failure": MetadataValue.int(total_fail),
        "n_irf_updated": MetadataValue.int(total_irf_updated),
        "n_timeseries_rows": MetadataValue.int(total_ts_rows),
    })
```

- [ ] **Step 2: Run linting**

Run: `cd /home/ringuet/hubeau_data_integration && ruff check src/hubeau_pipeline/assets/pastas_refit_asset.py`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/hubeau_pipeline/assets/pastas_refit_asset.py
git commit -m "feat(ml): add full re-fit asset (decomposition + water balance + enriched metrics)"
```

---

## Task 6: Register Assets and Jobs

**Files:**
- Modify: `src/hubeau_pipeline/assets/__init__.py`
- Modify: `src/hubeau_pipeline/jobs/ml_jobs.py`
- Modify: `src/hubeau_pipeline/jobs/__init__.py`

- [ ] **Step 1: Register 3 new assets in `assets/__init__.py`**

In `src/hubeau_pipeline/assets/__init__.py`, add imports after line 36 (`from .pastas_assets import ml_piezo_pastas_irf_features`):

```python
from .pastas_refit_asset import ml_piezo_pastas_full_refit
from .pastas_sgi_asset import ml_piezo_sgi
from .pastas_signatures_asset import ml_piezo_groundwater_signatures
```

Add to `all_ml_assets` list (after `ml_piezo_pastas_irf_features` on line 72):

```python
    ml_piezo_groundwater_signatures,
    ml_piezo_sgi,
    ml_piezo_pastas_full_refit,
```

- [ ] **Step 2: Add 3 new jobs in `jobs/ml_jobs.py`**

Append to `src/hubeau_pipeline/jobs/ml_jobs.py` after line 61:

```python

# Pastas groundwater signatures (manual — launch from Dagster UI)
pastas_signatures_job = define_asset_job(
    name="pastas_signatures_job",
    selection=AssetSelection.assets("ml_piezo_groundwater_signatures"),
    description="Compute 30 groundwater signatures + 7 Dutch stats for all eligible stations (~20-30min)",
)

# Pastas SGI (manual — launch from Dagster UI)
pastas_sgi_job = define_asset_job(
    name="pastas_sgi_job",
    selection=AssetSelection.assets("ml_piezo_sgi"),
    description="Compute monthly SGI for all eligible stations (~15-20min)",
)

# Pastas full re-fit (manual — launch from Dagster UI)
pastas_full_refit_job = define_asset_job(
    name="pastas_full_refit_job",
    selection=AssetSelection.assets("ml_piezo_pastas_full_refit"),
    description="Re-fit Pastas: decomposition + water balance + enriched metrics (~45-75min)",
)
```

Update the module docstring at the top to say `12 jobs` instead of `9 jobs`.

- [ ] **Step 3: Register 3 new jobs in `jobs/__init__.py`**

Add to the imports from `ml_jobs` (around line 54-64):

```python
    pastas_full_refit_job,
    pastas_sgi_job,
    pastas_signatures_job,
```

Add to `all_jobs` list (after `pastas_irf_features_job` on line 114):

```python
    pastas_signatures_job,
    pastas_sgi_job,
    pastas_full_refit_job,
```

Add to `__all__` list (after `"pastas_irf_features_job"` on line 162):

```python
    "pastas_signatures_job",
    "pastas_sgi_job",
    "pastas_full_refit_job",
```

- [ ] **Step 4: Run linting on all modified files**

Run: `cd /home/ringuet/hubeau_data_integration && ruff check src/hubeau_pipeline/assets/__init__.py src/hubeau_pipeline/jobs/ml_jobs.py src/hubeau_pipeline/jobs/__init__.py`
Expected: No errors

- [ ] **Step 5: Run all tests**

Run: `cd /home/ringuet/hubeau_data_integration && python -m pytest tests/ -v`
Expected: All tests PASS (existing + 3 new test files)

- [ ] **Step 6: Commit**

```bash
git add src/hubeau_pipeline/assets/__init__.py src/hubeau_pipeline/jobs/ml_jobs.py src/hubeau_pipeline/jobs/__init__.py
git commit -m "feat(ml): register 3 new Pastas assets + jobs (signatures, SGI, full refit)"
```

---

## Task 7: Verify Dagster Definitions Load

- [ ] **Step 1: Verify Python imports resolve**

Run: `cd /home/ringuet/hubeau_data_integration && python -c "from hubeau_pipeline.assets import all_assets; print(f'{len(all_assets)} assets loaded')"`
Expected: `16 assets loaded` (was 13 bronze + dbt + ml, now +3)

Note: The dbt asset is a single `@dbt_assets` that auto-discovers models, so the count depends on how Dagster counts them. The key assertion is no ImportError.

- [ ] **Step 2: Verify jobs load**

Run: `cd /home/ringuet/hubeau_data_integration && python -c "from hubeau_pipeline.jobs import all_jobs; print(f'{len(all_jobs)} jobs loaded')"`
Expected: `37 jobs loaded` (was 34, now +3)

- [ ] **Step 3: Commit final (if any linting fixes needed)**

```bash
git add -A
git commit -m "chore: finalize Pastas v3 pipeline extension"
```
