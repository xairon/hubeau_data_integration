# Pastas Groundwater Signatures Rework — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework `ml_piezo_groundwater_signatures` asset — remove Dutch stats, inline signature logic, align schema on Pastas `__all__`, add robustness guards.

**Architecture:** Single Dagster asset calling `ps.stats.signatures.summary()` directly (no wrapper). DROP+recreate table for schema migration. Same AIDA eligibility filter as other Pastas assets.

**Tech Stack:** Pastas >= 1.7.0, Dagster, PostgreSQL, joblib

**Spec:** `docs/superpowers/specs/2026-04-07-pastas-signatures-rework-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Rewrite | `src/hubeau_pipeline/assets/pastas_signatures_asset.py` | Asset + inline signature computation |
| Delete | `src/hubeau_pipeline/ml/pastas_signatures.py` | No longer needed |
| Rewrite | `tests/test_pastas_signatures.py` | Tests updated for new interface |
| Minor edit | `src/hubeau_pipeline/assets/__init__.py` | Remove stale import path |

---

### Task 1: Rewrite the asset file

**Files:**
- Rewrite: `src/hubeau_pipeline/assets/pastas_signatures_asset.py`

- [ ] **Step 1: Replace `pastas_signatures_asset.py` with the new implementation**

```python
"""Dagster Asset — Pastas Groundwater Signatures.

Computes 31 groundwater signatures for all eligible piezometric stations
from gold.hubeau_daily_chroniques using ps.stats.signatures.summary().

Uses AIDA eligibility criteria: >= 10y temporal span, all 3 variables
present (GWL + precip + evap), and at least one 365-day window with <= 10% NaN.

No Pastas model fitting required — operates directly on raw GWL series.
"""

import logging

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset
from joblib import Parallel, delayed

from ..resources import PostgreSQLResource
from .pastas_assets import MIN_YEARS, _check_station_quality

logger = logging.getLogger(__name__)

try:
    import pastas as ps

    PASTAS_AVAILABLE = True
except ImportError:
    PASTAS_AVAILABLE = False

BATCH_SIZE = 500
N_JOBS = 8

# 31 signatures from ps.stats.signatures.__all__
SIGNATURE_NAMES = [
    "cv_period_mean",
    "cv_date_min",
    "cv_date_max",
    "cv_fall_rate",
    "cv_rise_rate",
    "parde_seasonality",
    "avg_seasonal_fluctuation",
    "interannual_variation",
    "low_pulse_count",
    "high_pulse_count",
    "low_pulse_duration",
    "high_pulse_duration",
    "bimodality_coefficient",
    "mean_annual_maximum",
    "rise_rate",
    "fall_rate",
    "reversals_avg",
    "reversals_cv",
    "colwell_contingency",
    "colwell_constancy",
    "recession_constant",
    "recovery_constant",
    "duration_curve_slope",
    "duration_curve_ratio",
    "richards_pathlength",
    "baselevel_index",
    "baselevel_stability",
    "magnitude",
    "autocorr_time",
    "date_min",
    "date_max",
]

_META_COLS = [
    "series_start",
    "series_end",
    "series_length_days",
    "n_valid_days",
    "n_signatures_computed",
    "pastas_version",
]

_ALL_VALUE_COLS = SIGNATURE_NAMES + _META_COLS

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ml.pastas_groundwater_signatures (
    code_bss                 TEXT PRIMARY KEY,
    cv_period_mean           DOUBLE PRECISION,
    cv_date_min              DOUBLE PRECISION,
    cv_date_max              DOUBLE PRECISION,
    cv_fall_rate             DOUBLE PRECISION,
    cv_rise_rate             DOUBLE PRECISION,
    parde_seasonality        DOUBLE PRECISION,
    avg_seasonal_fluctuation DOUBLE PRECISION,
    interannual_variation    DOUBLE PRECISION,
    low_pulse_count          DOUBLE PRECISION,
    high_pulse_count         DOUBLE PRECISION,
    low_pulse_duration       DOUBLE PRECISION,
    high_pulse_duration      DOUBLE PRECISION,
    bimodality_coefficient   DOUBLE PRECISION,
    mean_annual_maximum      DOUBLE PRECISION,
    rise_rate                DOUBLE PRECISION,
    fall_rate                DOUBLE PRECISION,
    reversals_avg            DOUBLE PRECISION,
    reversals_cv             DOUBLE PRECISION,
    colwell_contingency      DOUBLE PRECISION,
    colwell_constancy        DOUBLE PRECISION,
    recession_constant       DOUBLE PRECISION,
    recovery_constant        DOUBLE PRECISION,
    duration_curve_slope     DOUBLE PRECISION,
    duration_curve_ratio     DOUBLE PRECISION,
    richards_pathlength      DOUBLE PRECISION,
    baselevel_index          DOUBLE PRECISION,
    baselevel_stability      DOUBLE PRECISION,
    magnitude                DOUBLE PRECISION,
    autocorr_time            DOUBLE PRECISION,
    date_min                 DOUBLE PRECISION,
    date_max                 DOUBLE PRECISION,
    series_start             DATE,
    series_end               DATE,
    series_length_days       INTEGER,
    n_valid_days             INTEGER,
    n_signatures_computed    INTEGER,
    pastas_version           TEXT,
    computed_at              TIMESTAMP DEFAULT NOW()
)
"""

_UPSERT_COLS = ", ".join(["code_bss"] + _ALL_VALUE_COLS + ["computed_at"])
_UPSERT_VALS = ", ".join([f"%({c})s" for c in ["code_bss"] + _ALL_VALUE_COLS] + ["NOW()"])
_UPSERT_SET = ", ".join([f"{c} = EXCLUDED.{c}" for c in _ALL_VALUE_COLS] + ["computed_at = NOW()"])

_UPSERT = f"""
INSERT INTO ml.pastas_groundwater_signatures ({_UPSERT_COLS})
VALUES ({_UPSERT_VALS})
ON CONFLICT (code_bss) DO UPDATE SET {_UPSERT_SET}
"""


def _sanitize_value(v):
    """Replace inf/-inf with None, keep NaN as None for DB."""
    if v is None:
        return None
    if isinstance(v, float) and (np.isinf(v) or np.isnan(v)):
        return None
    return v


def compute_signatures(code_bss: str, gwl: pd.Series) -> dict:
    """Compute 31 Pastas signatures for one station.

    Returns dict with code_bss, 31 signature values (sanitized),
    metadata, and success flag.
    """
    if not PASTAS_AVAILABLE:
        return _failure_result(code_bss, gwl, "pastas not installed")

    if len(gwl) == 0 or gwl.isna().all():
        return _failure_result(code_bss, gwl, "empty or all-NaN series")

    valid = gwl.dropna()
    base_meta = {
        "code_bss": code_bss,
        "series_start": gwl.index.min().date(),
        "series_end": gwl.index.max().date(),
        "series_length_days": (gwl.index.max() - gwl.index.min()).days,
        "n_valid_days": len(valid),
        "pastas_version": ps.__version__,
    }

    try:
        sigs_df = ps.stats.signatures.summary(gwl)
        sigs = sigs_df.iloc[:, 0].to_dict()

        result = {**base_meta}
        n_computed = 0
        for name in SIGNATURE_NAMES:
            raw = sigs.get(name)
            val = _sanitize_value(raw)
            result[name] = val
            if val is not None:
                n_computed += 1

        result["n_signatures_computed"] = n_computed
        result["success"] = True

        if n_computed < len(SIGNATURE_NAMES) // 2:
            logger.warning(
                "%s: only %d/%d signatures computed (>50%% NaN)",
                code_bss,
                n_computed,
                len(SIGNATURE_NAMES),
            )

        return result

    except Exception as e:
        return _failure_result(code_bss, gwl, str(e))


def _failure_result(code_bss: str, gwl: pd.Series, error: str) -> dict:
    """Build a failure result dict."""
    result: dict = {"code_bss": code_bss, "success": False, "error": error}
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
    result["pastas_version"] = ps.__version__ if PASTAS_AVAILABLE else None
    for name in SIGNATURE_NAMES:
        result[name] = None
    return result


def _get_eligible_station_ids(pg: PostgreSQLResource) -> list[str]:
    """Stations passing AIDA SQL pre-filter (>= 10y span, all 3 vars present)."""
    min_days = MIN_YEARS * 365
    query = f"""
    SELECT code_bss
    FROM gold.hubeau_daily_chroniques
    GROUP BY code_bss
    HAVING MAX(date) - MIN(date) >= {min_days}
       AND COUNT(niveau_nappe_eau) > 0
       AND COUNT(total_precipitation) > 0
       AND COUNT(potential_evaporation) > 0
    ORDER BY code_bss
    """
    with pg.get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df["code_bss"].tolist()


def _load_gwl_batch(pg: PostgreSQLResource, station_ids: list[str]) -> dict[str, pd.Series]:
    """Load GWL series for a batch of stations, applying AIDA quality filter."""
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
        if _check_station_quality(gwl):
            stations[code_bss] = gwl

    return stations


def _persist_results(pg: PostgreSQLResource, results: list[dict]) -> int:
    """UPSERT results into ml.pastas_groundwater_signatures."""
    if not results:
        return 0
    with pg.get_connection() as conn:
        cur = conn.cursor()
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
    description=(
        "Compute 31 Pastas groundwater signatures "
        "(AIDA criteria: >= 10y span + quality filter)"
    ),
)
def ml_piezo_groundwater_signatures(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
):
    """Compute signatures in batches and persist to ml.pastas_groundwater_signatures."""
    if not PASTAS_AVAILABLE:
        context.log.error("pastas not installed — aborting")
        return

    context.log.info(
        "Identifying eligible stations (AIDA: >= %dy span + quality filter)...",
        MIN_YEARS,
    )
    all_ids = _get_eligible_station_ids(pg)
    context.log.info("SQL pre-filter: %d stations with >= %dy span", len(all_ids), MIN_YEARS)

    if not all_ids:
        context.log.warning("No eligible stations found.")
        return

    # Schema migration: DROP + recreate (incompatible schema change)
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS ml")
        cur.execute("DROP TABLE IF EXISTS ml.pastas_groundwater_signatures")
        cur.execute(_CREATE_TABLE)
        conn.commit()

    n_batches = (len(all_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    total_success = 0
    total_fail = 0
    total_eligible = 0
    total_persisted = 0

    for batch_idx in range(n_batches):
        batch_ids = all_ids[batch_idx * BATCH_SIZE : (batch_idx + 1) * BATCH_SIZE]
        stations = _load_gwl_batch(pg, batch_ids)
        n_eligible = len(stations)
        total_eligible += n_eligible

        if n_eligible == 0:
            context.log.info(
                "Batch %d/%d: 0 eligible stations, skipping",
                batch_idx + 1,
                n_batches,
            )
            continue

        results = Parallel(n_jobs=N_JOBS, backend="threading")(
            delayed(compute_signatures)(code_bss, gwl)
            for code_bss, gwl in stations.items()
        )

        n_ok = sum(1 for r in results if r.get("success"))
        total_success += n_ok
        total_fail += len(results) - n_ok

        n_persisted = _persist_results(pg, results)
        total_persisted += n_persisted

        # Log first failures for debugging
        for r in results:
            if not r.get("success"):
                context.log.warning(
                    "  FAIL %s: %s", r["code_bss"], r.get("error", "unknown")
                )

        context.log.info(
            "Batch %d/%d: %d/%d success, %d persisted",
            batch_idx + 1,
            n_batches,
            n_ok,
            n_eligible,
            n_persisted,
        )
        del stations, results

    n_rejected = len(all_ids) - total_eligible
    context.log.info(
        "Done: %d/%d success (%d rejected by quality filter, %d compute failures, %d persisted)",
        total_success,
        total_eligible,
        n_rejected,
        total_fail,
        total_persisted,
    )
    context.add_output_metadata(
        {
            "n_stations_sql_prefilter": MetadataValue.int(len(all_ids)),
            "n_stations_quality_filter": MetadataValue.int(total_eligible),
            "n_success": MetadataValue.int(total_success),
            "n_failure": MetadataValue.int(total_fail),
            "n_persisted": MetadataValue.int(total_persisted),
        }
    )
```

- [ ] **Step 2: Verify syntax**

Run: `python -c "import ast; ast.parse(open('src/hubeau_pipeline/assets/pastas_signatures_asset.py').read()); print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/hubeau_pipeline/assets/pastas_signatures_asset.py
git commit -m "refactor(pastas): rewrite signatures asset — remove Dutch stats, inline logic, add inf guard"
```

---

### Task 2: Delete the wrapper module

**Files:**
- Delete: `src/hubeau_pipeline/ml/pastas_signatures.py`

- [ ] **Step 1: Delete the wrapper**

```bash
rm src/hubeau_pipeline/ml/pastas_signatures.py
```

- [ ] **Step 2: Verify no remaining imports**

Run: `grep -r "pastas_signatures" src/hubeau_pipeline/ --include="*.py"`

Expected output — only references in the asset itself (no more `from ..ml.pastas_signatures`):
```
src/hubeau_pipeline/assets/pastas_signatures_asset.py:...  (only the asset file, no ml/ import)
src/hubeau_pipeline/jobs/ml_jobs.py:...  (job name string, not an import)
src/hubeau_pipeline/jobs/__init__.py:...  (job name string, not an import)
src/hubeau_pipeline/assets/__init__.py:...  (import of the asset, not the wrapper)
```

- [ ] **Step 3: Commit**

```bash
git add -u src/hubeau_pipeline/ml/pastas_signatures.py
git commit -m "refactor(pastas): delete pastas_signatures.py wrapper (logic inlined into asset)"
```

---

### Task 3: Update tests

**Files:**
- Rewrite: `tests/test_pastas_signatures.py`

The existing tests import from the deleted `ml.pastas_signatures` module. Rewrite to test the new `compute_signatures` function from the asset module.

- [ ] **Step 1: Rewrite `tests/test_pastas_signatures.py`**

```python
"""Tests for Pastas groundwater signatures computation."""

import numpy as np
import pandas as pd
import pytest

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

    def test_31_signatures_present(self):
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
```

- [ ] **Step 2: Run tests**

Run: `cd /home/ringuet/hubeau_data_integration && python -m pytest tests/test_pastas_signatures.py -v`

Expected: All tests pass (assuming Pastas is importable in test env). If Pastas is not available locally, the tests will fail with import error — that's expected since they run inside the Docker worker.

- [ ] **Step 3: Commit**

```bash
git add tests/test_pastas_signatures.py
git commit -m "test(pastas): update signature tests — new interface, no Dutch stats, inf guard"
```

---

### Task 4: Clean up `__init__.py` import

**Files:**
- Verify: `src/hubeau_pipeline/assets/__init__.py`

- [ ] **Step 1: Verify the import is already correct**

The current import at line 39 is:
```python
from .pastas_signatures_asset import ml_piezo_groundwater_signatures
```

This imports from the asset file (not the deleted wrapper), so **no change is needed**. The old import from `ml.pastas_signatures` was only in the asset file itself, which was already rewritten in Task 1.

- [ ] **Step 2: Verify Dagster definitions load**

Run: `cd /home/ringuet/hubeau_data_integration && python -c "from hubeau_pipeline.assets import all_assets; print(f'{len(all_assets)} assets loaded OK')"`

Expected: `30 assets loaded OK` (or similar count, no ImportError)

- [ ] **Step 3: Final commit (if any fixup needed)**

Only if step 2 reveals an issue. Otherwise skip.
