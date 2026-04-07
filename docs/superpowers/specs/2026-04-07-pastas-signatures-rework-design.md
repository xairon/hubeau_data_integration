# Pastas Groundwater Signatures — Rework Design

**Date:** 2026-04-07
**Scope:** `ml_piezo_groundwater_signatures` asset only
**Goal:** Clean rework for AIDA benchmark paper — correct Pastas usage, remove Dutch stats, align schema on Pastas `__all__`

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Scope | Signatures asset only | Other Pastas assets (IRF, SGI, refit) unchanged |
| Eligibility filter | Same AIDA cohorte (GWL + precip + evap + >= 10y + quality) | Benchmark consistency — same stations across all Pastas assets |
| Signature parameters | `ps.stats.signatures.summary()` defaults | Reproducible, citable in paper |
| Dutch stats (GHG/GLG/GVG) | Removed | Not in AIDA benchmark, Netherlands-specific |
| GWL preprocessing | Raw (no interpolation) | Honest NaN for signatures requiring daily data |

## Schema: `ml.pastas_groundwater_signatures`

DROP + recreate (incompatible schema change, manual asset, data is recalculable).

```sql
CREATE TABLE IF NOT EXISTS ml.pastas_groundwater_signatures (
    code_bss                 TEXT PRIMARY KEY,
    -- 31 Pastas signatures (ps.stats.signatures.__all__)
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
    -- Metadata
    series_start             DATE,
    series_end               DATE,
    series_length_days       INTEGER,
    n_valid_days             INTEGER,
    n_signatures_computed    INTEGER,
    pastas_version           TEXT,
    computed_at              TIMESTAMP DEFAULT NOW()
)
```

Changes vs current:
- **Removed:** 7 Dutch stats columns (gg, ghg, glg, gvg, q_ghg, q_glg, q_gvg)
- **Added:** `pastas_version` (traceability for paper)

## File Changes

### Deleted
- `src/hubeau_pipeline/ml/pastas_signatures.py` — wrapper no longer needed, `summary()` called directly in asset

### Modified
- `src/hubeau_pipeline/assets/pastas_signatures_asset.py` — full rework:
  - Inline signature computation (no wrapper)
  - Remove Dutch stats
  - Add `pastas_version` to output
  - Add inf guard (`np.isinf` → `None`)
  - DROP + recreate table on first run
  - Align signature column list on `ps.stats.signatures.__all__` (31 signatures)
- `src/hubeau_pipeline/assets/__init__.py` — remove import of `pastas_signatures` from `ml/` (if any)

### Unchanged
- `src/hubeau_pipeline/jobs/ml_jobs.py` — `pastas_signatures_job` unchanged
- `src/hubeau_pipeline/jobs/__init__.py` — unchanged
- All other Pastas assets (IRF, SGI, refit) — unchanged

## Asset Logic

```
ml_piezo_groundwater_signatures:
  group: ml_piezo
  deps: [hubeau_daily_chroniques]
  
  1. SQL pre-filter (same AIDA query):
     SELECT code_bss FROM gold.hubeau_daily_chroniques
     GROUP BY code_bss
     HAVING MAX(date) - MIN(date) >= 3650
        AND COUNT(niveau_nappe_eau) > 0
        AND COUNT(total_precipitation) > 0
        AND COUNT(potential_evaporation) > 0

  2. DROP + CREATE table (schema migration)

  3. Batch loop (BATCH_SIZE=500):
     a. Load raw GWL for batch
     b. AIDA quality filter (_check_station_quality from pastas_assets)
     c. Parallel(n_jobs=8, backend="threading"):
        - sigs_df = ps.stats.signatures.summary(gwl)
        - Sanitize: np.inf/-np.inf → None
        - Count n_signatures_computed (non-NaN values)
        - Build result dict with metadata
     d. UPSERT batch → ml.pastas_groundwater_signatures

  4. Dagster metadata:
     - n_stations_sql_prefilter
     - n_stations_quality_filter
     - n_success
     - n_failure
     - n_persisted
```

## Post-hoc Guards

- `np.isinf(v)` → `None` (protects against `magnitude = (max-min)/min` when min ≈ 0)
- Log warning for stations with > 50% signatures NaN
- Log first 10 failure errors for debugging

## Constants

```python
BATCH_SIZE = 500     # stations per batch
N_JOBS = 8           # parallel workers (threading backend)
MIN_YEARS = 10       # from pastas_assets (AIDA criteria)
```
