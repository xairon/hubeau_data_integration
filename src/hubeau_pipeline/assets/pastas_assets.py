"""Dagster Asset — Pastas IRF Features for piezometric stations.

Fits a Pastas TFN model (Gamma IRF + FlexModel recharge) on each eligible
station from gold.hubeau_daily_chroniques, extracts IRF parameters and
fit quality metrics, and persists to ml.pastas_irf_features via UPSERT.

Memory-efficient: loads data in batches to stay within the 4GB worker limit.
"""

import logging

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset
from joblib import Parallel, delayed

from ..ml.pastas_wrapper import fit_single_station
from ..resources import PostgreSQLResource

logger = logging.getLogger(__name__)

# Filtering criteria (aligned with aida_embedding_benchmark)
MIN_YEARS = 10  # minimum temporal span
WINDOW_SIZE = 365  # sliding window for quality check
NAN_THRESHOLD = 0.10  # max 10% NaN per window after interpolation
INTERP_LIMIT = 60  # max gap to interpolate (days)
BATCH_SIZE = 200  # stations per batch (limits peak memory)
N_JOBS = 8  # parallel workers (loky forks — must fit in 4GB total)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ml.pastas_irf_features (
    code_bss            TEXT PRIMARY KEY,
    -- IRF parameters
    recharge_a          DOUBLE PRECISION,
    recharge_n          DOUBLE PRECISION,
    recharge_scale      DOUBLE PRECISION,
    recharge_f          DOUBLE PRECISION,
    -- Derived features
    tmax_days           DOUBLE PRECISION,
    cutoff_95_days      DOUBLE PRECISION,
    gain                DOUBLE PRECISION,
    mean_response_time  DOUBLE PRECISION,
    -- Fit quality
    evp                 DOUBLE PRECISION,
    rmse                DOUBLE PRECISION,
    nash                DOUBLE PRECISION,
    r2                  DOUBLE PRECISION,
    n_observations      INTEGER,
    fit_success         BOOLEAN,
    -- Metadata
    series_start        DATE,
    series_end          DATE,
    series_length_days  INTEGER,
    nan_fraction        DOUBLE PRECISION,
    -- Audit
    fitted_at           TIMESTAMP DEFAULT NOW(),
    pastas_version      TEXT
)
"""

_UPSERT = """
INSERT INTO ml.pastas_irf_features (
    code_bss,
    recharge_a, recharge_n, recharge_scale, recharge_f,
    tmax_days, cutoff_95_days, gain, mean_response_time,
    evp, rmse, nash, r2, n_observations, fit_success,
    series_start, series_end, series_length_days, nan_fraction,
    fitted_at, pastas_version
) VALUES (
    %(code_bss)s,
    %(recharge_a)s, %(recharge_n)s, %(recharge_scale)s, %(recharge_f)s,
    %(tmax_days)s, %(cutoff_95_days)s, %(gain)s, %(mean_response_time)s,
    %(evp)s, %(rmse)s, %(nash)s, %(r2)s, %(n_observations)s, %(fit_success)s,
    %(series_start)s, %(series_end)s, %(series_length_days)s, %(nan_fraction)s,
    NOW(), %(pastas_version)s
)
ON CONFLICT (code_bss) DO UPDATE SET
    recharge_a = EXCLUDED.recharge_a,
    recharge_n = EXCLUDED.recharge_n,
    recharge_scale = EXCLUDED.recharge_scale,
    recharge_f = EXCLUDED.recharge_f,
    tmax_days = EXCLUDED.tmax_days,
    cutoff_95_days = EXCLUDED.cutoff_95_days,
    gain = EXCLUDED.gain,
    mean_response_time = EXCLUDED.mean_response_time,
    evp = EXCLUDED.evp,
    rmse = EXCLUDED.rmse,
    nash = EXCLUDED.nash,
    r2 = EXCLUDED.r2,
    n_observations = EXCLUDED.n_observations,
    fit_success = EXCLUDED.fit_success,
    series_start = EXCLUDED.series_start,
    series_end = EXCLUDED.series_end,
    series_length_days = EXCLUDED.series_length_days,
    nan_fraction = EXCLUDED.nan_fraction,
    fitted_at = NOW(),
    pastas_version = EXCLUDED.pastas_version
"""


def _check_station_quality(gwl: pd.Series) -> bool:
    """Return True if station has at least one good 365-day window.

    Applies aida_benchmark quality filter:
    1. Resample to daily, interpolate gaps up to INTERP_LIMIT days
    2. Trim to first/last valid observation, re-interpolate
    3. Require at least one WINDOW_SIZE window with <= NAN_THRESHOLD NaN
    """
    ts = gwl.resample("D").mean()
    ts = ts.interpolate(method="linear", limit=INTERP_LIMIT, limit_direction="both")

    if ts.isna().all():
        return False

    valid = ts.notna()
    ts = ts.loc[valid.idxmax():valid[::-1].idxmax()]
    ts = ts.interpolate(method="linear", limit=INTERP_LIMIT, limit_direction="both")

    if len(ts) < WINDOW_SIZE:
        return False

    vals = ts.values
    for start in range(0, len(vals) - WINDOW_SIZE + 1, WINDOW_SIZE):
        window = vals[start:start + WINDOW_SIZE]
        if np.isnan(window).mean() <= NAN_THRESHOLD:
            return True
    return False


def _get_eligible_station_ids(pg: PostgreSQLResource) -> list[str]:
    """Get list of station IDs passing the SQL pre-filter (>= 10y span, all vars)."""
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


def _load_station_batch(pg: PostgreSQLResource, station_ids: list[str]) -> dict[str, dict[str, pd.Series]]:
    """Load data for a batch of stations and apply quality filter.

    Returns only stations passing the aida_benchmark quality check.
    """
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

    df = pd.DataFrame(rows, columns=["code_bss", "date", "niveau_nappe_eau", "total_precipitation", "potential_evaporation"])
    df["date"] = pd.to_datetime(df["date"])
    # Convert None → NaN for numeric cols (cursor.fetchall returns None, not NaN)
    for col in ["niveau_nappe_eau", "total_precipitation", "potential_evaporation"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["potential_evaporation"] = df["potential_evaporation"].abs()

    stations = {}
    for code_bss, group in df.groupby("code_bss"):
        group = group.set_index("date").sort_index()
        gwl = group["niveau_nappe_eau"]

        if not _check_station_quality(gwl):
            continue

        stations[code_bss] = {
            "gwl": gwl,
            "precip": group["total_precipitation"],
            "evap": group["potential_evaporation"],
        }

    return stations


def _persist_results(pg: PostgreSQLResource, results: list[dict]) -> int:
    """UPSERT results into ml.pastas_irf_features. Returns count persisted."""
    with pg.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS ml")
        cur.execute(_CREATE_TABLE)

        count = 0
        for row in results:
            params = {k: v for k, v in row.items() if k != "error"}
            cur.execute(_UPSERT, params)
            count += 1

        conn.commit()
    return count


@asset(
    group_name="ml_piezo",
    deps=["hubeau_daily_chroniques"],
    description="Fit Pastas TFN models on all eligible piezometric stations and extract IRF features",
)
def ml_piezo_pastas_irf_features(
    context: AssetExecutionContext,
    pg: PostgreSQLResource,
):
    """Fit Pastas models in batches and persist IRF features to ml.pastas_irf_features."""
    # Stage 1: get eligible station IDs (lightweight SQL query)
    context.log.info("Identifying eligible stations (SQL pre-filter: >= 10y span)...")
    all_ids = _get_eligible_station_ids(pg)
    context.log.info(f"SQL pre-filter: {len(all_ids)} stations with >= {MIN_YEARS}y span")

    if not all_ids:
        context.log.warning("No eligible stations found. Skipping.")
        return

    # Ensure table exists before batched inserts
    _persist_results(pg, [])

    # Stage 2: process in batches to limit memory
    n_batches = (len(all_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    context.log.info(f"Processing {len(all_ids)} stations in {n_batches} batches of {BATCH_SIZE} (n_jobs={N_JOBS})")

    total_success = 0
    total_fail = 0
    total_eligible = 0
    total_persisted = 0
    all_failures = []

    for batch_idx in range(n_batches):
        batch_ids = all_ids[batch_idx * BATCH_SIZE:(batch_idx + 1) * BATCH_SIZE]

        # Load and quality-filter this batch
        stations = _load_station_batch(pg, batch_ids)
        n_eligible = len(stations)
        total_eligible += n_eligible

        if n_eligible == 0:
            context.log.info(f"Batch {batch_idx + 1}/{n_batches}: 0 eligible stations, skipping")
            continue

        # Parallel fit within this batch
        results = Parallel(n_jobs=N_JOBS, backend="threading")(
            delayed(fit_single_station)(code_bss, data["gwl"], data["precip"], data["evap"])
            for code_bss, data in stations.items()
        )

        n_ok = sum(1 for r in results if r.get("fit_success"))
        n_fail = n_eligible - n_ok
        total_success += n_ok
        total_fail += n_fail

        # Persist this batch immediately (don't accumulate in memory)
        n_persisted = _persist_results(pg, results)
        total_persisted += n_persisted

        # Collect failures for logging
        batch_failures = [r for r in results if not r.get("fit_success")]
        all_failures.extend(batch_failures[:3])

        context.log.info(
            f"Batch {batch_idx + 1}/{n_batches}: "
            f"{n_ok}/{n_eligible} success, {n_persisted} persisted"
        )

        # Free memory
        del stations, results

    # Summary
    n_rejected = len(all_ids) - total_eligible
    context.log.info(
        f"Done: {total_success}/{total_eligible} fit success "
        f"({n_rejected} rejected by quality filter, {total_fail} fit failures)"
    )

    for f in all_failures[:10]:
        context.log.warning(f"  FAIL {f['code_bss']}: {f.get('error', 'unknown')}")

    context.log.info(f"Total persisted: {total_persisted} rows in ml.pastas_irf_features")

    context.add_output_metadata({
        "n_stations_sql_prefilter": MetadataValue.int(len(all_ids)),
        "n_stations_quality_filter": MetadataValue.int(total_eligible),
        "n_fit_success": MetadataValue.int(total_success),
        "n_fit_failure": MetadataValue.int(total_fail),
        "success_rate_pct": MetadataValue.float(total_success / total_eligible * 100 if total_eligible > 0 else 0),
        "n_persisted": MetadataValue.int(total_persisted),
    })
