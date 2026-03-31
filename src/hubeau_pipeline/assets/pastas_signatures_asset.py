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
            delayed(compute_signatures_single)(code_bss, gwl) for code_bss, gwl in stations.items()
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

    context.log.info(f"Done: {total_success} success, {total_fail} failures, {total_persisted} persisted")
    context.add_output_metadata(
        {
            "n_eligible": MetadataValue.int(len(all_ids)),
            "n_success": MetadataValue.int(total_success),
            "n_failure": MetadataValue.int(total_fail),
            "n_persisted": MetadataValue.int(total_persisted),
        }
    )
