"""Dagster Asset — Pastas SGI (Standardized Groundwater Index).

Computes monthly SGI for all eligible piezometric stations (>= 5 years)
from gold.hubeau_daily_chroniques. No Pastas model fitting required.

SGI is a monthly time series (not daily) — normalized to N(0,1).
"""

import logging

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
    """Compute SGI for one station."""
    try:
        # ps.stats.sgi returns daily values; resample to monthly mean
        sgi_daily = ps.stats.sgi(gwl, timescale_months=1)
        sgi = sgi_daily.resample("ME").mean().dropna()
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
            delayed(_compute_sgi_single)(code_bss, gwl) for code_bss, gwl in stations.items()
        )

        n_ok = sum(1 for r in results if r.get("success"))
        total_success += n_ok
        total_fail += len(results) - n_ok

        n_rows = _persist_sgi_batch(pg, results)
        total_rows += n_rows

        context.log.info(
            f"Batch {batch_idx + 1}/{n_batches}: " f"{n_ok}/{len(results)} success, {n_rows} rows inserted"
        )
        del stations, results

    context.log.info(f"Done: {total_success} stations, {total_rows} total SGI rows")
    context.add_output_metadata(
        {
            "n_eligible": MetadataValue.int(len(all_ids)),
            "n_success": MetadataValue.int(total_success),
            "n_failure": MetadataValue.int(total_fail),
            "n_sgi_rows": MetadataValue.int(total_rows),
        }
    )
