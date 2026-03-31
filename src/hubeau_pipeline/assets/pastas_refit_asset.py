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
    "code_bss",
    "date",
    "simulated",
    "residuals",
    "recharge_contribution",
    "wb_recharge",
    "wb_actual_evaporation",
    "wb_surface_runoff",
    "wb_effective_precip",
    "wb_root_zone_storage",
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

    df = pd.DataFrame(
        rows,
        columns=["code_bss", "date", "niveau_nappe_eau", "total_precipitation", "potential_evaporation"],
    )
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
    rename_map = {}
    for col in ts.columns:
        if not col.startswith("wb_"):
            continue
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

    for expected in [
        "wb_recharge",
        "wb_actual_evaporation",
        "wb_surface_runoff",
        "wb_effective_precip",
        "wb_root_zone_storage",
    ]:
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
            for col in _TS_COLS[2:]:
                v = row.get(col)
                vals.append("\\N" if v is None or (isinstance(v, float) and np.isnan(v)) else str(v))
            buf.write("\t".join(vals) + "\n")
            total_rows += 1

    if total_rows == 0:
        return 0

    buf.seek(0)
    with pg.get_connection() as conn:
        cur = conn.cursor()
        station_ids = [r["code_bss"] for r in results if r["fit_success"]]
        if station_ids:
            placeholders = ",".join(["%s"] * len(station_ids))
            cur.execute(
                f"DELETE FROM ml.pastas_model_timeseries WHERE code_bss IN ({placeholders})",
                station_ids,
            )
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

        n_updated = _update_irf_scalars(pg, results)
        total_irf_updated += n_updated

        n_rows = _copy_timeseries_batch(pg, results)
        total_ts_rows += n_rows

        context.log.info(
            f"Batch {batch_idx + 1}/{n_batches}: "
            f"{n_ok}/{len(results)} fit success, "
            f"{n_updated} irf updated, {n_rows} ts rows"
        )

        for r in results:
            if not r["fit_success"]:
                context.log.warning(f"  FAIL {r['code_bss']}: {r.get('error', 'unknown')}")

        del stations, results

    context.log.info(
        f"Done: {total_success} success, {total_fail} fail, "
        f"{total_irf_updated} irf updated, {total_ts_rows} ts rows"
    )
    context.add_output_metadata(
        {
            "n_stations": MetadataValue.int(len(all_ids)),
            "n_fit_success": MetadataValue.int(total_success),
            "n_fit_failure": MetadataValue.int(total_fail),
            "n_irf_updated": MetadataValue.int(total_irf_updated),
            "n_timeseries_rows": MetadataValue.int(total_ts_rows),
        }
    )
