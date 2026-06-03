"""Nightly per-station standardized-index classification → gold.station_current_index (fixed reference)."""
import logging

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from ..ml.indices import compute_reference_grid, grid_to_zscore, classify_value
from ..ml.current_index_persistence import init_current_index_table, upsert_current_index
from ..resources import PostgreSQLResource

logger = logging.getLogger(__name__)

_DOMAINS = [
    ("piezo", "gold.fct_monthly_chroniques", "code_bss", "niveau_moyen", "IPS", False),
    ("hydro", "gold.fct_monthly_hydro", "code_station", "resultat_moyen", "SSFI", True),
]


@asset(
    name="station_current_index",
    group_name="indices",
    deps=["station_reference_stats"],
    description="Latest standardized index (IPS/SSFI) classified against the fixed reference grid.",
)
def station_current_index(context: AssetExecutionContext, pg: PostgreSQLResource):
    init_current_index_table(pg)
    total = 0
    for domain, table, code_col, value_col, index_name, positive_only in _DOMAINS:
        with pg.get_connection() as conn:
            df = pd.read_sql(
                f"SELECT {code_col} AS code, mois, {value_col} AS val "
                f"FROM {table} WHERE {value_col} IS NOT NULL "
                f"AND {value_col} < 1e8 AND {value_col} > -1e4 "
                f"ORDER BY {code_col}, mois",
                conn,
            )
        rows = []
        for code, g in df.groupby("code"):
            months = g["mois"].astype(str).tolist()
            values = g["val"].astype(float).tolist()
            res = compute_reference_grid(months, values, positive_only=positive_only)
            last_dt = pd.to_datetime(months[-1])
            last_val = float(values[-1])
            z = grid_to_zscore(last_val, res["grid"].get(last_dt.month))
            cls = classify_value(z) if z is not None else "UNKNOWN"
            rows.append((code, domain, index_name, z, cls,
                         last_dt.date(), res["baseline_start"], res["baseline_end"]))
        upsert_current_index(pg, rows)
        total += len(rows)
        context.log.info("%s: classified %d stations (fixed ref)", domain, len(rows))
    context.add_output_metadata({"stations_classified": MetadataValue.int(total)})
    return total
