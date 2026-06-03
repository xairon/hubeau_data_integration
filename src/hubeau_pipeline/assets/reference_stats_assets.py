"""Per-station per-month fixed reference grid → gold.station_reference_stats."""
import logging

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from ..ml.indices import compute_reference_grid
from ..ml.reference_stats_persistence import init_reference_stats_table, upsert_reference_stats
from ..resources import PostgreSQLResource

logger = logging.getLogger(__name__)

# (domain, table, code_col, value_col, positive_only)
_DOMAINS = [
    ("piezo", "gold.fct_monthly_chroniques", "code_bss", "niveau_moyen", False),
    ("hydro", "gold.fct_monthly_hydro", "code_station", "resultat_moyen", True),
]


@asset(
    name="station_reference_stats",
    group_name="indices",
    description="Fixed-reference (1991-2020 + fallback) per-month percentile grid per station.",
)
def station_reference_stats(context: AssetExecutionContext, pg: PostgreSQLResource):
    init_reference_stats_table(pg)
    total = 0
    for domain, table, code_col, value_col, positive_only in _DOMAINS:
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
            for m in range(1, 13):
                rows.append((domain, code, m, res["grid"].get(m),
                             res["baseline_start"], res["baseline_end"],
                             res["flag"], res["n_years"]))
        upsert_reference_stats(pg, rows)
        total += len(rows)
        context.log.info("%s: reference grid for %d station-months", domain, len(rows))
    context.add_output_metadata({"rows_written": MetadataValue.int(total)})
    return total
