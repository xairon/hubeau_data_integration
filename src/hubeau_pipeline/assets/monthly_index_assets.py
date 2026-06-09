"""Per-station MONTHLY standardized index re-scored against the fixed reference grid → gold.fct_monthly_index."""
import logging

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from ..ml.indices import compute_reference_grid, grid_to_zscore, classify_value
from ..ml.monthly_index_persistence import init_monthly_index_table, upsert_monthly_index
from ..resources import PostgreSQLResource

logger = logging.getLogger(__name__)

_DOMAINS = [
    ("piezo", "gold.fct_monthly_chroniques", "code_bss", "niveau_moyen", False),
    ("hydro", "gold.fct_monthly_hydro", "code_station", "resultat_moyen", True),
]


def rescore_series(domain, code, months, values, ref):
    """Re-score every month against the station's fixed reference grid.

    Returns list of (code, type, month_date, z|None, index_class, flag).
    """
    grid = ref["grid"]
    flag = ref["flag"]
    out = []
    for m_iso, val in zip(months, values):
        dt = pd.to_datetime(m_iso)
        z = grid_to_zscore(float(val), grid.get(dt.month)) if val is not None else None
        cls = classify_value(z) if z is not None else "UNKNOWN"
        out.append((code, domain, dt.date(), z, cls, flag))
    return out


@asset(
    name="fct_monthly_index",
    group_name="indices",
    deps=["station_reference_stats"],
    description="Monthly standardized index (IPS/SSFI) re-scored against the fixed reference grid, full history.",
)
def fct_monthly_index(context: AssetExecutionContext, pg: PostgreSQLResource):
    init_monthly_index_table(pg)
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
            ref = compute_reference_grid(months, values, positive_only=positive_only)
            rows.extend(rescore_series(domain, code, months, values, ref))
        upsert_monthly_index(pg, rows)
        total += len(rows)
        context.log.info("%s: re-scored %d station-months (fixed ref)", domain, len(rows))
    context.add_output_metadata({"station_months": MetadataValue.int(total)})
    return total
