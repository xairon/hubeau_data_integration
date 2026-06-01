"""Nightly per-station standardized-index (IPS/SSFI) classification → gold.station_current_index."""
import logging

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from ..ml.indices import classify_latest_spli, classify_latest_ssfi
from ..ml.current_index_persistence import init_current_index_table, upsert_current_index
from ..resources import PostgreSQLResource

logger = logging.getLogger(__name__)

# (domain, table, code_col, value_col, index_name, classify_fn)
_DOMAINS = [
    ("piezo", "gold.fct_monthly_chroniques", "code_bss", "niveau_moyen", "IPS", classify_latest_spli),
    ("hydro", "gold.fct_monthly_hydro", "code_station", "resultat_moyen", "SSFI", classify_latest_ssfi),
]


@asset(
    name="station_current_index",
    group_name="indices",
    description="Latest standardized index (IPS/SSFI) + 7-class per station, written to gold.station_current_index.",
)
def station_current_index(context: AssetExecutionContext, pg: PostgreSQLResource):
    init_current_index_table(pg)
    total = 0
    for domain, table, code_col, value_col, index_name, classify_fn in _DOMAINS:
        with pg.get_connection() as conn:
            df = pd.read_sql(
                # Guard against Hub'Eau positive sentinels (~1e9 l/s) that would
                # corrupt the SSFI gamma fit; harmless for piezo m NGF levels.
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
            z, cls = classify_fn(months, values)
            ref_month = pd.to_datetime(months[-1]).date()
            rows.append((code, domain, index_name, z, cls,
                         ref_month, pd.to_datetime(months[0]).date(), ref_month))
        upsert_current_index(pg, rows)
        total += len(rows)
        context.log.info("%s: classified %d stations", domain, len(rows))
    context.add_output_metadata({"stations_classified": MetadataValue.int(total)})
    return total
