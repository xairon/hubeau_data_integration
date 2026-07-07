"""SPI/STI par cellule de grille ERA5 → gold.fct_era5_indices_grid.

Nightly (job station_index_refresh, sensor post-transform) : recalcule les 3 derniers mois.
Bootstrap : table vide → historique complet 1950→présent par tranches de 5 ans.
Les paramètres de référence (gamma/μ/σ 1991-2020) viennent de gold.fct_era5_climatology_grid.
"""
import logging

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_dbt import get_asset_key_for_model

from ..ml.era5_indices import MIN_YEARS_REF, compute_spi, compute_sti
from ..ml.era5_indices_persistence import (
    init_era5_indices_table,
    latest_index_month,
    upsert_era5_indices,
)
from ..resources import PostgreSQLResource
from .dbt_assets import hubeau_dbt_assets

logger = logging.getLogger(__name__)

WINDOWS = [1, 3, 6, 12]
NIGHTLY_MONTHS = 3        # fenêtre de recalcul quotidienne
BOOTSTRAP_CHUNK_YEARS = 5

# Cumuls/moyennes glissants par cellule, joints aux normales, pour une fenêtre donnée.
# Le warmup de 11 mois avant start_month garantit des fenêtres 12 mois complètes.
# NB : le garde n_mois = fenêtre ne protège que contre le ramp-up de début de série ; la
# contiguïté calendaire des mois est une propriété du mart amont (grille ERA5 continue,
# 0 trou vérifié 1990→présent) — surveillée par data_completeness_job (entrée
# "gold ERA5 grille"), pas re-vérifiée ici.
_QUERY = """
WITH rolled AS (
    SELECT
        era5_latitude, era5_longitude, mois,
        SUM(precipitation_totale) OVER w AS precip_cumul,
        AVG(temperature_moyenne)  OVER w AS temp_fenetre,
        COUNT(*)                  OVER w AS n_mois
    FROM gold.fct_era5_monthly_grid
    WHERE mois_complet
      AND mois >= %(warmup_month)s
      AND mois <  %(end_month)s
    WINDOW w AS (
        PARTITION BY era5_latitude, era5_longitude ORDER BY mois
        ROWS BETWEEN %(window_minus_1)s PRECEDING AND CURRENT ROW
    )
)
SELECT
    r.era5_latitude, r.era5_longitude, r.mois,
    r.precip_cumul, r.temp_fenetre,
    c.gamma_alpha, c.gamma_beta, c.prob_zero,
    c.temp_moyenne, c.temp_stddev, c.nb_annees
FROM rolled r
JOIN gold.fct_era5_climatology_grid c
  ON c.era5_latitude = r.era5_latitude
 AND c.era5_longitude = r.era5_longitude
 AND c.mois_calendaire = EXTRACT(MONTH FROM r.mois)::int
 AND c.fenetre = %(window)s
WHERE r.mois >= %(start_month)s
  AND r.n_mois = %(window)s
"""


def _compute_range(pg, start_month, end_month):
    """Calcule et upserte SPI/STI pour [start_month, end_month), toutes fenêtres."""
    total = 0
    for window in WINDOWS:
        warmup = start_month - pd.DateOffset(months=window - 1)
        with pg.get_connection() as conn:
            df = pd.read_sql(
                _QUERY,
                conn,
                params={
                    "warmup_month": warmup.date(),
                    "start_month": start_month.date(),
                    "end_month": end_month.date(),
                    "window": window,
                    "window_minus_1": window - 1,
                },
            )
        if df.empty:
            continue
        spi = compute_spi(df["precip_cumul"], df["gamma_alpha"], df["gamma_beta"], df["prob_zero"])
        sti = compute_sti(df["temp_fenetre"], df["temp_moyenne"], df["temp_stddev"])
        # Seuil WMO : référence trop courte → indices NULL
        thin = df["nb_annees"].to_numpy() < MIN_YEARS_REF
        spi[thin] = np.nan
        sti[thin] = np.nan
        rows = [
            (lat, lon, mois, window,
             None if np.isnan(s) else float(s),
             None if np.isnan(t) else float(t))
            for lat, lon, mois, s, t in zip(
                df["era5_latitude"], df["era5_longitude"], df["mois"], spi, sti, strict=True
            )
        ]
        upsert_era5_indices(pg, rows)
        total += len(rows)
    return total


@asset(
    name="fct_era5_indices_grid",
    group_name="indices",
    deps=[
        get_asset_key_for_model([hubeau_dbt_assets], "fct_era5_monthly_grid"),
        get_asset_key_for_model([hubeau_dbt_assets], "fct_era5_climatology_grid"),
    ],
    description=(
        "SPI/STI par cellule ERA5 (fenêtres 1/3/6/12 mois, normale 1991-2020). "
        "Nightly: 3 derniers mois. Table vide: bootstrap 1950→présent."
    ),
)
def fct_era5_indices_grid(context: AssetExecutionContext, pg: PostgreSQLResource):
    init_era5_indices_table(pg)

    now_month = pd.Timestamp.today().normalize().replace(day=1)
    last = latest_index_month(pg)

    if last is None:
        context.log.info("Table vide → bootstrap historique complet 1950→présent")
        total = 0
        start = pd.Timestamp("1950-01-01")
        while start < now_month:
            end = min(start + pd.DateOffset(years=BOOTSTRAP_CHUNK_YEARS), now_month)
            n = _compute_range(pg, start, end)
            total += n
            context.log.info("Chunk %s → %s : %d lignes", start.date(), end.date(), n)
            start = end
    else:
        start = now_month - pd.DateOffset(months=NIGHTLY_MONTHS)
        total = _compute_range(pg, start, now_month)
        context.log.info("Recalcul %s → %s : %d lignes", start.date(), now_month.date(), total)

    context.add_output_metadata({"upserted_rows": MetadataValue.int(total)})
    return total
