"""Référence SPEI 1991-2020 → gold.fct_era5_spei_climatology_grid.

Fit log-logistique (L-moments) du cumul bilan hydrique par cellule × mois
calendaire × fenêtre. Rebuild rare (full), consommé par fct_era5_indices_grid.
"""
import logging

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_dbt import get_asset_key_for_model

from ..ml.era5_indices import MIN_YEARS_REF, fit_loglogistic_lmoments
from ..ml.era5_spei_climatology_persistence import (
    init_spei_climatology_table,
    upsert_spei_climatology,
)
from ..resources import PostgreSQLResource
from .dbt_assets import hubeau_dbt_assets

logger = logging.getLogger(__name__)

WINDOWS = [1, 3, 6, 12]

# Cumul glissant du bilan hydrique sur 1991-2020 (warmup 11 mois depuis 1990),
# mois precip-complets uniquement (l'ETP suit la précip, pas la température).
_REF_QUERY = """
WITH rolled AS (
    SELECT
        era5_latitude, era5_longitude, mois,
        SUM(bilan_hydrique) OVER w AS bilan_cumul,
        COUNT(*)            OVER w AS n_mois
    FROM gold.fct_era5_monthly_grid
    WHERE mois_complet
      AND mois >= DATE '1990-01-01'
      AND mois <  DATE '2021-01-01'
    WINDOW w AS (
        PARTITION BY era5_latitude, era5_longitude ORDER BY mois
        ROWS BETWEEN %(window_minus_1)s PRECEDING AND CURRENT ROW
    )
)
SELECT
    era5_latitude, era5_longitude,
    EXTRACT(MONTH FROM mois)::int AS mois_calendaire,
    bilan_cumul
FROM rolled
WHERE mois >= DATE '1991-01-01'
  AND n_mois = %(window)s
"""


def fit_reference_frame(df, window):
    """Groupe df par (cellule, mois calendaire) et fitte la log-logistique.

    Retourne une liste de tuples upsertables ; les groupes trop courts
    (< MIN_YEARS_REF) ou à fit dégénéré sont ignorés.
    """
    rows = []
    for (lat, lon, mc), grp in df.groupby(
        ["era5_latitude", "era5_longitude", "mois_calendaire"], sort=False
    ):
        samples = grp["bilan_cumul"].to_numpy(dtype=float)
        n = np.isfinite(samples).sum()
        if n < MIN_YEARS_REF:
            continue
        alpha, beta, gamma_loc = fit_loglogistic_lmoments(samples)
        if not np.isfinite([alpha, beta, gamma_loc]).all():
            continue
        rows.append((float(lat), float(lon), int(mc), int(window),
                     alpha, beta, gamma_loc, int(n)))
    return rows


@asset(
    name="fct_era5_spei_climatology_grid",
    group_name="indices",
    deps=[get_asset_key_for_model([hubeau_dbt_assets], "fct_era5_monthly_grid")],
    description=(
        "Paramètres log-logistiques SPEI (référence 1991-2020) par cellule ERA5 "
        "× mois calendaire × fenêtre 1/3/6/12. Rebuild full."
    ),
)
def fct_era5_spei_climatology_grid(context: AssetExecutionContext, pg: PostgreSQLResource):
    init_spei_climatology_table(pg)
    total = 0
    for window in WINDOWS:
        with pg.get_connection() as conn:
            df = pd.read_sql(
                _REF_QUERY, conn,
                params={"window": window, "window_minus_1": window - 1},
            )
        rows = fit_reference_frame(df, window)
        upsert_spei_climatology(pg, rows)
        total += len(rows)
        context.log.info("Fenêtre %d : %d cellules×mois fittées", window, len(rows))
    context.add_output_metadata({"fitted_groups": MetadataValue.int(total)})
    return total
