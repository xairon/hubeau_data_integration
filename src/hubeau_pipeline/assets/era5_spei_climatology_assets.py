"""Référence SPEI 1991-2020 → gold.fct_era5_spei_climatology_grid.

Fit log-logistique (L-moments) du cumul bilan hydrique par cellule × mois
calendaire × fenêtre. Rebuild rare (full), consommé par fct_era5_indices_grid.
"""
import logging

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_dbt import get_asset_key_for_model

from ..ml.era5_indices import MIN_YEARS_REF, _fit_loglogistic_detailed
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


# Motifs de rejet suivis dans les stats retournées par fit_reference_frame,
# en plus de "n_annees_insuffisant" (garde MIN_YEARS_REF, distincte du
# "n_insuffisant" interne au fit — cf. _fit_loglogistic_detailed).
_FIT_REJECT_REASONS = ("pwm_degenere", "beta_hors_domaine", "alpha_invalide", "n_insuffisant")


def fit_reference_frame(df, window):
    """Groupe df par (cellule, mois calendaire) et fitte la log-logistique.

    Retourne (rows, stats) : rows est la liste de tuples upsertables (groupes
    acceptés) ; stats est un dict[str, int] exposant, pour tout groupe examiné,
    la raison de rejet — "n_annees_insuffisant" (< MIN_YEARS_REF) ou l'un des
    motifs de _fit_loglogistic_detailed — afin de pouvoir agréger la couverture
    a posteriori sans que les rejets ne laissent aucune trace.
    """
    rows = []
    stats = {"groupes": 0, "ok": 0, "n_annees_insuffisant": 0}
    stats.update(dict.fromkeys(_FIT_REJECT_REASONS, 0))

    for (lat, lon, mc), grp in df.groupby(
        ["era5_latitude", "era5_longitude", "mois_calendaire"], sort=False
    ):
        stats["groupes"] += 1
        samples = grp["bilan_cumul"].to_numpy(dtype=float)
        n = np.isfinite(samples).sum()
        if n < MIN_YEARS_REF:
            stats["n_annees_insuffisant"] += 1
            continue
        alpha, beta, gamma_loc, reason = _fit_loglogistic_detailed(samples)
        if reason is not None:
            stats[reason] += 1
            continue
        stats["ok"] += 1
        rows.append((float(lat), float(lon), int(mc), int(window),
                     alpha, beta, gamma_loc, int(n)))
    return rows, stats


_ALL_REJECT_REASONS = ("n_annees_insuffisant", *_FIT_REJECT_REASONS)


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
    cumulative = {"groupes": 0, "ok": 0}
    cumulative.update(dict.fromkeys(_ALL_REJECT_REASONS, 0))

    for window in WINDOWS:
        with pg.get_connection() as conn:
            df = pd.read_sql(
                _REF_QUERY, conn,
                params={"window": window, "window_minus_1": window - 1},
            )
        rows, stats = fit_reference_frame(df, window)
        upsert_spei_climatology(pg, rows)
        total += len(rows)

        groupes = stats["groupes"]

        def _pct(n, _groupes=groupes):
            return (100.0 * n / _groupes) if _groupes else 0.0

        reject_detail = ", ".join(
            f"{reason}={stats[reason]} ({_pct(stats[reason]):.1f}%)"
            for reason in _ALL_REJECT_REASONS
        )
        context.log.info(
            "Fenêtre %d : %d groupes, %d ok (%.1f%%) — rejets : %s",
            window, groupes, stats["ok"], _pct(stats["ok"]), reject_detail,
        )
        for key, value in stats.items():
            cumulative[key] += value

    metadata = {
        f"rejets_{reason}": MetadataValue.int(cumulative[reason])
        for reason in _ALL_REJECT_REASONS
    }
    metadata["groupes_total"] = MetadataValue.int(cumulative["groupes"])
    metadata["fitted_groups"] = MetadataValue.int(total)
    metadata["taux_couverture_pct"] = MetadataValue.float(
        round(100.0 * cumulative["ok"] / cumulative["groupes"], 2)
        if cumulative["groupes"] else 0.0
    )
    context.add_output_metadata(metadata)
    return total
