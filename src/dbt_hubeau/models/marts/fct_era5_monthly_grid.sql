{{
  config(
    materialized = 'incremental',
    unique_key = ['era5_latitude', 'era5_longitude', 'mois'],
    incremental_strategy = 'delete+insert',
    incremental_predicates = [
      time_range_delete_predicate('mois', '4 months')
    ],
    indexes = [
      {'columns': ['mois'], 'type': 'brin'},
      {'columns': ['era5_latitude', 'era5_longitude']}
    ],
    post_hook = [
      "{{ add_primary_key(['era5_latitude', 'era5_longitude', 'mois']) }}"
    ]
  )
}}

-- Agrégation mensuelle ERA5 par point de grille (0.1°), toute la France, 1950→présent.
-- Base du module Climat junon : vue Situation, séries de point, comparaisons.
-- Signe PEV : ERA5 stocke la PEV négative (flux descendant) → etp_totale = SUM(-pev) (mm positifs).
-- INCREMENTAL delete+insert : régénère les 3 derniers mois (predicate 4 mois > fenêtre
-- régénérée pour couvrir le 1er du mois tronqué — sinon conflit de PK).
-- Table plain (PAS d'hypertable) : règle projet pour les tables mensuelles.

WITH daily AS (
    SELECT
        latitude,
        longitude,
        time::date AS jour,
        temperature_2m,
        total_precipitation,
        potential_evaporation
    FROM {{ ref('stg_era5_timeseries') }}
    {% if is_incremental() %}
    WHERE time >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
    {% endif %}
)

SELECT
    latitude  AS era5_latitude,
    longitude AS era5_longitude,
    DATE_TRUNC('month', jour)::date AS mois,

    AVG(temperature_2m) AS temperature_moyenne,
    MIN(temperature_2m) AS temperature_min,
    MAX(temperature_2m) AS temperature_max,

    SUM(total_precipitation)        AS precipitation_totale,
    SUM(-potential_evaporation)     AS etp_totale,
    SUM(total_precipitation) - SUM(-potential_evaporation) AS bilan_hydrique,

    COUNT(*) AS nb_jours,
    -- Mois complet = autant de jours que le mois calendaire en compte
    COUNT(*) = EXTRACT(DAY FROM (DATE_TRUNC('month', jour) + INTERVAL '1 month - 1 day'))::int
        AS mois_complet

FROM daily
GROUP BY latitude, longitude, DATE_TRUNC('month', jour)
