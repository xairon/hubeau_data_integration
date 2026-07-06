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
    -- Arrondi défensif 0.1° + dédup : silver a déjà contenu des variantes float non
    -- arrondies (jan-mai 2026, purgées) ; même défense que int_era5_grid_points /
    -- int_era5_for_all_stations pour qu'une pollution future ne fragmente pas la grille
    -- ni ne double-compte un jour (DISTINCT ON garde la ligne la plus récente).
    SELECT DISTINCT ON (ROUND(latitude, 1), ROUND(longitude, 1), time::date)
        ROUND(latitude, 1)  AS latitude,
        ROUND(longitude, 1) AS longitude,
        time::date AS jour,
        temperature_2m,
        total_precipitation,
        potential_evaporation
    FROM {{ ref('stg_era5_timeseries') }}
    {% if is_incremental() %}
    -- Lookback surchargeable pour backfill ciblé (ex. correctif coordonnées 2026) :
    -- --vars '{"era5_monthly_grid_lookback_months": 6}' — ATTENTION : DELETE manuel
    -- préalable des mois hors du predicate de 4 mois requis (sinon conflit de PK).
    WHERE time >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '{{ var("era5_monthly_grid_lookback_months", 3) }} months')
    {% endif %}
    ORDER BY ROUND(latitude, 1), ROUND(longitude, 1), time::date, created_at DESC NULLS LAST
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
