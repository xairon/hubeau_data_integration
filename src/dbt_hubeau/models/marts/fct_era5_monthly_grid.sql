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
--
-- CUTOVER TEMPÉRATURE (2026-07-13) : temperature_moyenne/min/max viennent désormais de
-- stg_era5_daily_temp_stats (t2m_mean/min/max), calculées côté CDS à partir des 24 pas
-- horaires du jour — une vraie moyenne/Tn/Tx journalière. Avant, ces colonnes dérivaient
-- de stg_era5_timeseries.temperature_2m, un échantillon instantané à 00:00 UTC (biais
-- froid nocturne ~2-4°C, pas une vraie moyenne). Précipitation/ETP/bilan_hydrique/nb_jours/
-- mois_complet restent inchangés, dérivés de stg_era5_timeseries (pas d'équivalent
-- journalier vrai dispo pour ces variables). Voir docs/ERA5.md.

WITH precip_daily AS (
    -- Arrondi défensif 0.1° + dédup : silver a déjà contenu des variantes float non
    -- arrondies (jan-mai 2026, purgées) ; même défense que int_era5_grid_points /
    -- int_era5_for_all_stations pour qu'une pollution future ne fragmente pas la grille
    -- ni ne double-compte un jour (DISTINCT ON garde la ligne la plus récente).
    SELECT DISTINCT ON (ROUND(latitude, 1), ROUND(longitude, 1), time::date)
        ROUND(latitude, 1)  AS latitude,
        ROUND(longitude, 1) AS longitude,
        time::date AS jour,
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
),

temp_daily AS (
    -- Même dédup défensive que precip_daily, appliquée à la vraie source journalière
    -- (24 pas horaires agrégés côté CDS, pas un instantané 00:00 UTC).
    SELECT DISTINCT ON (ROUND(latitude, 1), ROUND(longitude, 1), time::date)
        ROUND(latitude, 1)  AS latitude,
        ROUND(longitude, 1) AS longitude,
        time::date AS jour,
        t2m_mean,
        t2m_min,
        t2m_max
    FROM {{ ref('stg_era5_daily_temp_stats') }}
    {% if is_incremental() %}
    WHERE time >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '{{ var("era5_monthly_grid_lookback_months", 3) }} months')
    {% endif %}
    ORDER BY ROUND(latitude, 1), ROUND(longitude, 1), time::date, created_at DESC NULLS LAST
)

SELECT
    p.latitude  AS era5_latitude,
    p.longitude AS era5_longitude,
    DATE_TRUNC('month', p.jour)::date AS mois,

    AVG(t.t2m_mean) AS temperature_moyenne,
    MIN(t.t2m_min)  AS temperature_min,
    MAX(t.t2m_max)  AS temperature_max,

    SUM(p.total_precipitation)        AS precipitation_totale,
    SUM(-p.potential_evaporation)     AS etp_totale,
    SUM(p.total_precipitation) - SUM(-p.potential_evaporation) AS bilan_hydrique,

    COUNT(*) AS nb_jours,
    -- Mois complet = autant de jours que le mois calendaire en compte
    COUNT(*) = EXTRACT(DAY FROM (DATE_TRUNC('month', p.jour) + INTERVAL '1 month - 1 day'))::int
        AS mois_complet

FROM precip_daily p
LEFT JOIN temp_daily t
    ON p.latitude = t.latitude
    AND p.longitude = t.longitude
    AND p.jour = t.jour
GROUP BY p.latitude, p.longitude, DATE_TRUNC('month', p.jour)
