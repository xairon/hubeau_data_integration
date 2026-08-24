{{
  config(
    materialized = 'incremental',
    unique_key = ['era5_latitude', 'era5_longitude', 'mois'],
    incremental_strategy = 'delete+insert',
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
-- stg_era5_daily_temp_stats (t2m_mean/min/max), agrégées LOCALEMENT à partir des 24 pas
-- horaires bruts de reanalysis-era5-land — une vraie moyenne/Tn/Tx journalière. Avant, ces colonnes dérivaient
-- de stg_era5_timeseries.temperature_2m, un échantillon instantané à 00:00 UTC (biais
-- froid nocturne ~2-4°C, pas une vraie moyenne). Voir docs/ERA5.md.
--
-- CUTOVER ETP (2026-07-24) : etp_totale est désormais une ET0 de référence calculée par
-- HARGREAVES (FAO-56) à partir des Tmin/Tmax/Tmoy journaliers vrais — rendue possible
-- précisément par le cutover température ci-dessus. Auparavant etp_totale était
-- SUM(-potential_evaporation) d'ERA5-Land, qui vaut ~2,15× l'ET0 de référence (mesuré :
-- 1 756 vs 818 mm/an) et mettait la France en déficit permanent. Cette PEV brute reste
-- exposée sous etp_pev_era5 pour traçabilité, mais n'est plus consommée.
-- bilan_hydrique = precipitation_totale − etp_totale suit donc Hargreaves, et le SPEI
-- avec lui. Précipitation/nb_jours/mois_complet restent dérivés de stg_era5_timeseries.

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
    -- (24 pas horaires bruts agrégés localement, pas un instantané 00:00 UTC).
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
),

-- ETP de référence par HARGREAVES (FAO-56 eq. 52), en mm/jour :
--   ET0 = 0.0023 · Ra · (Tmoy + 17.8) · √(Tmax − Tmin)
-- Ra = rayonnement extraterrestre (FAO-56 eq. 21-25), fonction de la latitude et du
-- jour de l'année uniquement — donc entièrement calculable ici, sans donnée de
-- rayonnement/vent/humidité.
--
-- POURQUOI Hargreaves et non la `potential_evaporation` d'ERA5 : mesuré sur
-- 30 888 mailles-mois (2015-2025), la PEV vaut **2,15× l'ET0 de référence**
-- (1 756 vs 818 mm/an) et met la France en déficit hydrique permanent de −793 mm/an,
-- au lieu de l'excédent de +146 mm/an que donne Hargreaves. La PEV d'ERA5 n'est PAS
-- une ET0 de référence FAO : c'est l'évaporation d'une surface sans stress hydrique
-- calculée avec la résistance aérodynamique du modèle, connue pour surestimer
-- largement l'ET0. Hargreaves est le repli FAO-56 recommandé quand on ne dispose que
-- de la température, et c'est la méthode employée par la littérature d'attribution
-- (World Weather Attribution). Voir docs/ERA5.md.
etp_daily AS (
    SELECT
        latitude,
        longitude,
        jour,
        GREATEST(
            0.0023
            -- Ra converti de MJ/m²/j en mm/j équivalent (× 0.408)
            * 0.408 * (24 * 60 / PI()) * 0.0820
              * (1 + 0.033 * COS(2 * PI() * EXTRACT(DOY FROM jour)::numeric / 365))   -- dr
              * (
                  -- ωs · sin(φ) · sin(δ) + cos(φ) · cos(δ) · sin(ωs)
                  ACOS(GREATEST(LEAST(
                      -TAN(RADIANS(latitude))
                      * TAN(0.409 * SIN(2 * PI() * EXTRACT(DOY FROM jour)::numeric / 365 - 1.39))
                  , 1), -1))
                  * SIN(RADIANS(latitude))
                  * SIN(0.409 * SIN(2 * PI() * EXTRACT(DOY FROM jour)::numeric / 365 - 1.39))
                  + COS(RADIANS(latitude))
                  * COS(0.409 * SIN(2 * PI() * EXTRACT(DOY FROM jour)::numeric / 365 - 1.39))
                  * SIN(ACOS(GREATEST(LEAST(
                      -TAN(RADIANS(latitude))
                      * TAN(0.409 * SIN(2 * PI() * EXTRACT(DOY FROM jour)::numeric / 365 - 1.39))
                  , 1), -1)))
                )
            * (t2m_mean + 17.8)
            * SQRT(GREATEST(t2m_max - t2m_min, 0))
        , 0) AS et0_hargreaves
    FROM temp_daily
    WHERE t2m_mean IS NOT NULL AND t2m_min IS NOT NULL AND t2m_max IS NOT NULL
)

SELECT
    p.latitude  AS era5_latitude,
    p.longitude AS era5_longitude,
    DATE_TRUNC('month', p.jour)::date AS mois,

    AVG(t.t2m_mean) AS temperature_moyenne,
    MIN(t.t2m_min)  AS temperature_min,
    MAX(t.t2m_max)  AS temperature_max,

    SUM(p.total_precipitation)        AS precipitation_totale,

    -- ETP de référence (Hargreaves) : c'est CETTE colonne que consomment le bilan
    -- hydrique, le SPEI et l'application.
    SUM(x.et0_hargreaves)             AS etp_totale,
    SUM(p.total_precipitation) - SUM(x.et0_hargreaves) AS bilan_hydrique,

    -- PEV brute d'ERA5-Land, conservée pour traçabilité/comparaison. NE PAS l'utiliser
    -- comme ETP de référence : ~2,15× trop élevée (cf. le commentaire de etp_daily).
    SUM(-p.potential_evaporation)     AS etp_pev_era5,

    COUNT(*) AS nb_jours,
    -- Mois complet = autant de jours que le mois calendaire en compte
    COUNT(*) = EXTRACT(DAY FROM (DATE_TRUNC('month', p.jour) + INTERVAL '1 month - 1 day'))::int
        AS mois_complet,

    -- Complétude TEMPÉRATURE découplée de la précipitation : le LEFT JOIN température peut
    -- laisser des jours sans t2m (NULL) dans un mois par ailleurs precip-complet. Compté
    -- séparément pour que la normale STI (climato) n'utilise que des mois température-complets
    -- (COUNT ignore les t2m_mean NULL des jours non appariés par le LEFT JOIN).
    COUNT(t.t2m_mean) AS nb_jours_temp,
    COUNT(t.t2m_mean) = EXTRACT(DAY FROM (DATE_TRUNC('month', p.jour) + INTERVAL '1 month - 1 day'))::int
        AS temp_complet

FROM precip_daily p
LEFT JOIN temp_daily t
    ON p.latitude = t.latitude
    AND p.longitude = t.longitude
    AND p.jour = t.jour
LEFT JOIN etp_daily x
    ON p.latitude = x.latitude
    AND p.longitude = x.longitude
    AND p.jour = x.jour
GROUP BY p.latitude, p.longitude, DATE_TRUNC('month', p.jour)
