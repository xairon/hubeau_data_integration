{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['mois_calendaire', 'fenetre']}
    ],
    post_hook = [
      "{{ add_primary_key(['era5_latitude', 'era5_longitude', 'mois_calendaire', 'fenetre']) }}"
    ]
  )
}}

-- Normales climatiques 1991-2020 par cellule × mois calendaire × fenêtre glissante (1/3/6/12 mois).
-- Fournit les paramètres SPI (gamma, méthode des moments, cf. McKee 1993 avec prob_zero)
-- et STI (moyenne/écart-type) consommés par l'asset Python fct_era5_indices_grid.
-- Fenêtres ROWS BETWEEN : le garde n_<w> = <w> protège uniquement contre le ramp-up de
-- début de série (fenêtres tronquées en tête). La contiguïté calendaire des mois est une
-- propriété amont de la grille ERA5 (0 trou vérifié 1990→2021), pas re-vérifiée ici.
--
-- COMPLÉTUDE DÉCOUPLÉE PRÉCIP / TEMP : le filtre `mois_complet` (précipitation) sélectionne
-- les mois de la fenêtre de référence, mais un mois precip-complet peut avoir une température
-- partielle/NULL (LEFT JOIN température dans le mart mensuel). Les normales de PRÉCIPITATION
-- restent calées sur `mois_complet` ; les normales de TEMPÉRATURE (temp_moyenne, temp_stddev,
-- nb_annees_temp) n'utilisent QUE les mois `temp_complet=true`. PostgreSQL interdit FILTER sur
-- une fonction fenêtre → on isole la contribution température via CASE WHEN temp_complet.

{% set windows = [1, 3, 6, 12] %}

WITH base AS (
    SELECT
        era5_latitude,
        era5_longitude,
        mois,
        precipitation_totale,
        temperature_moyenne,
        temp_complet
    FROM {{ ref('fct_era5_monthly_grid') }}
    WHERE mois_complet
      -- 1990 inclus : les fenêtres 12 mois finissant début 1991 en ont besoin
      AND mois >= DATE '1990-01-01'
      AND mois <  DATE '2021-01-01'
),

rolled AS (
    SELECT
        era5_latitude,
        era5_longitude,
        mois,
        {% for w in windows %}
        SUM(precipitation_totale) OVER (
            PARTITION BY era5_latitude, era5_longitude ORDER BY mois
            ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW) AS precip_{{ w }},
        -- Moyenne fenêtre sur les seuls mois température-complets (FILTER interdit sur fonction
        -- fenêtre → CASE WHEN). Le compagnon n_temp_<w> compte ces mois pour ne valider la
        -- fenêtre que si TOUS ses <w> mois sont température-complets (cf. unpivoted).
        AVG(CASE WHEN temp_complet THEN temperature_moyenne END) OVER (
            PARTITION BY era5_latitude, era5_longitude ORDER BY mois
            ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW) AS temp_{{ w }},
        COUNT(*) OVER (
            PARTITION BY era5_latitude, era5_longitude ORDER BY mois
            ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW) AS n_{{ w }},
        COUNT(CASE WHEN temp_complet THEN 1 END) OVER (
            PARTITION BY era5_latitude, era5_longitude ORDER BY mois
            ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW) AS n_temp_{{ w }}{{ "," if not loop.last }}
        {% endfor %}
    FROM base
),

unpivoted AS (
    {% for w in windows %}
    SELECT
        era5_latitude,
        era5_longitude,
        EXTRACT(MONTH FROM mois)::int AS mois_calendaire,
        {{ w }} AS fenetre,
        precip_{{ w }} AS precip_cumul,
        -- Température : fenêtre valide seulement si ses <w> mois sont TOUS température-complets,
        -- sinon NULL (exclue des stats sans affecter la précipitation, qui reste gardée par n_<w>).
        CASE WHEN n_temp_{{ w }} = {{ w }} THEN temp_{{ w }} END AS temp_fenetre
    FROM rolled
    WHERE mois >= DATE '1991-01-01'
      AND n_{{ w }} = {{ w }}
    {{ "UNION ALL" if not loop.last }}
    {% endfor %}
),

stats AS (
    SELECT
        era5_latitude,
        era5_longitude,
        mois_calendaire,
        fenetre,
        COUNT(*)                                            AS nb_annees,
        AVG(precip_cumul)                                   AS precip_moyenne,
        STDDEV_SAMP(precip_cumul)                           AS precip_stddev,
        AVG((precip_cumul <= 0)::int::numeric)              AS prob_zero,
        AVG(precip_cumul)      FILTER (WHERE precip_cumul > 0) AS precip_moy_pos,
        VAR_SAMP(precip_cumul) FILTER (WHERE precip_cumul > 0) AS precip_var_pos,
        -- Années de référence TEMPÉRATURE : temp_fenetre est NULL pour les fenêtres non
        -- température-complètes → COUNT/AVG/STDDEV les ignorent. nb_annees_temp <= nb_annees.
        COUNT(temp_fenetre)                                 AS nb_annees_temp,
        AVG(temp_fenetre)                                   AS temp_moyenne,
        STDDEV_SAMP(temp_fenetre)                           AS temp_stddev
    FROM unpivoted
    GROUP BY era5_latitude, era5_longitude, mois_calendaire, fenetre
)

SELECT
    era5_latitude,
    era5_longitude,
    mois_calendaire,
    fenetre,
    nb_annees,
    precip_moyenne,
    precip_stddev,
    prob_zero,
    -- Gamma méthode des moments sur les cumuls > 0 (β = scale)
    CASE WHEN precip_var_pos > 0 THEN precip_moy_pos ^ 2 / precip_var_pos END AS gamma_alpha,
    CASE WHEN precip_moy_pos > 0 AND precip_var_pos > 0
         THEN precip_var_pos / precip_moy_pos END                             AS gamma_beta,
    nb_annees_temp,
    temp_moyenne,
    temp_stddev
FROM stats
