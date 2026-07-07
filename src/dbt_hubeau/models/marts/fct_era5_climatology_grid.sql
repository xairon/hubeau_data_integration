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

{% set windows = [1, 3, 6, 12] %}

WITH base AS (
    SELECT
        era5_latitude,
        era5_longitude,
        mois,
        precipitation_totale,
        temperature_moyenne
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
        AVG(temperature_moyenne) OVER (
            PARTITION BY era5_latitude, era5_longitude ORDER BY mois
            ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW) AS temp_{{ w }},
        COUNT(*) OVER (
            PARTITION BY era5_latitude, era5_longitude ORDER BY mois
            ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW) AS n_{{ w }}{{ "," if not loop.last }}
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
        temp_{{ w }}   AS temp_fenetre
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
    temp_moyenne,
    temp_stddev
FROM stats
