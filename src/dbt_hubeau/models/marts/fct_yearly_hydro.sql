{{
  config(
    materialized = 'incremental',
    unique_key = ['code_station', 'annee', 'grandeur_hydro_elab'],
    incremental_strategy = 'delete+insert',
    indexes = [
      {'columns': ['code_station']},
      {'columns': ['code_site']},
      {'columns': ['annee']},
      {'columns': ['grandeur_hydro_elab']},
      {'columns': ['code_departement']}
    ],
    post_hook = [
      "{{ add_primary_key(['code_station', 'annee', 'grandeur_hydro_elab']) }}",
      "{{ add_foreign_key(['code_station'], 'stg_hydrometry_stations', ['code_station']) }}"
    ]
  )
}}

-- Statistiques annuelles par station hydrométrique
-- Source: fct_monthly_hydro
-- INCREMENTAL: recalcul complet des stations récentes (toutes années).

{% set lookback_days = var('streaming_lookback_days', 7) %}

WITH recent_stations AS (
    SELECT DISTINCT code_station
    FROM {{ ref('hydro_daily_chroniques') }}
    WHERE date >= CURRENT_DATE - INTERVAL '{{ lookback_days }} days'
),
monthly AS (
    SELECT * FROM {{ ref('fct_monthly_hydro') }}
    {% if is_incremental() %}
    WHERE code_station IN (SELECT code_station FROM recent_stations)
    {% endif %}
),

yearly_agg AS (
    SELECT
        code_station,
        code_site,
        grandeur_hydro_elab,
        EXTRACT(YEAR FROM mois)::integer AS annee,
        
        -- Métadonnées station
        MIN(code_departement) AS code_departement,
        MIN(nom_departement) AS nom_departement,
        MIN(code_region) AS code_region,
        MIN(libelle_region) AS libelle_region,
        MIN(libelle_station) AS libelle_station,
        MIN(libelle_site) AS libelle_site,
        
        -- Hydrométrie - Stats annuelles
        AVG(resultat_moyen) AS resultat_moyen_annuel,
        MIN(resultat_min) AS resultat_min_annuel,
        MAX(resultat_max) AS resultat_max_annuel,
        MAX(resultat_max) - MIN(resultat_min) AS amplitude_annuelle,
        STDDEV(resultat_moyen) AS resultat_stddev_annuel,
        
        -- Météo - Stats annuelles
        AVG(temperature_moyenne) AS temperature_moyenne_annuelle,
        MIN(temperature_min) AS temperature_min_annuelle,
        MAX(temperature_max) AS temperature_max_annuelle,
        SUM(precipitation_totale) AS precipitation_totale_annuelle,
        AVG(evaporation_moyenne) AS evaporation_moyenne_annuelle,
        
        -- Comptage
        SUM(nb_jours_mesures) AS nb_jours_mesures_annuel,
        COUNT(*) AS nb_mois_mesures,
        
        -- Coordonnées
        MIN(era5_latitude) AS era5_latitude,
        MIN(era5_longitude) AS era5_longitude
        
    FROM monthly
    GROUP BY code_station, code_site, grandeur_hydro_elab, EXTRACT(YEAR FROM mois)
),

-- Étape 1: Calcul des percentiles et LAG (une seule fois)
yearly_with_percentiles AS (
    SELECT
        *,
        PERCENT_RANK() OVER (
            PARTITION BY code_station, grandeur_hydro_elab ORDER BY resultat_moyen_annuel
        ) AS percentile_resultat_historique,
        LAG(resultat_moyen_annuel) OVER (
            PARTITION BY code_station, grandeur_hydro_elab ORDER BY annee
        ) AS resultat_annee_prec,
        LAG(precipitation_totale_annuelle) OVER (
            PARTITION BY code_station, grandeur_hydro_elab ORDER BY annee
        ) AS precipitation_annee_prec
    FROM yearly_agg
)

SELECT
    code_station,
    code_site,
    grandeur_hydro_elab,
    annee,
    code_departement,
    nom_departement,
    code_region,
    libelle_region,
    libelle_station,
    libelle_site,
    resultat_moyen_annuel,
    resultat_min_annuel,
    resultat_max_annuel,
    amplitude_annuelle,
    resultat_stddev_annuel,
    temperature_moyenne_annuelle,
    temperature_min_annuelle,
    temperature_max_annuelle,
    precipitation_totale_annuelle,
    evaporation_moyenne_annuelle,
    nb_jours_mesures_annuel,
    nb_mois_mesures,
    era5_latitude,
    era5_longitude,

    -- Variation vs année précédente
    resultat_moyen_annuel - resultat_annee_prec AS variation_resultat_vs_annee_prec,
    precipitation_totale_annuelle - precipitation_annee_prec AS variation_precipitation_vs_annee_prec,

    -- Percentile (calculé une seule fois dans la CTE)
    percentile_resultat_historique,

    -- Classification simple par quantiles
    CASE
        WHEN percentile_resultat_historique < 0.2 THEN 'TRES_BAS'
        WHEN percentile_resultat_historique < 0.4 THEN 'BAS'
        WHEN percentile_resultat_historique < 0.6 THEN 'NORMAL'
        WHEN percentile_resultat_historique < 0.8 THEN 'HAUT'
        ELSE 'TRES_HAUT'
    END AS classification_resultat_annuel

FROM yearly_with_percentiles
