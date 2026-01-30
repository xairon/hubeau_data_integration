{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_station', 'mois', 'grandeur_hydro_elab'], 'unique': True},
      {'columns': ['code_station']},
      {'columns': ['code_site']},
      {'columns': ['mois'], 'type': 'brin'},
      {'columns': ['grandeur_hydro_elab']},
      {'columns': ['code_departement']}
    ],
    post_hook = [
      "{{ add_primary_key(['code_station', 'mois', 'grandeur_hydro_elab']) }}",
      "{{ convert_to_hypertable('mois', '5 years') }}",
      "{{ enable_compression(segment_by=['code_station', 'code_departement'], order_by='mois DESC', compress_after='730 days') }}",
      "{{ add_foreign_key(['code_station'], 'stg_hydrometry_stations', ['code_station']) }}"
    ]
  )
}}

-- Agrégation mensuelle des chroniques hydrométriques
-- Source: hydro_daily_chroniques (fact table quotidienne)

WITH daily AS (
    SELECT * FROM {{ ref('hydro_daily_chroniques') }}
),

monthly_agg AS (
    SELECT
        code_station,
        code_site,
        grandeur_hydro_elab,
        DATE_TRUNC('month', date)::date AS mois,
        
        -- Métadonnées (premier du groupe)
        MIN(code_departement) AS code_departement,
        MIN(nom_departement) AS nom_departement,
        MIN(code_region) AS code_region,
        MIN(libelle_region) AS libelle_region,
        MIN(libelle_station) AS libelle_station,
        MIN(libelle_site) AS libelle_site,
        MIN(type_site) AS type_site,
        MIN(code_cours_eau) AS code_cours_eau,
        MIN(libelle_cours_eau) AS libelle_cours_eau,
        
        -- Hydrométrie - Stats mensuelles
        AVG(resultat_obs_elab) AS resultat_moyen,
        MIN(resultat_obs_elab) AS resultat_min,
        MAX(resultat_obs_elab) AS resultat_max,
        MAX(resultat_obs_elab) - MIN(resultat_obs_elab) AS amplitude_mensuelle,
        STDDEV(resultat_obs_elab) AS resultat_stddev,
        
        -- Météo ERA5 - Stats mensuelles
        AVG(temperature_2m) AS temperature_moyenne,
        MIN(temperature_2m) AS temperature_min,
        MAX(temperature_2m) AS temperature_max,
        SUM(total_precipitation) AS precipitation_totale,
        AVG(potential_evaporation) AS evaporation_moyenne,
        
        -- Comptage
        COUNT(*) AS nb_jours_mesures,
        
        -- Coordonnées
        MIN(era5_latitude) AS era5_latitude,
        MIN(era5_longitude) AS era5_longitude
        
    FROM daily
    GROUP BY code_station, code_site, grandeur_hydro_elab, DATE_TRUNC('month', date)
)

SELECT
    *,
    
    -- Moyennes mobiles (fenêtres glissantes)
    AVG(resultat_moyen) OVER (
        PARTITION BY code_station, grandeur_hydro_elab
        ORDER BY mois 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS resultat_moy_mobile_3m,
    
    AVG(resultat_moyen) OVER (
        PARTITION BY code_station, grandeur_hydro_elab
        ORDER BY mois 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS resultat_moy_mobile_12m,
    
    AVG(precipitation_totale) OVER (
        PARTITION BY code_station, grandeur_hydro_elab
        ORDER BY mois 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS precipitation_moy_mobile_12m,
    
    -- Variation vs mois précédent
    resultat_moyen - LAG(resultat_moyen) OVER (
        PARTITION BY code_station, grandeur_hydro_elab ORDER BY mois
    ) AS variation_resultat_vs_mois_prec,
    
    -- Variation vs même mois année précédente
    resultat_moyen - LAG(resultat_moyen, 12) OVER (
        PARTITION BY code_station, grandeur_hydro_elab ORDER BY mois
    ) AS variation_resultat_vs_annee_prec
    
FROM monthly_agg
