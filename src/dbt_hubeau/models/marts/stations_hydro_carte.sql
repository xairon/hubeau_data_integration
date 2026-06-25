{{
  config(
    materialized = 'incremental',
    unique_key = ['code_station'],
    incremental_strategy = 'delete+insert',
    indexes = [
      {'columns': ['code_site']},
      {'columns': ['grandeur_hydro_principale']},
      {'columns': ['geom'], 'type': 'gist'}
    ],
    post_hook = [
      "{{ add_primary_key(['code_station']) }}"
    ]
  )
}}

-- Mart "prêt carte" : une ligne par station hydrométrique avec géométrie + indicateurs.
-- INCREMENTAL: recalcul des stations récentes uniquement.

{% set lookback_days = var('streaming_lookback_days', 7) %}

WITH recent_stations AS (
    SELECT DISTINCT code_station
    FROM {{ ref('hydro_daily_chroniques') }}
    WHERE date >= CURRENT_DATE - INTERVAL '{{ lookback_days }} days'
),
mapping AS (
    SELECT
        code_station,
        code_site,
        geom,
        station_latitude AS latitude,
        station_longitude AS longitude,
        code_departement,
        nom_departement,
        code_region,
        libelle_region,
        libelle_station,
        libelle_site,
        type_site,
        surface_bv,
        code_cours_eau,
        libelle_cours_eau,
        uri_cours_eau
    FROM {{ ref('int_hydro_station_era5_mapping') }}
    {% if is_incremental() %}
    WHERE code_station IN (SELECT code_station FROM recent_stations)
    {% endif %}
),
dim AS (
    SELECT
        code_station,
        grandeur_hydro_principale,
        premiere_mesure,
        derniere_mesure,
        nb_jours_total,
        nb_mois_total,
        resultat_moyen_global,
        resultat_min_global,
        resultat_max_global,
        resultat_stddev_global,
        classification_resultat_dern_annee,
        percentile_resultat_dern_annee
    FROM {{ ref('dim_hydro_stations') }}
    {% if is_incremental() %}
    WHERE code_station IN (SELECT code_station FROM recent_stations)
    {% endif %}
)

SELECT
    m.code_station,
    m.code_site,
    m.geom,
    m.latitude,
    m.longitude,
    m.code_departement,
    m.nom_departement,
    m.code_region,
    m.libelle_region,
    m.libelle_station,
    m.libelle_site,
    m.type_site,
    m.surface_bv,
    m.code_cours_eau,
    m.libelle_cours_eau,
    m.uri_cours_eau,
    d.grandeur_hydro_principale,
    d.premiere_mesure,
    d.derniere_mesure,
    d.nb_jours_total,
    d.nb_mois_total,
    d.resultat_moyen_global,
    d.resultat_min_global,
    d.resultat_max_global,
    d.resultat_stddev_global,
    d.classification_resultat_dern_annee,
    d.percentile_resultat_dern_annee
FROM mapping m
LEFT JOIN dim d ON m.code_station = d.code_station
