{{
  config(
    materialized = 'incremental',
    unique_key = ['code_bss'],
    incremental_strategy = 'delete+insert',
    indexes = [
      {'columns': ['code_bss'], 'unique': True},
      {'columns': ['code_eh']},
      {'columns': ['niveau_alerte']},
      {'columns': ['geom'], 'type': 'gist'}
    ],
    post_hook = [
      "{{ add_primary_key(['code_bss']) }}"
    ]
  )
}}

-- Mart "prêt carte" pour Superset : une ligne par station avec géométrie + entité BDLISA + indicateurs (alerte, tendance).
-- Évite les jointures côté BI : une seule table pour les calques "stations piézo" avec libellés et alertes.
-- INCREMENTAL: recalcul des stations récentes uniquement.

{% set lookback_days = var('streaming_lookback_days', 7) %}

WITH recent_stations AS (
    SELECT DISTINCT code_bss
    FROM {{ ref('hubeau_daily_chroniques') }}
    WHERE date >= CURRENT_DATE - INTERVAL '{{ lookback_days }} days'
),
mapping AS (
    SELECT
        code_bss,
        geom,
        station_latitude AS latitude,
        station_longitude AS longitude,
        code_eh,
        libelle_eh,
        libelle_niveau_eh,
        etat_eh,
        libelle_etat_eh,
        nature_eh,
        libelle_nature_eh,
        milieu_eh,
        libelle_milieu_eh,
        theme_eh,
        libelle_theme_eh,
        origine_eh,
        libelle_origine_eh,
        code_commune_insee,
        nom_commune,
        code_departement,
        nom_departement,
        altitude_station,
        era5_latitude,
        era5_longitude,
        era5_distance_m
    FROM {{ ref('int_station_era5_mapping') }}
    {% if is_incremental() %}
    WHERE code_bss IN (SELECT code_bss FROM recent_stations)
    {% endif %}
),
dim AS (
    SELECT
        code_bss,
        niveau_alerte,
        tendance_classification,
        qualite_tendance,
        niveau_moyen_global,
        niveau_min_absolu,
        niveau_max_absolu,
        derniere_annee,
        niveau_derniere_annee,
        classification_derniere_annee,
        premiere_mesure,
        derniere_mesure,
        nb_mesures_total
    FROM {{ ref('dim_piezo_stations') }}
    {% if is_incremental() %}
    WHERE code_bss IN (SELECT code_bss FROM recent_stations)
    {% endif %}
)

SELECT
    m.code_bss,
    m.geom,
    m.latitude,
    m.longitude,
    m.code_eh,
    m.libelle_eh,
    m.libelle_niveau_eh,
    m.etat_eh,
    m.libelle_etat_eh,
    m.nature_eh,
    m.libelle_nature_eh,
    m.milieu_eh,
    m.libelle_milieu_eh,
    m.theme_eh,
    m.libelle_theme_eh,
    m.origine_eh,
    m.libelle_origine_eh,
    m.code_commune_insee,
    m.nom_commune,
    m.code_departement,
    m.nom_departement,
    m.altitude_station,
    m.era5_latitude,
    m.era5_longitude,
    m.era5_distance_m,
    d.niveau_alerte,
    d.tendance_classification,
    d.qualite_tendance,
    d.niveau_moyen_global,
    d.niveau_min_absolu,
    d.niveau_max_absolu,
    d.derniere_annee,
    d.niveau_derniere_annee,
    d.classification_derniere_annee,
    d.premiere_mesure,
    d.derniere_mesure,
    d.nb_mesures_total
FROM mapping m
LEFT JOIN dim d ON m.code_bss = d.code_bss
