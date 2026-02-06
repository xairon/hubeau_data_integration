{{
  config(
    materialized = 'incremental',
    unique_key = 'code_station',
    incremental_strategy = 'append',
    indexes=[
      {'columns': ['code_site']},
      {'columns': ['era5_latitude', 'era5_longitude']},
      {'columns': ['geom'], 'type': 'gist'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_station']) }}",
      "{{ add_foreign_key(['code_station'], 'stg_hydrometry_stations', ['code_station']) }}",
      "{{ add_foreign_key(['code_site'], 'stg_hydrometry_sites', ['code_site']) }}"
    ]
  )
}}

-- Mapping spatial: Stations hydrométriques → Point de grille ERA5 le plus proche
-- Utilise PostGIS KNN (opérateur <->) pour trouver le vrai nearest neighbor
-- Incrémental : ne calcule que pour les stations absentes de la table.
-- Optionnel: forcer un recalcul complet via var('recompute_hydro_station_era5_mapping', false).

{% set recompute_all = var('recompute_hydro_station_era5_mapping', false) %}

WITH stations AS (
    SELECT s.*
    FROM {{ ref('stg_hydrometry_stations') }} s
    WHERE s.geometry IS NOT NULL
      AND s.latitude_station IS NOT NULL
      AND s.longitude_station IS NOT NULL
      AND s.latitude_station >= 41.0 AND s.latitude_station <= 52.0
      AND s.longitude_station >= -5.5 AND s.longitude_station <= 10.0
      {% if is_incremental() and not recompute_all %}
      AND NOT EXISTS (SELECT 1 FROM {{ this }} t WHERE t.code_station = s.code_station)
      {% endif %}
),

sites AS (
    SELECT * FROM {{ ref('stg_hydrometry_sites') }}
),

era5_grid AS (
    SELECT * FROM {{ ref('int_era5_grid_points') }}
),

station_nearest_era5 AS (
    SELECT DISTINCT ON (s.code_station)
        s.code_station,
        s.code_site,
        s.latitude_station AS station_latitude,
        s.longitude_station AS station_longitude,
        s.geometry AS station_geom,
        s.code_departement,
        s.libelle_departement AS nom_departement,
        s.libelle_station,
        s.libelle_site,
        s.code_cours_eau,
        s.libelle_cours_eau,
        s.uri_cours_eau,
        -- ERA5 grid point info
        e.era5_latitude,
        e.era5_longitude,
        e.geom AS era5_geom,
        -- Distance in meters (using geography for accuracy)
        ST_Distance(s.geometry::geography, e.geom::geography) AS distance_m
    FROM stations s
    CROSS JOIN LATERAL (
        SELECT era5_latitude, era5_longitude, geom
        FROM era5_grid
        ORDER BY s.geometry <-> geom  -- KNN operator (uses GiST index)
        LIMIT 1
    ) e
),

site_metadata AS (
    SELECT
        code_site,
        surface_bv,
        code_commune_site,
        libelle_commune,
        code_region,
        libelle_region,
        code_zone_hydro_site,
        type_site
    FROM sites
)

SELECT
    sne.code_station::text AS code_station,
    sne.code_site::text AS code_site,
    sne.station_latitude::numeric AS station_latitude,
    sne.station_longitude::numeric AS station_longitude,
    sne.station_geom AS geom,
    sne.era5_latitude::numeric AS era5_latitude,
    sne.era5_longitude::numeric AS era5_longitude,
    (sne.distance_m)::numeric AS era5_distance_m,
    sne.code_departement::text AS code_departement,
    sne.nom_departement::text AS nom_departement,
    sne.libelle_station::text AS libelle_station,
    sne.libelle_site::text AS libelle_site,
    sne.code_cours_eau::text AS code_cours_eau,
    sne.libelle_cours_eau::text AS libelle_cours_eau,
    sne.uri_cours_eau::text AS uri_cours_eau,
    sm.surface_bv::numeric AS surface_bv,
    sm.code_commune_site::text AS code_commune_site,
    sm.libelle_commune::text AS libelle_commune,
    sm.code_region::text AS code_region,
    sm.libelle_region::text AS libelle_region,
    sm.code_zone_hydro_site::text AS code_zone_hydro_site,
    sm.type_site::text AS type_site
FROM station_nearest_era5 sne
LEFT JOIN site_metadata sm ON sne.code_site = sm.code_site
