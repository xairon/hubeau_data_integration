{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['code_bss'], 'unique': True},
      {'columns': ['era5_latitude', 'era5_longitude']},
      {'columns': ['geom'], 'type': 'gist'}
    ]
  )
}}

-- Mapping spatial: Stations piézo → Point de grille ERA5 le plus proche
-- Utilise PostGIS KNN (opérateur <->) pour trouver le vrai nearest neighbor
-- Beaucoup plus précis que le ROUND(0.1) précédent

WITH stations AS (
    SELECT * FROM {{ ref('stg_piezo_stations') }}
    WHERE geom IS NOT NULL
      -- Filtre pour la France métropolitaine (Hexagone) uniquement
      -- Les DOM-TOM n'ont pas de couverture ERA5 dans ce dataset
      AND y >= 41.0 AND y <= 51.5 
      AND x >= -5.5 AND x <= 10.0
),

era5_grid AS (
    SELECT * FROM {{ ref('int_era5_grid_points') }}
),

tme AS (
    SELECT * FROM {{ ref('stg_tme_entites') }}
),

-- PostGIS KNN: Find nearest ERA5 grid point for each station
station_nearest_era5 AS (
    SELECT DISTINCT ON (s.code_bss)
        s.code_bss,
        s.y AS station_latitude,
        s.x AS station_longitude,
        s.geom AS station_geom,
        s.codes_bdlisa,
        SPLIT_PART(s.codes_bdlisa, ',', 1) AS code_eh_primary,
        s.code_commune_insee,
        s.nom_commune,
        s.altitude_station,
        s.code_departement,
        s.nom_departement,
        -- ERA5 grid point info
        e.latitude AS era5_latitude,
        e.longitude AS era5_longitude,
        e.geom AS era5_geom,
        -- Distance in meters (using geography for accuracy)
        ST_Distance(s.geom::geography, e.geom::geography) AS distance_m
    FROM stations s
    CROSS JOIN LATERAL (
        SELECT latitude, longitude, geom
        FROM era5_grid
        ORDER BY s.geom <-> geom  -- KNN operator (uses GiST index)
        LIMIT 1
    ) e
)

SELECT
    sne.code_bss,
    sne.era5_latitude,
    sne.era5_longitude,
    sne.station_latitude,
    sne.station_longitude,
    sne.station_geom AS geom,
    sne.distance_m AS era5_distance_m,
    sne.codes_bdlisa,
    sne.code_commune_insee,
    sne.nom_commune,
    sne.altitude_station,
    sne.code_departement,
    sne.nom_departement,
    -- TME metadata (joined on primary code)
    COALESCE(t.code_eh, sne.code_eh_primary) AS code_eh,
    t.libelle_eh,
    t.niveau_eh,
    t.etat_eh,
    t.nature_eh,
    t.milieu_eh,
    t.theme_eh,
    t.origine_eh
FROM station_nearest_era5 sne
LEFT JOIN tme t ON sne.code_eh_primary = t.code_eh
