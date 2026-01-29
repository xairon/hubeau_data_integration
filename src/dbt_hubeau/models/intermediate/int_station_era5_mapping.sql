{{
  config(
    materialized = 'incremental',
    unique_key = 'code_bss',
    indexes=[
      {'columns': ['code_bss'], 'unique': True},
      {'columns': ['era5_latitude', 'era5_longitude']},
      {'columns': ['geom'], 'type': 'gist'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_bss']) }}",
      "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"
    ]
  )
}}

-- Mapping spatial: Stations piézo → Point de grille ERA5 le plus proche
-- Utilise PostGIS KNN (opérateur <->) pour trouver le vrai nearest neighbor
-- Beaucoup plus précis que le ROUND(0.1) précédent
-- Incremental: on ne calcule que pour les nouvelles stations

WITH stations AS (
    SELECT * FROM {{ ref('stg_piezo_stations') }}
    WHERE geometry IS NOT NULL
      -- Filtre pour la France métropolitaine (Hexagone) uniquement
      -- Les DOM-TOM n'ont pas de couverture ERA5 dans ce dataset
      AND y >= 41.0 AND y <= 51.5 
      AND x >= -5.5 AND x <= 10.0
      
      {% if is_incremental() %}
      -- On ne traite que les stations qu'on ne connait pas encore
      AND code_bss NOT IN (SELECT code_bss FROM {{ this }})
      {% endif %}
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
        s.geometry AS station_geom,
        s.codes_bdlisa,
        SPLIT_PART(s.codes_bdlisa, ',', 1) AS code_eh_primary,
        s.code_commune_insee,
        s.nom_commune,
        s.altitude_station,
        s.code_departement,
        s.nom_departement,
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
)

SELECT
    sne.code_bss::text AS code_bss,
    sne.era5_latitude::numeric AS era5_latitude,
    sne.era5_longitude::numeric AS era5_longitude,
    sne.station_latitude::numeric AS station_latitude,
    sne.station_longitude::numeric AS station_longitude,
    sne.station_geom AS geom,
    (sne.distance_m)::numeric AS era5_distance_m,
    sne.codes_bdlisa::text AS codes_bdlisa,
    sne.code_commune_insee::text AS code_commune_insee,
    sne.nom_commune::text AS nom_commune,
    sne.altitude_station::numeric AS altitude_station,
    sne.code_departement::text AS code_departement,
    sne.nom_departement::text AS nom_departement,
    COALESCE(t.code_eh, sne.code_eh_primary)::text AS code_eh,
    t.libelle_eh::text AS libelle_eh,
    t.niveau_eh::text AS niveau_eh,
    t.libelle_niveau_eh::text AS libelle_niveau_eh,
    t.etat_eh::text AS etat_eh,
    t.libelle_etat_eh::text AS libelle_etat_eh,
    t.nature_eh::text AS nature_eh,
    t.libelle_nature_eh::text AS libelle_nature_eh,
    t.milieu_eh::text AS milieu_eh,
    t.libelle_milieu_eh::text AS libelle_milieu_eh,
    t.theme_eh::text AS theme_eh,
    t.libelle_theme_eh::text AS libelle_theme_eh,
    t.origine_eh::text AS origine_eh,
    t.libelle_origine_eh::text AS libelle_origine_eh
FROM station_nearest_era5 sne
LEFT JOIN tme t ON sne.code_eh_primary = t.code_eh
