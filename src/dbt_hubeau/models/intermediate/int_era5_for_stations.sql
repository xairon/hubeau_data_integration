{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['latitude', 'longitude', 'era5_date']}
    ]
  )
}}

-- ERA5 filtré uniquement sur les points de grille utilisés par les stations piézo
-- Réduit le volume de ~300M lignes à ~10-15M lignes (seulement les grid points avec stations)

WITH station_grid_points AS (
    -- Récupère les points de grille ERA5 uniques utilisés par les stations
    SELECT DISTINCT 
        era5_latitude AS latitude,
        era5_longitude AS longitude
    FROM {{ ref('int_station_era5_mapping') }}
),

filtered_era5 AS (
    SELECT
        e.latitude,
        e.longitude,
        e.time::date AS era5_date,
        e.temperature_2m,
        e.total_precipitation,
        e.potential_evaporation
    FROM {{ ref('stg_era5_timeseries') }} e
    INNER JOIN station_grid_points g
        ON e.latitude = g.latitude
        AND e.longitude = g.longitude
)

SELECT * FROM filtered_era5
