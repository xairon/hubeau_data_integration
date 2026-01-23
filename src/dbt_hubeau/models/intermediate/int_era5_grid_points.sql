{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['latitude', 'longitude'], 'unique': True},
      {'columns': ['geom'], 'type': 'gist'}
    ]
  )
}}

-- Reference table of unique ERA5 grid points
-- Used for fast spatial lookups from station tables
-- Source: stg_era5_timeseries (deduplicated to grid points only)

SELECT DISTINCT
    latitude,
    longitude,
    {{ make_point('longitude', 'latitude') }} AS geom
FROM {{ ref('stg_era5_timeseries') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
