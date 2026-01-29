{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['era5_latitude', 'era5_longitude'], 'unique': True},
      {'columns': ['geom'], 'type': 'gist'}
    ],
    post_hook=["{{ add_primary_key(['era5_latitude', 'era5_longitude']) }}"]
  )
}}

-- Points de grille ERA5 uniques (pour jointure spatiale)
-- Source: stg_era5_timeseries

SELECT DISTINCT
    latitude::numeric AS era5_latitude,
    longitude::numeric AS era5_longitude,
    {{ make_point('longitude', 'latitude') }} AS geom
FROM {{ ref('stg_era5_timeseries') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
