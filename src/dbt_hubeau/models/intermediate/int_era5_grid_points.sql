{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['geom'], 'type': 'gist'}
    ],
    post_hook=["{{ add_primary_key(['era5_latitude', 'era5_longitude']) }}"]
  )
}}

-- Points de grille ERA5 uniques (pour jointure spatiale)
-- Source: stg_era5_timeseries
-- Normalisation 0.1° : fusionne les variantes flottantes d'une même cellule
-- (48.1 du backfill historique vs 48.09999999999995 de l'incrémental quotidien).
-- Sans ça, le mapping station→grille accroche la variante récente et rate l'historique.
-- cf. mémoire era5-coordinate-precision-bug.

WITH rounded AS (
    SELECT
        ROUND(latitude::numeric, 1) AS era5_latitude,
        ROUND(longitude::numeric, 1) AS era5_longitude
    FROM {{ ref('stg_era5_timeseries') }}
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
)

SELECT DISTINCT
    era5_latitude,
    era5_longitude,
    {{ make_point('era5_longitude', 'era5_latitude') }} AS geom
FROM rounded
