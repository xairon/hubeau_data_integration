-- Staging model for ERA5 timeseries
-- Source: bronze.era5_france_timeseries
-- Silver layer: autocast + filtrage des observations nulles (météo)

WITH source AS (
    SELECT * FROM {{ source('staging', 'era5_france_timeseries') }}
)

SELECT
    id,
    time,
    latitude,
    longitude,
    temperature_2m,
    total_precipitation,
    potential_evaporation,
    source_file_id
FROM source
WHERE time IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
  -- Filtrer les lignes où toutes les observations météo sont nulles
  AND (
    temperature_2m IS NOT NULL 
    OR total_precipitation IS NOT NULL 
    OR potential_evaporation IS NOT NULL
  )
