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
