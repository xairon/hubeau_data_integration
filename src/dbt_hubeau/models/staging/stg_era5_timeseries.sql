{{
  config(
    materialized = 'incremental',
    unique_key = ['latitude', 'longitude', 'time'],
    indexes = [
      {'columns': ['latitude', 'longitude', 'time'], 'unique': True},
      {'columns': ['time'], 'type': 'brin'},
      {'columns': ['geom'], 'type': 'gist'}
    ]
  )
}}

-- Staging model for ERA5 timeseries
-- Source: bronze.era5_france_timeseries
-- Silver layer: copie bronze + typage + déduplication + PostGIS + suppression métadonnées dlt
-- Incremental: basé sur _dlt_load_id
-- Primary Key: latitude + longitude + time

WITH source AS (
    SELECT * FROM {{ source('staging', 'era5_france_timeseries') }}
    WHERE time IS NOT NULL
      {% if is_incremental() %}
      -- We only process rows from new load batches
      AND _dlt_load_id > (SELECT MAX(_dlt_load_id) FROM {{ this }})
      {% endif %}
),

deduplicated AS (
    SELECT DISTINCT ON (latitude::numeric, longitude::numeric, time)
        -- Champs castés explicitement
        latitude::numeric AS latitude,
        longitude::numeric AS longitude,
        temperature_2m::numeric AS temperature_2m,
        total_precipitation::numeric AS total_precipitation,
        potential_evaporation::numeric AS potential_evaporation,
        _dlt_load_id, -- Keep for incremental logic

        -- Sélection de tous les autres champs sauf ceux déjà castés et les métadonnées dlt
        {{ dbt_utils.star(
            from=source('staging', 'era5_france_timeseries'), 
            except=[
                "latitude",
                "longitude",
                "temperature_2m",
                "total_precipitation",
                "potential_evaporation",
                "_dlt_load_id",
                "_dlt_id"
            ]
        ) }}
    FROM source
    ORDER BY latitude::numeric, longitude::numeric, time
)

SELECT 
    *,
    {{ make_point('longitude', 'latitude') }} AS geom
FROM deduplicated
