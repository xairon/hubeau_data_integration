{{
  config(
    materialized = 'incremental',
    unique_key = ['latitude', 'longitude', 'time'],
    incremental_strategy = 'merge',
    indexes = [
      {'columns': ['latitude', 'longitude', 'time'], 'unique': True},
      {'columns': ['time'], 'type': 'brin'},
      {'columns': ['geometry'], 'type': 'gist'},
      {'columns': ['source_file_id']}
    ],
    post_hook=[
      "{{ convert_to_hypertable('time', '1 month') }}"
    ]
  )
}}

-- Staging model for ERA5 timeseries
-- Source: bronze.era5_france_timeseries
-- Silver layer: copie bronze + typage + déduplication + PostGIS
-- Incremental: basé sur source_file_id (identifiant unique par batch d'insertion)
-- Primary Key: latitude + longitude + time

WITH source AS (
    SELECT * FROM {{ source('staging', 'era5_france_timeseries') }}
    WHERE time IS NOT NULL
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL
      AND temperature_2m IS NOT NULL
    {% if is_incremental() %}
      -- Incrémental "safe": ne traite que les nouveaux timestamps
      -- (beaucoup moins coûteux qu'un NOT IN sur DISTINCT source_file_id)
      AND time > (SELECT COALESCE(MAX(time), '1900-01-01'::timestamp) FROM {{ this }})
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
        source_file_id,
        time,
        created_at
    FROM source
    ORDER BY latitude::numeric, longitude::numeric, time
)

SELECT 
    *,
    {{ make_point('longitude', 'latitude') }} AS geometry
FROM deduplicated

