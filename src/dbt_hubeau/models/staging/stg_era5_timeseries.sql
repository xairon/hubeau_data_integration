{% set reprocess_from_ts = var('era5_reprocess_from_timestamp', none) %}
{{
  config(
    materialized = 'incremental',
    unique_key = ['latitude', 'longitude', 'time'],
    incremental_strategy = 'delete+insert' if reprocess_from_ts is not none else 'append',
    indexes = [
      {'columns': ['latitude', 'longitude', 'time'], 'unique': True},
      {'columns': ['time'], 'type': 'brin'},
      {'columns': ['geometry'], 'type': 'gist'},
      {'columns': ['source_file_id']}
    ],
    post_hook=[
      "{{ add_primary_key(['latitude', 'longitude', 'time']) }}",
      "{{ convert_to_hypertable('time', '1 month') }}",
      "{{ enable_compression(segment_by=['latitude', 'longitude'], order_by='time DESC', compress_after='90 days') }}"
    ]
  )
}}

-- Staging model for ERA5 timeseries
-- Source: bronze.era5_france_timeseries
-- Silver: typage, déduplication, PostGIS. Incremental append (MERGE non supporté sur hypertables compressées).
-- Primary Key: latitude + longitude + time. Filtre incrémental: time > max(time).

WITH source AS (
    SELECT * FROM {{ source('staging', 'era5_france_timeseries') }}
    WHERE time IS NOT NULL
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL
      AND temperature_2m IS NOT NULL
    {% if is_incremental() %}
      {% if reprocess_from_ts is not none %}
      AND time >= '{{ reprocess_from_ts }}'::timestamp
      {% else %}
      -- Incrémental "safe": ne traite que les nouveaux timestamps
      -- (beaucoup moins coûteux qu'un NOT IN sur DISTINCT source_file_id)
      AND time > (SELECT COALESCE(MAX(time), '1900-01-01'::timestamp) FROM {{ this }})
      {% endif %}
    {% endif %}
),

deduplicated AS (
    SELECT DISTINCT ON ({{ cast_silver_numeric('latitude') }}, {{ cast_silver_numeric('longitude') }}, time)
        -- Champs castés explicitement (gère NULL / chaîne vide / littéral 'NULL')
        {{ cast_silver_numeric('latitude') }} AS latitude,
        {{ cast_silver_numeric('longitude') }} AS longitude,
        {{ cast_silver_numeric('temperature_2m') }} AS temperature_2m,
        {{ cast_silver_numeric('total_precipitation') }} AS total_precipitation,
        {{ cast_silver_numeric('potential_evaporation') }} AS potential_evaporation,
        {{ cast_silver_timestamp('time') }} AS time,
        source_file_id,
        created_at
    FROM source
    ORDER BY {{ cast_silver_numeric('latitude') }}, {{ cast_silver_numeric('longitude') }}, time
)

SELECT 
    *,
    {{ make_point('longitude', 'latitude') }} AS geometry
FROM deduplicated

