{% set reprocess_from_ts = var('era5_reprocess_from_timestamp', none) %}
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    indexes = [
      {'columns': ['time'], 'type': 'brin'},
      {'columns': ['geometry'], 'type': 'gist'},
      {'columns': ['source_file_id']}
    ],
    post_hook=[
      "{{ add_primary_key(['latitude', 'longitude', 'time']) }}",
      "{{ convert_to_hypertable('time', '1 month') }}",
      "{{ enable_compression(segment_by=[], order_by='time DESC', compress_after='90 days') }}"
    ]
  )
}}

-- Staging model for ERA5 timeseries
-- Source: bronze.era5_france_timeseries
-- Silver: typage, déduplication, PostGIS. Incremental delete+insert (safe dedup on hypertables).
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
    -- Normalisation de précision : ERA5-Land est une grille à 0.1°, mais le backfill
    -- historique et l'incrémental quotidien écrivent les coords à des précisions
    -- flottantes différentes (ex. 48.1 vs 48.09999999999995). On arrondit à 0.1° pour
    -- fusionner ces variantes en un seul point de grille (sinon doublons en aval →
    -- mapping station→grille qui rate l'historique). cf. mémoire era5-coordinate-precision-bug.
    SELECT DISTINCT ON (ROUND({{ cast_silver_numeric('latitude') }}, 1), ROUND({{ cast_silver_numeric('longitude') }}, 1), {{ cast_silver_timestamp('time') }})
        -- Champs castés explicitement (gère NULL / chaîne vide / littéral 'NULL')
        ROUND({{ cast_silver_numeric('latitude') }}, 1) AS latitude,
        ROUND({{ cast_silver_numeric('longitude') }}, 1) AS longitude,
        {{ cast_silver_numeric('temperature_2m') }} AS temperature_2m,
        {{ cast_silver_numeric('total_precipitation') }} AS total_precipitation,
        {{ cast_silver_numeric('potential_evaporation') }} AS potential_evaporation,
        {{ cast_silver_timestamp('time') }} AS time,
        source_file_id,
        created_at
    FROM source
    ORDER BY ROUND({{ cast_silver_numeric('latitude') }}, 1), ROUND({{ cast_silver_numeric('longitude') }}, 1), {{ cast_silver_timestamp('time') }}, created_at DESC NULLS LAST
)

SELECT 
    *,
    {{ make_point('longitude', 'latitude') }} AS geometry
FROM deduplicated

