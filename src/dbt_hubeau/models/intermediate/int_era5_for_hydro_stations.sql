{% set enable_compress = var('enable_timescale_compression', false) %}
{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['latitude', 'longitude']},
      {'columns': ['era5_date'], 'type': 'brin'}
    ],
    post_hook = [
      "{{ add_primary_key(['latitude', 'longitude', 'era5_date']) }}",
      "{{ convert_to_hypertable('era5_date', '1 year') }}",
      "{{ add_foreign_key(['latitude', 'longitude'], 'int_era5_grid_points', ['era5_latitude', 'era5_longitude']) }}"
    ] + (["{{ enable_compression(segment_by=[], order_by='era5_date DESC', compress_after='365 days') }}"] if enable_compress else [])
  )
}}

-- ERA5 filtré sur les points de grille utilisés par les stations hydrométriques
-- Filtrage: uniquement les 3 colonnes météo non nulles

WITH station_grid_points AS (
    SELECT DISTINCT
        era5_latitude AS latitude,
        era5_longitude AS longitude
    FROM {{ ref('int_hydro_station_era5_mapping') }}
),

filtered_era5 AS (
    SELECT
        e.latitude::numeric AS latitude,
        e.longitude::numeric AS longitude,
        e.time::date AS era5_date,
        e.temperature_2m::numeric AS temperature_2m,
        e.total_precipitation::numeric AS total_precipitation,
        e.potential_evaporation::numeric AS potential_evaporation
    FROM {{ ref('stg_era5_timeseries') }} e
    INNER JOIN station_grid_points g
        ON e.latitude = g.latitude
        AND e.longitude = g.longitude
    WHERE e.temperature_2m IS NOT NULL
      AND e.total_precipitation IS NOT NULL
      AND e.potential_evaporation IS NOT NULL
)

SELECT * FROM filtered_era5
