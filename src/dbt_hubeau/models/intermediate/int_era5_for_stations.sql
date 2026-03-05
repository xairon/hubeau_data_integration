{{
  config(
    materialized = 'incremental',
    unique_key = ['latitude', 'longitude', 'era5_date'],
    incremental_strategy = 'delete+insert',
    incremental_predicates = [
      time_range_delete_predicate('era5_date', '30 days')
    ],
    indexes = [
      {'columns': ['latitude', 'longitude']},
      {'columns': ['era5_date'], 'type': 'brin'}
    ],
    post_hook = [
      "{{ add_primary_key(['latitude', 'longitude', 'era5_date']) }}",
      "{{ add_foreign_key(['latitude', 'longitude'], 'int_era5_grid_points', ['era5_latitude', 'era5_longitude']) }}"
    ]
  )
}}

-- ERA5 filtré sur les points de grille utilisés par les stations piézo
-- Incremental: ne traite que les nouvelles dates ERA5
-- Filtrage: uniquement les 3 colonnes météo non nulles

WITH station_grid_points AS (
    SELECT DISTINCT
        era5_latitude AS latitude,
        era5_longitude AS longitude
    FROM {{ ref('int_station_era5_mapping') }}
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
    {% if is_incremental() %}
      AND e.time > (SELECT COALESCE(MAX(era5_date), '1900-01-01'::date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM filtered_era5
