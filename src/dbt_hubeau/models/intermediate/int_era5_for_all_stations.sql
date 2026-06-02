{{
  config(
    materialized = 'incremental',
    unique_key = ['latitude', 'longitude', 'era5_date'],
    incremental_strategy = 'delete+insert',
    incremental_predicates = [
      time_range_delete_predicate('era5_date', '30 days')
    ],
    indexes = [
      {'columns': ['era5_date'], 'type': 'brin'}
    ],
    post_hook = [
      "{{ add_primary_key(['latitude', 'longitude', 'era5_date']) }}",
      "{{ add_foreign_key(['latitude', 'longitude'], 'int_era5_grid_points', ['era5_latitude', 'era5_longitude']) }}"
    ]
  )
}}

-- ERA5 filtré sur les points de grille utilisés par TOUTES les stations (piézo + hydro).
-- Union des grid points des deux mappings pour éviter la duplication de données ERA5.
-- Incremental: ne traite que les nouvelles dates ERA5.

WITH all_station_grid_points AS (
    SELECT DISTINCT
        era5_latitude AS latitude,
        era5_longitude AS longitude
    FROM {{ ref('int_station_era5_mapping') }}

    UNION

    SELECT DISTINCT
        era5_latitude AS latitude,
        era5_longitude AS longitude
    FROM {{ ref('int_hydro_station_era5_mapping') }}
),

filtered_era5 AS (
    -- Arrondi 0.1° à la lecture de silver : agrège les variantes flottantes d'une même
    -- cellule (48.1 historique + 48.09999999999995 incrémental) sous une seule coord,
    -- de sorte que les stations récupèrent tout l'historique. DISTINCT ON protège la PK
    -- au cas où deux variantes partageraient une date. cf. mémoire era5-coordinate-precision-bug.
    SELECT DISTINCT ON (ROUND(e.latitude::numeric, 1), ROUND(e.longitude::numeric, 1), e.time::date)
        ROUND(e.latitude::numeric, 1) AS latitude,
        ROUND(e.longitude::numeric, 1) AS longitude,
        e.time::date AS era5_date,
        e.temperature_2m::numeric AS temperature_2m,
        e.total_precipitation::numeric AS total_precipitation,
        e.potential_evaporation::numeric AS potential_evaporation
    FROM {{ ref('stg_era5_timeseries') }} e
    INNER JOIN all_station_grid_points g
        ON ROUND(e.latitude::numeric, 1) = g.latitude
        AND ROUND(e.longitude::numeric, 1) = g.longitude
    WHERE e.temperature_2m IS NOT NULL
      AND e.total_precipitation IS NOT NULL
      AND e.potential_evaporation IS NOT NULL
    {% if is_incremental() %}
      AND e.time > (SELECT COALESCE(MAX(era5_date), '1900-01-01'::date) FROM {{ this }})
    {% endif %}
    ORDER BY ROUND(e.latitude::numeric, 1), ROUND(e.longitude::numeric, 1), e.time::date
)

SELECT * FROM filtered_era5
