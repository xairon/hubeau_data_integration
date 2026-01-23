{{
  config(
    materialized = 'table',
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
-- Incremental: DÉSACTIVÉ (colonne _dlt_load_id manquante sur le serveur)
-- Primary Key: latitude + longitude + time

WITH source AS (
    SELECT * FROM {{ source('staging', 'era5_france_timeseries') }}
    WHERE time IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT ON (latitude::numeric, longitude::numeric, time)
        -- Champs castés explicitement
        latitude::numeric AS latitude,
        longitude::numeric AS longitude,
        temperature_2m::numeric AS temperature_2m,
        total_precipitation::numeric AS total_precipitation,
        potential_evaporation::numeric AS potential_evaporation,

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
