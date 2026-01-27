{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_station'], 'unique': True},
      {'columns': ['code_site']},
      {'columns': ['code_departement']},
      {'columns': ['geometry'], 'type': 'gist'}
    ]
  )
}}

-- Staging model for hydrometry stations
-- Source: bronze.hydrometry_stations_raw
-- Silver layer: copie bronze + typage + déduplication + PostGIS + suppression métadonnées dlt
-- Primary Key: code_station

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_stations_raw') }}
    WHERE code_station IS NOT NULL
      -- AND date_debut_mesure IS NOT NULL -- Column missing in source on server
      AND longitude_station IS NOT NULL
      AND latitude_station IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT ON (code_station)
        -- Champs castés explicitement (pour assurer le bon type)
        date_ouverture_station::date AS date_ouverture_station,
        date_fermeture_station::date AS date_fermeture_station,
        date_maj_station::timestamp AS date_maj_station,
        longitude_station::numeric AS longitude_station,
        latitude_station::numeric AS latitude_station,
        altitude_ref_alti_station::numeric AS altitude_ref_alti_station,
        
        -- Sélection de tous les autres champs sauf ceux déjà castés et les métadonnées dlt
        {{ dbt_utils.star(
            from=source('staging', 'hydrometry_stations_raw'), 
            except=[
                "date_ouverture_station",
                "date_fermeture_station",
                "date_maj_station",
                "longitude_station",
                "latitude_station",
                "altitude_ref_alti_station",
                "_dlt_load_id",
                "_dlt_id"
            ]
        ) }}
    FROM source
    ORDER BY code_station, date_maj_station DESC NULLS LAST
)

SELECT 
    *,
    {{ make_point('longitude_station', 'latitude_station') }} AS geometry
FROM deduplicated
