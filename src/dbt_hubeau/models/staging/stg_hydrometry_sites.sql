{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_site'], 'unique': True},
      {'columns': ['code_departement']},
      {'columns': ['geom'], 'type': 'gist'}
    ]
  )
}}

-- Staging model for hydrometry sites
-- Source: bronze.hydrometry_sites_raw
-- Silver layer: copie bronze + typage + déduplication + PostGIS + suppression métadonnées dlt
-- Primary Key: code_site

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_sites_raw') }}
    WHERE code_site IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT ON (code_site)
        -- Champs castés explicitement
        longitude_site::numeric AS longitude_site,
        latitude_site::numeric AS latitude_site,
        altitude_site::numeric AS altitude_site,

        -- Sélection de tous les autres champs sauf ceux déjà castés et les métadonnées dlt
        {{ dbt_utils.star(
            from=source('staging', 'hydrometry_sites_raw'), 
            except=[
                "longitude_site",
                "latitude_site",
                "altitude_site",
                "_dlt_load_id",
                "_dlt_id"
            ]
        ) }}
    FROM source
    ORDER BY code_site
)

SELECT 
    *,
    {{ make_point('longitude_site', 'latitude_site') }} AS geom
FROM deduplicated
