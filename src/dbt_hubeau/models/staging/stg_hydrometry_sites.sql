{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_site'], 'unique': True},
      {'columns': ['code_departement']},
      {'columns': ['geometry'], 'type': 'gist'}
    ],
    post_hook=["{{ add_primary_key(['code_site']) }}"]
  )
}}

-- Staging model for hydrometry sites
-- Source: bronze.hydrometry_sites_raw
-- Silver layer: copie bronze + typage + déduplication + PostGIS + suppression métadonnées dlt
-- Primary Key: code_site

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_sites_raw') }}
    WHERE code_site IS NOT NULL
      AND longitude_site IS NOT NULL
      AND latitude_site IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT ON (code_site)
        -- Champs castés explicitement (gère NULL / chaîne vide / littéral 'NULL')
        {{ cast_silver_numeric('longitude_site') }} AS longitude_site,
        {{ cast_silver_numeric('latitude_site') }} AS latitude_site,
        {{ cast_silver_numeric('altitude_site') }} AS altitude_site,

        -- Colonnes texte
        {{ cast_silver_text('code_site') }} AS code_site,
        {{ cast_silver_text('code_departement') }} AS code_departement,

        -- Autres champs
        {{ dbt_utils.star(
            from=source('staging', 'hydrometry_sites_raw'), 
            except=[
                "longitude_site", "latitude_site", "altitude_site",
                "code_site", "code_departement",
                "_dlt_load_id", "_dlt_id"
            ]
        ) }}
    FROM source
    ORDER BY code_site
)

SELECT 
    *,
    {{ make_point('longitude_site', 'latitude_site') }} AS geometry
FROM deduplicated
