-- Staging model for hydrometry sites
-- Source: bronze.hydrometry_sites_raw
-- Silver layer: autocast + filtrage des nulls

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_sites_raw') }}
)

SELECT
    code_site,
    libelle_site,
    code_commune_site AS code_commune_insee,  -- utiliser code_commune_site
    libelle_commune AS nom_commune,  -- utiliser libelle_commune
    code_departement,
    libelle_departement AS nom_departement,  -- utiliser libelle_departement
    code_region,
    libelle_region AS nom_region,  -- utiliser libelle_region
    longitude_site::numeric AS site_longitude,  -- utiliser longitude_site au lieu de x
    latitude_site::numeric AS site_latitude,    -- utiliser latitude_site au lieu de y
    altitude_site::numeric AS altitude_site
FROM source
WHERE code_site IS NOT NULL
