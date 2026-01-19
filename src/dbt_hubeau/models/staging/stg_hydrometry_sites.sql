-- Staging model for hydrometry sites
-- Source: bronze.hydrometry_sites_raw
-- Silver layer: autocast + filtrage des nulls

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_sites_raw') }}
)

SELECT
    code_site,
    libelle_site,
    code_commune_insee,
    nom_commune,
    code_departement,
    nom_departement,
    code_region,
    nom_region,
    x::numeric AS site_longitude,
    y::numeric AS site_latitude,
    altitude_site::numeric AS altitude_site
FROM source
WHERE code_site IS NOT NULL
