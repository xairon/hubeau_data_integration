-- Staging model for hydrometry stations
-- Source: bronze.hydrometry_stations_raw
-- Silver layer: autocast + filtrage des nulls

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_stations_raw') }}
)

SELECT
    code_station,
    libelle_station,
    code_site AS code_entite,  -- code_entite n'existe pas, utiliser code_site
    libelle_site AS libelle_entite,  -- libelle_entite n'existe pas, utiliser libelle_site
    code_site,
    libelle_site,
    longitude_station::numeric AS station_longitude,  -- utiliser longitude_station au lieu de x
    latitude_station::numeric AS station_latitude,    -- utiliser latitude_station au lieu de y
    code_commune_station AS code_commune_insee,  -- utiliser code_commune_station
    libelle_commune AS nom_commune,  -- utiliser libelle_commune
    code_departement,
    libelle_departement AS nom_departement,  -- utiliser libelle_departement
    code_region,
    libelle_region AS nom_region,  -- utiliser libelle_region
    altitude_ref_alti_station::numeric AS altitude_station,  -- utiliser altitude_ref_alti_station
    type_station AS type_entite,  -- utiliser type_station
    en_service AS statut_station,  -- utiliser en_service
    date_ouverture_station::date AS date_ouverture_station,
    date_fermeture_station::date AS date_fermeture_station
FROM source
WHERE latitude_station IS NOT NULL 
  AND longitude_station IS NOT NULL
  AND code_station IS NOT NULL
