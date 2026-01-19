-- Staging model for hydrometry stations
-- Source: bronze.hydrometry_stations_raw
-- Silver layer: autocast + filtrage des nulls

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_stations_raw') }}
)

SELECT
    code_station,
    libelle_station,
    code_entite,
    libelle_entite,
    code_site,
    libelle_site,
    x::numeric AS station_longitude,
    y::numeric AS station_latitude,
    code_commune_insee,
    nom_commune,
    code_departement,
    nom_departement,
    code_region,
    nom_region,
    altitude_station::numeric AS altitude_station,
    type_entite,
    statut_station,
    date_ouverture_station::date AS date_ouverture_station,
    date_fermeture_station::date AS date_fermeture_station
FROM source
WHERE y IS NOT NULL 
  AND x IS NOT NULL
  AND code_station IS NOT NULL
