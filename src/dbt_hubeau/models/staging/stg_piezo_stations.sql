-- Staging model for piezometry stations
-- Source: bronze.piezometry_stations_raw
-- Silver layer: autocast + filtrage des nulls

WITH source AS (
    SELECT * FROM {{ source('staging', 'piezometry_stations_raw') }}
)

SELECT
    code_bss,
    y::numeric AS station_latitude,
    x::numeric AS station_longitude,
    codes_bdlisa,
    code_commune_insee,
    nom_commune,
    altitude_station::numeric AS altitude_station,
    code_departement,
    nom_departement
FROM source
WHERE y IS NOT NULL AND x IS NOT NULL
