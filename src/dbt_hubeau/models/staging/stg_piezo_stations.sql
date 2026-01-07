WITH source AS (
    SELECT * FROM {{ source('staging', 'piezometry_stations_raw') }}
)

SELECT
    code_bss,
    y::numeric AS station_latitude,
    x::numeric AS station_longitude,
    urns_bdlisa AS urn_bdlisa,
    code_commune_insee,
    nom_commune,
    altitude_station::numeric AS altitude_station,
    code_departement,
    nom_departement
FROM source
WHERE y IS NOT NULL AND x IS NOT NULL
