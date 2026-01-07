WITH stations AS (
    SELECT * FROM {{ ref('stg_piezo_stations') }}
)

SELECT
    code_bss,
    -- Round to nearest 0.1° grid point
    ROUND(station_latitude * 10) / 10 AS era5_latitude,
    ROUND(station_longitude * 10) / 10 AS era5_longitude,
    station_latitude,
    station_longitude,
    urn_bdlisa,
    code_commune_insee,
    nom_commune,
    altitude_station,
    code_departement,
    nom_departement
FROM stations
WHERE station_latitude >= 41.0 AND station_latitude <= 51.5 
  AND station_longitude >= -5.5 AND station_longitude <= 10.0
