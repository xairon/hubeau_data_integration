{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['code_bss']},
      {'columns': ['era5_latitude', 'era5_longitude']}
    ]
  )
}}

-- Mapping spatial: Stations piézo → Grille ERA5 + Métadonnées TME

WITH stations AS (
    SELECT * FROM {{ ref('stg_piezo_stations') }}
),

tme AS (
    SELECT * FROM {{ ref('stg_tme_entites') }}
),

station_mapping AS (
    SELECT
        s.code_bss,
        -- Round to nearest 0.1° grid point for ERA5 matching
        ROUND(s.station_latitude * 10) / 10 AS era5_latitude,
        ROUND(s.station_longitude * 10) / 10 AS era5_longitude,
        s.station_latitude,
        s.station_longitude,
        s.urn_bdlisa,
        -- Extract CodeEH from urn_bdlisa (format: SAQ0000974AA → 974AA)
        CASE 
            WHEN s.urn_bdlisa IS NOT NULL AND LENGTH(s.urn_bdlisa) > 7 
            THEN SUBSTRING(s.urn_bdlisa FROM 8)
            ELSE NULL 
        END AS code_eh_extracted,
        s.code_commune_insee,
        s.nom_commune,
        s.altitude_station,
        s.code_departement,
        s.nom_departement
    FROM stations s
    WHERE s.station_latitude >= 41.0 AND s.station_latitude <= 51.5 
      AND s.station_longitude >= -5.5 AND s.station_longitude <= 10.0
)

SELECT
    sm.code_bss,
    sm.era5_latitude,
    sm.era5_longitude,
    sm.station_latitude,
    sm.station_longitude,
    sm.urn_bdlisa,
    sm.code_commune_insee,
    sm.nom_commune,
    sm.altitude_station,
    sm.code_departement,
    sm.nom_departement,
    -- TME metadata
    t.code_eh,
    t.libelle_eh,
    t.niveau_eh,
    t.etat_eh,
    t.nature_eh,
    t.milieu_eh,
    t.theme_eh,
    t.origine_eh
FROM station_mapping sm
LEFT JOIN tme t ON sm.code_eh_extracted = t.code_eh
