{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['code_bss', 'date']},
      {'columns': ['date']},
      {'columns': ['code_departement']}
    ]
  )
}}

-- Table finale : Jointure optimisée Piezo + ERA5 (filtré aux stations)

WITH measurements AS (
    SELECT * FROM {{ ref('int_daily_measurements') }}
),

mapping AS (
    SELECT * FROM {{ ref('int_station_era5_mapping') }}
),

era5_filtered AS (
    -- Utilise la table ERA5 PRE-FILTREE (beaucoup plus petite)
    SELECT * FROM {{ ref('int_era5_for_stations') }}
),

final AS (
    SELECT
        m.code_bss,
        m.date_mesure AS date,
        
        -- Piezo data
        m.niveau_nappe_eau,
        m.profondeur_nappe,
        
        -- ERA5 data
        e.temperature_2m,
        e.total_precipitation,
        e.potential_evaporation,
        
        -- Station metadata
        map.urn_bdlisa,
        map.code_commune_insee,
        map.nom_commune,
        map.altitude_station,
        map.code_departement,
        map.nom_departement,
        
        -- Technical columns
        map.era5_latitude,
        map.era5_longitude
        
    FROM measurements m
    INNER JOIN mapping map ON m.code_bss = map.code_bss
    LEFT JOIN era5_filtered e 
        ON map.era5_latitude = e.latitude 
        AND map.era5_longitude = e.longitude
        AND m.date_mesure = e.era5_date
)

SELECT * FROM final
