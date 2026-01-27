{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_station'], 'unique': True},
      {'columns': ['code_site']},
      {'columns': ['code_departement']},
      {'columns': ['geom'], 'type': 'gist'}
    ]
  )
}}

-- Dimension stations hydrométriques enrichie
-- Source: stg_hydrometry_stations

WITH stations AS (
    SELECT * FROM {{ ref('stg_hydrometry_stations') }}
),

-- On pourrait joindre ici avec des stats d'observations si on créait fct_monthly_hydro
-- Pour l'instant, c'est une dimension simple enrichie

metadata AS (
    SELECT
        code_station,
        libelle_station,
        code_site,
        libelle_site,
        code_cours_eau,
        nom_cours_eau,
        code_departement,
        nom_departement,
        date_ouverture_station,
        date_fermeture_station,
        longitude_station,
        latitude_station,
        geom,
        
        -- Statut
        CASE 
            WHEN date_fermeture_station IS NULL OR date_fermeture_station > CURRENT_DATE THEN 'ACTIVE'
            ELSE 'FERMEE'
        END AS statut_station
        
    FROM stations
)

SELECT * FROM metadata
