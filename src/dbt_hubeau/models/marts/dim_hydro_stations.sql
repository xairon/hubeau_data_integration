{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_station'], 'unique': True},
      {'columns': ['code_site']},
      {'columns': ['code_departement']},
      {'columns': ['geometry'], 'type': 'gist'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_station']) }}",
      "{{ add_foreign_key(['code_station'], 'stg_hydrometry_stations', ['code_station']) }}",
      "{{ add_foreign_key(['code_site'], 'stg_hydrometry_sites', ['code_site']) }}"
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
        -- Hub'Eau fournit libelle_cours_eau / uri_cours_eau (pas "nom_cours_eau")
        libelle_cours_eau AS nom_cours_eau,
        uri_cours_eau,
        code_departement,
        -- Hub'Eau fournit libelle_departement (pas "nom_departement")
        libelle_departement AS nom_departement,
        date_ouverture_station,
        date_fermeture_station,
        longitude_station,
        latitude_station,
        geometry,
        
        -- Statut
        CASE 
            WHEN date_fermeture_station IS NULL OR date_fermeture_station > CURRENT_DATE THEN 'ACTIVE'
            ELSE 'FERMEE'
        END AS statut_station
        
    FROM stations
)

SELECT * FROM metadata
