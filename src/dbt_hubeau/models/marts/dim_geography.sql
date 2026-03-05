{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_commune']},
      {'columns': ['code_departement']},
      {'columns': ['code_region']}
    ]
  )
}}

-- Dimension géographique issue des stations piézo et hydro

WITH piezo AS (
    SELECT DISTINCT
        code_commune_insee AS code_commune,
        nom_commune AS nom_commune,
        code_departement,
        nom_departement,
        NULL::text AS code_region,
        NULL::text AS nom_region
    FROM {{ ref('stg_piezo_stations') }}
),

hydro AS (
    SELECT DISTINCT
        code_commune_site AS code_commune,
        libelle_commune AS nom_commune,
        code_departement,
        libelle_departement AS nom_departement,
        code_region,
        libelle_region AS nom_region
    FROM {{ ref('stg_hydrometry_sites') }}
),

combined AS (
    SELECT * FROM piezo
    UNION ALL
    SELECT * FROM hydro
)

SELECT
    code_commune,
    MAX(nom_commune) AS nom_commune,
    code_departement,
    MAX(nom_departement) AS nom_departement,
    MAX(code_region) AS code_region,
    MAX(nom_region) AS nom_region
FROM combined
WHERE code_departement IS NOT NULL
GROUP BY code_commune, code_departement
