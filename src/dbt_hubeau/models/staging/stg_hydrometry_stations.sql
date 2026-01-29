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
      "{{ add_foreign_key(['code_site'], 'stg_hydrometry_sites', ['code_site']) }}"
    ]
  )
}}

-- Staging model for hydrometry stations
-- Source: bronze.hydrometry_stations_raw
-- Silver: typage, déduplication, PostGIS. Ne garde que les stations dont le code_site existe dans stg_hydrometry_sites (FK).
-- Primary Key: code_station. FK: code_site -> stg_hydrometry_sites(code_site)

WITH sites AS (
    SELECT code_site FROM {{ ref('stg_hydrometry_sites') }}
),

source AS (
    SELECT s.*
    FROM {{ source('staging', 'hydrometry_stations_raw') }} s
    INNER JOIN sites ON s.code_site = sites.code_site
    WHERE s.code_station IS NOT NULL
      AND s.longitude_station IS NOT NULL
      AND s.latitude_station IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT ON (code_station)
        -- Champs castés explicitement (gère NULL / chaîne vide / littéral 'NULL')
        {{ cast_silver_date('date_ouverture_station') }} AS date_ouverture_station,
        {{ cast_silver_date('date_fermeture_station') }} AS date_fermeture_station,
        {{ cast_silver_timestamp('date_maj_station') }} AS date_maj_station,
        {{ cast_silver_numeric('longitude_station') }} AS longitude_station,
        {{ cast_silver_numeric('latitude_station') }} AS latitude_station,
        {{ cast_silver_numeric('altitude_ref_alti_station') }} AS altitude_ref_alti_station,

        -- Colonnes texte
        {{ cast_silver_text('code_station') }} AS code_station,
        {{ cast_silver_text('code_site') }} AS code_site,
        {{ cast_silver_text('code_departement') }} AS code_departement,
        {{ cast_silver_text('libelle_station') }} AS libelle_station,
        {{ cast_silver_text('libelle_site') }} AS libelle_site,
        {{ cast_silver_text('code_cours_eau') }} AS code_cours_eau,
        {{ cast_silver_text('libelle_cours_eau') }} AS libelle_cours_eau,
        {{ cast_silver_text('uri_cours_eau') }} AS uri_cours_eau,
        {{ cast_silver_text('libelle_departement') }} AS libelle_departement
    FROM source
    ORDER BY code_station, date_maj_station DESC NULLS LAST
)

SELECT 
    *,
    {{ make_point('longitude_station', 'latitude_station') }} AS geometry
FROM deduplicated
