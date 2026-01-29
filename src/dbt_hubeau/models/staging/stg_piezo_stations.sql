{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_bss'], 'unique': True},
      {'columns': ['code_departement']},
      {'columns': ['geometry'], 'type': 'gist'}
    ],
    post_hook=["{{ add_primary_key(['code_bss']) }}"]
  )
}}

-- Staging model for piezometry stations
-- Source: bronze.piezometry_stations_raw
-- Silver layer: copie bronze + typage + déduplication + PostGIS + suppression métadonnées dlt
-- Primary Key: code_bss

WITH source AS (
    SELECT * FROM {{ source('staging', 'piezometry_stations_raw') }}
    WHERE code_bss IS NOT NULL
      AND date_debut_mesure IS NOT NULL
      AND x IS NOT NULL 
      AND y IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT ON (code_bss)
        -- Champs castés explicitement (gère NULL / chaîne vide / littéral 'NULL')
        {{ cast_silver_numeric('x') }} AS x,
        {{ cast_silver_numeric('y') }} AS y,
        {{ cast_silver_numeric('altitude_station') }} AS altitude_station,
        {{ cast_silver_date('date_debut_mesure') }} AS date_debut_mesure,
        {{ cast_silver_date('date_fin_mesure') }} AS date_fin_mesure,
        {{ cast_silver_timestamp('date_maj') }} AS date_maj,
        {{ cast_silver_int('nb_mesures_piezo') }} AS nb_mesures_piezo,

        -- Colonnes texte (cast explicite pour éviter character varying NULL)
        {{ cast_silver_text('code_bss') }} AS code_bss,
        {{ cast_silver_text('code_commune_insee') }} AS code_commune_insee,
        {{ cast_silver_text('nom_commune') }} AS nom_commune,
        {{ cast_silver_text('code_departement') }} AS code_departement,
        {{ cast_silver_text('nom_departement') }} AS nom_departement,
        {{ cast_silver_text('codes_bdlisa') }} AS codes_bdlisa,
        {{ cast_silver_text('code_entite') }} AS code_entite,
        {{ cast_silver_text('code_masse_eau') }} AS code_masse_eau,

        {{ dbt_utils.star(
            from=source('staging', 'piezometry_stations_raw'),
            except=[
                "x", "y", "altitude_station",
                "date_debut_mesure", "date_fin_mesure", "date_maj", "nb_mesures_piezo",
                "code_bss", "code_commune_insee", "nom_commune", "code_departement", "nom_departement", "codes_bdlisa",
                "code_entite", "code_masse_eau",
                "_dlt_load_id", "_dlt_id"
            ]
        ) }}
    FROM source
    ORDER BY code_bss, date_maj DESC NULLS LAST
)

SELECT 
    *,
    -- PostGIS geometry column (SRID 4326 = WGS84)
    {{ make_point('x', 'y') }} AS geometry
FROM deduplicated
