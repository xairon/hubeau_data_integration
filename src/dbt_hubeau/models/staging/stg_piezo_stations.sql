{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_bss'], 'unique': True},
      {'columns': ['code_departement']},
      {'columns': ['geom'], 'type': 'gist'}
    ]
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
        -- Champs castés explicitement
        x::numeric AS x,
        y::numeric AS y,
        altitude_station::numeric AS altitude_station,
        date_debut_mesure::date AS date_debut_mesure,
        date_fin_mesure::date AS date_fin_mesure,
        date_maj::timestamp AS date_maj,
        nb_mesures_piezo::integer AS nb_mesures_piezo,

        -- Sélection de tous les autres champs sauf ceux déjà castés et les métadonnées dlt
        {{ dbt_utils.star(
            from=source('staging', 'piezometry_stations_raw'), 
            except=[
                "x",
                "y",
                "altitude_station",
                "date_debut_mesure",
                "date_fin_mesure",
                "date_maj",
                "nb_mesures_piezo",
                "_dlt_load_id",
                "_dlt_id"
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
