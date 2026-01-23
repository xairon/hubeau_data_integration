{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_bss', 'date_mesure'], 'unique': True},
      {'columns': ['code_bss']},
      {'columns': ['date_mesure'], 'type': 'brin'}
    ]
  )
}}

-- Staging model for piezometry chroniques
-- Source: bronze.piezometry_chroniques_raw
-- Silver layer: copie bronze + typage + déduplication + suppression métadonnées dlt
-- Primary Key: code_bss + date_mesure

WITH source AS (
    SELECT * FROM {{ source('staging', 'piezometry_chroniques_raw') }}
    WHERE date_mesure IS NOT NULL
      AND code_bss IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT ON (code_bss, date_mesure::date)
        -- Champs castés explicitement
        date_mesure::date AS date_mesure,
        niveau_nappe_eau::numeric AS niveau_nappe_eau,
        profondeur_nappe::numeric AS profondeur_nappe,

        -- Sélection de tous les autres champs sauf ceux déjà castés et les métadonnées dlt
        {{ dbt_utils.star(
            from=source('staging', 'piezometry_chroniques_raw'), 
            except=[
                "date_mesure",
                "niveau_nappe_eau",
                "profondeur_nappe",
                "_dlt_load_id",
                "_dlt_id"
            ]
        ) }}
    FROM source
    ORDER BY code_bss, date_mesure::date, niveau_nappe_eau DESC NULLS LAST
)

SELECT * FROM deduplicated
