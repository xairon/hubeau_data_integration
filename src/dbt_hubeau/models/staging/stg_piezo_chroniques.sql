{{
  config(
    materialized = 'incremental',
    unique_key = ['code_bss', 'date_mesure'],
    indexes = [
      {'columns': ['code_bss', 'date_mesure'], 'unique': True},
      {'columns': ['code_bss']},
      {'columns': ['date_mesure'], 'type': 'brin'}
    ],
    post_hook=[
      "{{ convert_to_hypertable('date_mesure', '1 year') }}"
    ]
  )
}}

-- Staging model for piezometry chroniques
-- Source: bronze.piezometry_chroniques_raw
-- Silver layer: copie bronze + typage + déduplication + suppression métadonnées dlt
-- Incremental: basé sur _dlt_load_id
-- Primary Key: code_bss + date_mesure

WITH source AS (
    SELECT * FROM {{ source('staging', 'piezometry_chroniques_raw') }}
    WHERE date_mesure IS NOT NULL
      AND code_bss IS NOT NULL
      {% if is_incremental() %}
      AND _dlt_load_id > (SELECT MAX(_dlt_load_id) FROM {{ this }})
      {% endif %}
),

deduplicated AS (
    SELECT DISTINCT ON (code_bss, date_mesure::date)
        date_mesure::date AS date_mesure,
        niveau_nappe_eau::numeric AS niveau_nappe_eau,
        profondeur_nappe::numeric AS profondeur_nappe,
        _dlt_load_id,

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
