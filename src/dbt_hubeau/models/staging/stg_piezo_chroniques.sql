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
      "{{ add_primary_key(['code_bss', 'date_mesure']) }}",
      "{{ convert_to_hypertable('date_mesure', '1 year') }}",
      "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"
    ]
  )
}}

-- Staging model for piezometry chroniques
-- Source: bronze.piezometry_chroniques_raw
-- Silver: typage, déduplication, filtrage (mesure non nulle), sans colonnes DLT
-- Primary Key: (code_bss, date_mesure). FK: code_bss -> stg_piezo_stations(code_bss)
-- Incremental: par date (nouvelles données uniquement)

WITH source AS (
    SELECT * FROM {{ source('staging', 'piezometry_chroniques_raw') }}
    WHERE date_mesure IS NOT NULL
      AND code_bss IS NOT NULL
      -- Filtrage des valeurs nulles en silver : ne garder que les lignes avec mesures utiles
      AND {{ cast_silver_numeric('niveau_nappe_eau') }} IS NOT NULL
      AND {{ cast_silver_numeric('profondeur_nappe') }} IS NOT NULL
      {% if is_incremental() %}
      AND {{ cast_silver_date('date_mesure') }} > (SELECT COALESCE(MAX(date_mesure), '1900-01-01'::date) FROM {{ this }})
      {% endif %}
),

deduplicated AS (
    SELECT DISTINCT ON (code_bss, {{ cast_silver_date('date_mesure') }})
        {{ cast_silver_date('date_mesure') }} AS date_mesure,
        {{ cast_silver_numeric('niveau_nappe_eau') }} AS niveau_nappe_eau,
        {{ cast_silver_numeric('profondeur_nappe') }} AS profondeur_nappe,
        {{ cast_silver_text('code_bss') }} AS code_bss,

        {{ dbt_utils.star(
            from=source('staging', 'piezometry_chroniques_raw'), 
            except=[
                "date_mesure", "niveau_nappe_eau", "profondeur_nappe", "code_bss",
                "_dlt_load_id", "_dlt_id"
            ]
        ) }}
    FROM source
    ORDER BY code_bss, {{ cast_silver_date('date_mesure') }}, niveau_nappe_eau DESC NULLS LAST
)

SELECT * FROM deduplicated
