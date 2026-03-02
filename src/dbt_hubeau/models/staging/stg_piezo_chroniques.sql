{{
  config(
    materialized = 'incremental',
    unique_key = ['code_bss', 'date_mesure'],
    incremental_strategy = 'delete+insert',
    indexes = [
      {'columns': ['code_bss']},
      {'columns': ['date_mesure'], 'type': 'brin'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_bss', 'date_mesure']) }}",
      "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"
    ]
  )
}}

-- Staging model for piezometry chroniques
-- Source: bronze.piezometry_chroniques_raw
-- Silver: typage, déduplication, filtrage (mesure non nulle), sans colonnes DLT
-- Primary Key: (code_bss, date_mesure). FK: code_bss -> stg_piezo_stations(code_bss)
-- Incremental: par date avec fenêtre de rechargement configurable pour corrections tardives

{% set lookback_days = var('piezometry_incremental_lookback_days', 7) %}
{% set reprocess_from_date = var('piezometry_reprocess_from_date', none) %}

WITH source AS (
    SELECT * FROM {{ source('staging', 'piezometry_chroniques_raw') }}
    WHERE date_mesure IS NOT NULL
      AND code_bss IS NOT NULL
      -- Filtrage des valeurs nulles en silver : garder les lignes avec au moins une mesure utile
      AND ({{ cast_silver_numeric('niveau_nappe_eau') }} IS NOT NULL
           OR {{ cast_silver_numeric('profondeur_nappe') }} IS NOT NULL)
      {% if is_incremental() %}
      {% if reprocess_from_date is not none %}
      AND {{ cast_silver_date('date_mesure') }} >= '{{ reprocess_from_date }}'::date
      {% else %}
      AND {{ cast_silver_date('date_mesure') }} >= (
          SELECT COALESCE(MAX(date_mesure), '1900-01-01'::date)
                 - INTERVAL '{{ lookback_days }} days'
          FROM {{ this }}
      )
      {% endif %}
      {% endif %}
),

deduplicated AS (
    SELECT DISTINCT ON (code_bss, {{ cast_silver_date('date_mesure') }})
        {{ cast_silver_date('date_mesure') }} AS date_mesure,
        {{ cast_silver_numeric('niveau_nappe_eau') }} AS niveau_nappe_eau,
        {{ cast_silver_numeric('profondeur_nappe') }} AS profondeur_nappe,
        {{ cast_silver_text('code_bss') }} AS code_bss
    FROM source
    ORDER BY code_bss, {{ cast_silver_date('date_mesure') }}, {{ cast_silver_numeric('niveau_nappe_eau') }} DESC NULLS LAST
),

-- Ne garder que les chroniques dont la station existe (respect de la FK code_bss -> stg_piezo_stations)
stations AS (
    SELECT code_bss FROM {{ ref('stg_piezo_stations') }}
),

-- Validation: s'assurer que des stations existent avant de continuer
validation AS (
    SELECT COUNT(*) as station_count FROM stations
)

SELECT d.*
FROM deduplicated d
INNER JOIN stations s ON d.code_bss = s.code_bss
CROSS JOIN validation v
WHERE v.station_count > 0  -- Guard: returns 0 rows if stations table is empty (safe for incremental delete+insert)
