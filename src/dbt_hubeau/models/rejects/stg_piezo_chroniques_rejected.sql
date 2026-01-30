{{
  config(
    materialized = 'incremental',
    unique_key = ['_dlt_id'],
    indexes = [
      {'columns': ['rejection_reason']},
      {'columns': ['code_bss']},
      {'columns': ['date_mesure'], 'type': 'brin'}
    ],
    schema = 'silver_rejects'
  )
}}

-- Lignes rejetées par stg_piezo_chroniques (trace pour audit / qualité)
-- 1) Données invalides (date/code_bss/mesure nuls)
-- 2) Chroniques dont la station n'existe pas dans stg_piezo_stations (STATION_INEXISTANTE)

WITH stations AS (
    SELECT code_bss FROM {{ ref('stg_piezo_stations') }}
),

-- Rejets pour données nulles ou invalides
rejected_nulls AS (
    SELECT *
    FROM {{ source('staging', 'piezometry_chroniques_raw') }}
    WHERE (   date_mesure IS NULL
           OR code_bss IS NULL
           OR {{ cast_silver_numeric('niveau_nappe_eau') }} IS NULL
           OR {{ cast_silver_numeric('profondeur_nappe') }} IS NULL
          )
      {% if is_incremental() %}
      AND _dlt_id NOT IN (SELECT _dlt_id FROM {{ this }})
      {% endif %}
),

-- Rejets pour station inexistante (données valides mais code_bss absent de stg_piezo_stations)
rejected_station_unknown AS (
    SELECT r.*
    FROM {{ source('staging', 'piezometry_chroniques_raw') }} r
    WHERE date_mesure IS NOT NULL
      AND code_bss IS NOT NULL
      AND {{ cast_silver_numeric('niveau_nappe_eau') }} IS NOT NULL
      AND {{ cast_silver_numeric('profondeur_nappe') }} IS NOT NULL
      AND {{ cast_silver_text('code_bss') }} NOT IN (SELECT code_bss FROM stations)
      {% if is_incremental() %}
      AND r._dlt_id NOT IN (SELECT _dlt_id FROM {{ this }})
      {% endif %}
),

rejected_with_reason AS (
    SELECT
        *,
        CASE
            WHEN date_mesure IS NULL THEN 'DATE_MESURE_NULL'
            WHEN code_bss IS NULL THEN 'CODE_BSS_NULL'
            WHEN {{ cast_silver_numeric('niveau_nappe_eau') }} IS NULL THEN 'NIVEAU_NAPPE_NULL'
            WHEN {{ cast_silver_numeric('profondeur_nappe') }} IS NULL THEN 'PROFONDEUR_NAPPE_NULL'
            ELSE 'OTHER'
        END AS rejection_reason
    FROM rejected_nulls
),
rejected_station_with_reason AS (
    SELECT *, 'STATION_INEXISTANTE' AS rejection_reason FROM rejected_station_unknown
)

SELECT * FROM rejected_with_reason
UNION ALL
SELECT * FROM rejected_station_with_reason
