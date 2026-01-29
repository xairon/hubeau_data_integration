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
-- Même critères que le staging, mais on garde les lignes qui NE passent PAS le filtre

WITH rejected AS (
    SELECT *
    FROM {{ source('staging', 'piezometry_chroniques_raw') }}
    WHERE date_mesure IS NULL
       OR code_bss IS NULL
       OR {{ cast_silver_numeric('niveau_nappe_eau') }} IS NULL
       OR {{ cast_silver_numeric('profondeur_nappe') }} IS NULL
      {% if is_incremental() %}
       AND _dlt_id NOT IN (SELECT _dlt_id FROM {{ this }})
      {% endif %}
)

SELECT
    *,
    CASE
        WHEN date_mesure IS NULL THEN 'DATE_MESURE_NULL'
        WHEN code_bss IS NULL THEN 'CODE_BSS_NULL'
        WHEN {{ cast_silver_numeric('niveau_nappe_eau') }} IS NULL THEN 'NIVEAU_NAPPE_NULL'
        WHEN {{ cast_silver_numeric('profondeur_nappe') }} IS NULL THEN 'PROFONDEUR_NAPPE_NULL'
        ELSE 'OTHER'
    END AS rejection_reason
FROM rejected
