{{
  config(
    materialized = 'incremental',
    unique_key = ['_dlt_id'],
    indexes = [
      {'columns': ['rejection_reason']},
      {'columns': ['code_site']},
      {'columns': ['date_obs_elab'], 'type': 'brin'}
    ],
    schema = 'silver_rejects'
  )
}}

-- Lignes rejetées par stg_hydrometry_obs_elab (trace pour audit / qualité)
-- Même critères que le staging, mais on garde les lignes qui NE passent PAS le filtre

WITH rejected AS (
    SELECT *
    FROM {{ source('staging', 'hydrometry_obs_elab_raw') }}
    WHERE date_obs_elab IS NULL
       OR code_site IS NULL
       OR grandeur_hydro_elab IS NULL
       OR {{ cast_silver_numeric('resultat_obs_elab') }} IS NULL
      {% if is_incremental() %}
       AND _dlt_id NOT IN (SELECT _dlt_id FROM {{ this }})
      {% endif %}
)

SELECT
    *,
    CASE
        WHEN date_obs_elab IS NULL THEN 'DATE_OBS_ELAB_NULL'
        WHEN code_site IS NULL THEN 'CODE_SITE_NULL'
        WHEN grandeur_hydro_elab IS NULL THEN 'GRANDEUR_HYDRO_NULL'
        WHEN {{ cast_silver_numeric('resultat_obs_elab') }} IS NULL THEN 'RESULTAT_OBS_NULL'
        ELSE 'OTHER'
    END AS rejection_reason
FROM rejected
