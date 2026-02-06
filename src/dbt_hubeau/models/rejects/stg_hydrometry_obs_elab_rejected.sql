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
-- Tout ce qui ne va pas en silver : mesure nulle, clés nulles, ou code_site absent des sites

WITH sites AS (
    SELECT code_site FROM {{ ref('stg_hydrometry_sites') }}
),

rejected AS (
    SELECT o.*
    FROM {{ source('staging', 'hydrometry_obs_elab_raw') }} o
    WHERE (   o.date_obs_elab IS NULL
           OR o.code_site IS NULL
           OR o.grandeur_hydro_elab IS NULL
           OR {{ cast_silver_numeric('o.resultat_obs_elab') }} IS NULL
           OR NOT EXISTS (SELECT 1 FROM sites si WHERE si.code_site = o.code_site)
          )
      {% if is_incremental() %}
      AND NOT EXISTS (SELECT 1 FROM {{ this }} t WHERE t._dlt_id = o._dlt_id)
      {% endif %}
)

SELECT
    *,
    CASE
        WHEN code_site IS NULL THEN 'CODE_SITE_NULL'
        WHEN NOT EXISTS (SELECT 1 FROM sites si WHERE si.code_site = rejected.code_site) THEN 'CODE_SITE_NOT_IN_SITES'
        WHEN date_obs_elab IS NULL THEN 'DATE_OBS_ELAB_NULL'
        WHEN grandeur_hydro_elab IS NULL THEN 'GRANDEUR_HYDRO_NULL'
        WHEN {{ cast_silver_numeric('resultat_obs_elab') }} IS NULL THEN 'RESULTAT_OBS_NULL'
        ELSE 'OTHER'
    END AS rejection_reason
FROM rejected
