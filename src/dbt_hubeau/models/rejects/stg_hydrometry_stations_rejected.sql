{{
  config(
    materialized = 'incremental',
    unique_key = ['_dlt_id'],
    indexes = [
      {'columns': ['rejection_reason']},
      {'columns': ['code_site']},
      {'columns': ['code_station']}
    ],
    schema = 'silver_rejects'
  )
}}

-- Lignes rejetées par stg_hydrometry_stations (trace pour audit / qualité)
-- Tout ce qui ne va pas en silver : code_site absent des sites, ou clé/coords nulles

WITH sites AS (
    SELECT code_site FROM {{ ref('stg_hydrometry_sites') }}
),

rejected AS (
    SELECT s.*
    FROM {{ source('staging', 'hydrometry_stations_raw') }} s
    WHERE (   s.code_station IS NULL
           OR s.longitude_station IS NULL
           OR s.latitude_station IS NULL
           OR NOT EXISTS (SELECT 1 FROM sites si WHERE si.code_site = s.code_site)
          )
      {% if is_incremental() %}
      AND NOT EXISTS (SELECT 1 FROM {{ this }} t WHERE t._dlt_id = s._dlt_id)
      {% endif %}
)

SELECT
    *,
    CASE
        WHEN code_station IS NULL THEN 'CODE_STATION_NULL'
        WHEN NOT EXISTS (SELECT 1 FROM sites si WHERE si.code_site = rejected.code_site) THEN 'CODE_SITE_NOT_IN_SITES'
        WHEN longitude_station IS NULL OR latitude_station IS NULL THEN 'COORDS_NULL'
        ELSE 'OTHER'
    END AS rejection_reason
FROM rejected
