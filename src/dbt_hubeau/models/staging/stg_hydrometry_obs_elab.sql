{{
  config(
    materialized = 'incremental',
    unique_key = ['code_site', 'date_obs_elab', 'grandeur_hydro_elab'],
    indexes = [
      {'columns': ['code_site', 'date_obs_elab', 'grandeur_hydro_elab'], 'unique': True},
      {'columns': ['code_site']},
      {'columns': ['date_obs_elab'], 'type': 'brin'}
    ]
  )
}}

-- Staging model for hydrometry observations élaborées
-- Source: bronze.hydrometry_obs_elab_raw
-- Silver layer: copie bronze + typage + déduplication + suppression métadonnées dlt
-- Incremental: basé sur _dlt_load_id
-- Primary Key: code_site + date_obs_elab + grandeur_hydro_elab

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_obs_elab_raw') }}
    WHERE date_obs_elab IS NOT NULL
      AND code_site IS NOT NULL
      {% if is_incremental() %}
      AND _dlt_load_id > (SELECT MAX(_dlt_load_id) FROM {{ this }})
      {% endif %}
),

deduplicated AS (
    SELECT DISTINCT ON (code_site, date_obs_elab::date, grandeur_hydro_elab)
        date_obs_elab::date AS date_obs_elab,
        resultat_obs_elab::numeric AS resultat_obs_elab,
        _dlt_load_id,

        {{ dbt_utils.star(
            from=source('staging', 'hydrometry_obs_elab_raw'), 
            except=[
                "date_obs_elab",
                "resultat_obs_elab",
                "_dlt_load_id",
                "_dlt_id"
            ]
        ) }}
    FROM source
    ORDER BY code_site, date_obs_elab::date, grandeur_hydro_elab, resultat_obs_elab DESC NULLS LAST
)

SELECT * FROM deduplicated
