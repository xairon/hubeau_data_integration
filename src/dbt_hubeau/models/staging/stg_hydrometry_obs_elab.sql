{{
  config(
    materialized = 'table',
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
-- Primary Key: code_site + date_obs_elab + grandeur_hydro_elab

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_obs_elab_raw') }}
    WHERE date_obs_elab IS NOT NULL
      AND code_site IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT ON (code_site, date_obs_elab::date, grandeur_hydro_elab)
        -- Champs castés explicitement
        date_obs_elab::date AS date_obs_elab,
        resultat_obs_elab::numeric AS resultat_obs_elab,

        -- Sélection de tous les autres champs sauf ceux déjà castés et les métadonnées dlt
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
