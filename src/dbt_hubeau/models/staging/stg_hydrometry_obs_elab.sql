{{
  config(
    materialized = 'incremental',
    unique_key = ['code_site', 'date_obs_elab', 'grandeur_hydro_elab'],
    indexes = [
      {'columns': ['code_site', 'date_obs_elab', 'grandeur_hydro_elab'], 'unique': True},
      {'columns': ['code_site']},
      {'columns': ['date_obs_elab'], 'type': 'brin'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_site', 'date_obs_elab', 'grandeur_hydro_elab']) }}",
      "{{ convert_to_hypertable('date_obs_elab', '1 year') }}",
      "{{ add_foreign_key(['code_site'], 'stg_hydrometry_sites', ['code_site']) }}"
    ]
  )
}}

-- Staging model for hydrometry observations élaborées
-- Source: bronze.hydrometry_obs_elab_raw
-- Silver: typage, déduplication, filtrage (résultat non nul). Ne garde que les obs dont code_site existe dans stg_hydrometry_sites (FK).
-- Primary Key: (code_site, date_obs_elab, grandeur_hydro_elab). FK: code_site -> stg_hydrometry_sites(code_site)
-- Incremental: par date

WITH sites AS (
    SELECT code_site FROM {{ ref('stg_hydrometry_sites') }}
),

source AS (
    SELECT o.*
    FROM {{ source('staging', 'hydrometry_obs_elab_raw') }} o
    INNER JOIN sites ON o.code_site = sites.code_site
    WHERE o.date_obs_elab IS NOT NULL
      AND o.code_site IS NOT NULL
      AND o.grandeur_hydro_elab IS NOT NULL
      AND {{ cast_silver_numeric('o.resultat_obs_elab') }} IS NOT NULL
      {% if is_incremental() %}
      AND {{ cast_silver_date('o.date_obs_elab') }} > (SELECT COALESCE(MAX(date_obs_elab), '1900-01-01'::date) FROM {{ this }})
      {% endif %}
),

deduplicated AS (
    SELECT DISTINCT ON (code_site, {{ cast_silver_date('date_obs_elab') }}, grandeur_hydro_elab)
        {{ cast_silver_date('date_obs_elab') }} AS date_obs_elab,
        {{ cast_silver_numeric('resultat_obs_elab') }} AS resultat_obs_elab,
        {{ cast_silver_text('code_site') }} AS code_site,
        {{ cast_silver_text('grandeur_hydro_elab') }} AS grandeur_hydro_elab,

        {{ dbt_utils.star(
            from=source('staging', 'hydrometry_obs_elab_raw'), 
            except=[
                "date_obs_elab", "resultat_obs_elab", "code_site", "grandeur_hydro_elab",
                "_dlt_load_id", "_dlt_id"
            ]
        ) }}
    FROM source
    ORDER BY code_site, {{ cast_silver_date('date_obs_elab') }}, grandeur_hydro_elab, resultat_obs_elab DESC NULLS LAST
)

SELECT * FROM deduplicated
