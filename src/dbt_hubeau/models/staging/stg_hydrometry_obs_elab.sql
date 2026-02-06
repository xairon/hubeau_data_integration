{{
  config(
    materialized = 'incremental',
    unique_key = ['code_site', 'date_obs_elab', 'grandeur_hydro_elab'],
    incremental_strategy = 'delete+insert',
    indexes = [
      {'columns': ['code_site']},
      {'columns': ['date_obs_elab'], 'type': 'brin'},
      {'columns': ['geometry'], 'type': 'gist'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_site', 'date_obs_elab', 'grandeur_hydro_elab']) }}",
      "{{ convert_to_hypertable('date_obs_elab', '1 year') }}",
      "{{ add_foreign_key(['code_site'], 'stg_hydrometry_sites', ['code_site']) }}",
      "{{ enable_compression(segment_by=['grandeur_hydro_elab'], order_by='date_obs_elab DESC', compress_after='90 days') }}"
    ]
  )
}}

-- Staging model for hydrometry observations élaborées
-- Source: bronze.hydrometry_obs_elab_raw
-- Silver: typage, déduplication, filtrage (résultat non nul). Ne garde que les obs dont code_site existe dans stg_hydrometry_sites (FK).
-- Primary Key: (code_site, date_obs_elab, grandeur_hydro_elab). FK: code_site -> stg_hydrometry_sites(code_site)
-- Incremental: par date avec fenêtre de rechargement configurable pour corrections tardives

{% set lookback_days = var('hydrometry_incremental_lookback_days', 7) %}
{% set reprocess_from_date = var('hydrometry_reprocess_from_date', none) %}

WITH sites AS (
    SELECT code_site FROM {{ ref('stg_hydrometry_sites') }}
),

-- Validation: s'assurer que des sites existent avant de continuer
validation AS (
    SELECT COUNT(*) as site_count FROM sites
),

source AS (
    SELECT o.*
    FROM {{ source('staging', 'hydrometry_obs_elab_raw') }} o
    INNER JOIN sites ON o.code_site = sites.code_site
    CROSS JOIN validation v
    WHERE v.site_count > 0  -- CRITICAL: Fail if no sites (prevents silent data loss)
      AND o.date_obs_elab IS NOT NULL
      AND o.code_site IS NOT NULL
      AND o.grandeur_hydro_elab IS NOT NULL
      AND {{ cast_silver_numeric('o.resultat_obs_elab') }} IS NOT NULL
      {% if is_incremental() %}
      {% if reprocess_from_date is not none %}
      AND {{ cast_silver_date('o.date_obs_elab') }} >= '{{ reprocess_from_date }}'::date
      {% else %}
      AND {{ cast_silver_date('o.date_obs_elab') }} >= (
          SELECT COALESCE(MAX(date_obs_elab), '1900-01-01'::date)
                 - INTERVAL '{{ lookback_days }} days'
          FROM {{ this }}
      )
      {% endif %}
      {% endif %}
),

deduplicated AS (
    SELECT DISTINCT ON (code_site, {{ cast_silver_date('date_obs_elab') }}, grandeur_hydro_elab)
        {{ cast_silver_date('date_obs_elab') }} AS date_obs_elab,
        {{ cast_silver_numeric('resultat_obs_elab') }} AS resultat_obs_elab,
        {{ cast_silver_text('code_site') }} AS code_site,
        {{ cast_silver_text('grandeur_hydro_elab') }} AS grandeur_hydro_elab,
        {{ cast_silver_text('code_station') }} AS code_station,
        {{ cast_silver_timestamp('date_prod') }} AS date_prod,
        {{ cast_silver_text('code_statut') }} AS code_statut,
        {{ cast_silver_text('libelle_statut') }} AS libelle_statut,
        {{ cast_silver_text('code_methode') }} AS code_methode,
        {{ cast_silver_text('libelle_methode') }} AS libelle_methode,
        {{ cast_silver_text('code_qualification') }} AS code_qualification,
        {{ cast_silver_text('libelle_qualification') }} AS libelle_qualification,
        {{ cast_silver_numeric('longitude') }} AS longitude,
        {{ cast_silver_numeric('latitude') }} AS latitude
    FROM source
    ORDER BY code_site, {{ cast_silver_date('date_obs_elab') }}, grandeur_hydro_elab, resultat_obs_elab DESC NULLS LAST
),

with_geometry AS (
    SELECT d.*, s.geometry
    FROM deduplicated d
    LEFT JOIN {{ ref('stg_hydrometry_sites') }} s ON d.code_site = s.code_site
)

SELECT * FROM with_geometry
