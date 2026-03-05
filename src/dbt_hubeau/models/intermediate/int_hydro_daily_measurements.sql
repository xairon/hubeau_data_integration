{{
  config(
    materialized = 'incremental',
    unique_key = ['code_station', 'date_obs_elab', 'grandeur_hydro_elab'],
    incremental_strategy = 'delete+insert',
    incremental_predicates = [
      time_range_delete_predicate('date_obs_elab', '30 days')
    ],
    indexes = [
      {'columns': ['code_site']},
      {'columns': ['code_station']},
      {'columns': ['date_obs_elab'], 'type': 'brin'}
    ],
    post_hook = [
      "{{ add_primary_key(['code_station', 'date_obs_elab', 'grandeur_hydro_elab']) }}",
      "{{ add_foreign_key(['code_station'], 'stg_hydrometry_stations', ['code_station']) }}",
      "{{ add_foreign_key(['code_site'], 'stg_hydrometry_sites', ['code_site']) }}"
    ]
  )
}}

-- Mesures quotidiennes agrégées (hydrométrie)
-- Source: stg_hydrometry_obs_elab (filtrage des valeurs nulles fait en silver)
-- INCREMENTAL: ne traite que la fenêtre (lookback) pour le streaming nocturne.

{% set lookback_days = var('streaming_lookback_days', 7) %}

WITH observations AS (
    SELECT * FROM {{ ref('stg_hydrometry_obs_elab') }}
    {% if is_incremental() %}
    WHERE date_obs_elab >= (
        SELECT COALESCE(MAX(date_obs_elab), '1900-01-01'::date) - INTERVAL '{{ lookback_days }} days'
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    code_site::text AS code_site,
    code_station::text AS code_station,
    date_obs_elab::date AS date_obs_elab,
    grandeur_hydro_elab::text AS grandeur_hydro_elab,
    (AVG(resultat_obs_elab))::numeric AS resultat_obs_elab,
    COUNT(*)::integer AS nb_obs
FROM observations
WHERE code_station IS NOT NULL
GROUP BY code_site, code_station, date_obs_elab, grandeur_hydro_elab
