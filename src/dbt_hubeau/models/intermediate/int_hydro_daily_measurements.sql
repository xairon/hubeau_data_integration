{{
  config(
    materialized = 'incremental',
    unique_key = ['code_station', 'date_obs_elab', 'grandeur_hydro_elab'],
    incremental_strategy = 'delete+insert',
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
),

-- Filtre les obs dont code_station n'est pas (encore) dans le référentiel stations.
-- L'API Hub'Eau peut retourner des obs pour des stations absentes de l'endpoint stations ;
-- self-healing : ré-agrégées quand stations rattrape (fenêtre lookback).
valid_stations AS (
    SELECT code_station FROM {{ ref('stg_hydrometry_stations') }}
)

SELECT
    o.code_site::text AS code_site,
    o.code_station::text AS code_station,
    o.date_obs_elab::date AS date_obs_elab,
    o.grandeur_hydro_elab::text AS grandeur_hydro_elab,
    (AVG(o.resultat_obs_elab))::numeric AS resultat_obs_elab,
    COUNT(*)::integer AS nb_obs
FROM observations o
INNER JOIN valid_stations s ON o.code_station = s.code_station
GROUP BY o.code_site, o.code_station, o.date_obs_elab, o.grandeur_hydro_elab
