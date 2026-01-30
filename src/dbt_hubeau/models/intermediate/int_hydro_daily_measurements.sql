{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_station', 'date_obs_elab', 'grandeur_hydro_elab'], 'unique': True},
      {'columns': ['code_site']},
      {'columns': ['code_station']},
      {'columns': ['date_obs_elab'], 'type': 'brin'}
    ],
    post_hook = [
      "{{ add_primary_key(['code_station', 'date_obs_elab', 'grandeur_hydro_elab']) }}",
      "{{ convert_to_hypertable('date_obs_elab', '1 year') }}",
      "{{ add_foreign_key(['code_station'], 'stg_hydrometry_stations', ['code_station']) }}",
      "{{ add_foreign_key(['code_site'], 'stg_hydrometry_sites', ['code_site']) }}"
    ]
  )
}}

-- Mesures quotidiennes agrégées (hydrométrie)
-- Source: stg_hydrometry_obs_elab (filtrage des valeurs nulles fait en silver)

WITH observations AS (
    SELECT * FROM {{ ref('stg_hydrometry_obs_elab') }}
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
