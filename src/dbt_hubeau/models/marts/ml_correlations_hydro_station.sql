{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_station', 'grandeur_hydro_elab'], 'unique': True}
    ],
    post_hook=[
      "{{ add_primary_key(['code_station', 'grandeur_hydro_elab']) }}"
    ]
  )
}}

-- Corrélations stationnaires hydrométrie ↔ météo ERA5

SELECT
    code_station,
    grandeur_hydro_elab,
    CORR(resultat_obs_elab, temperature_2m) AS corr_resultat_temperature,
    CORR(resultat_obs_elab, total_precipitation) AS corr_resultat_precipitation,
    CORR(resultat_obs_elab, potential_evaporation) AS corr_resultat_evaporation
FROM {{ ref('hydro_daily_chroniques') }}
GROUP BY code_station, grandeur_hydro_elab
