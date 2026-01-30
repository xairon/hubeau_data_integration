{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_station', 'date', 'grandeur_hydro_elab'], 'unique': True},
      {'columns': ['code_station']},
      {'columns': ['date'], 'type': 'brin'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_station', 'date', 'grandeur_hydro_elab']) }}"
    ]
  )
}}

-- Features ML journalières pour l'hydrométrie
-- Source: hydro_daily_chroniques

WITH base AS (
    SELECT * FROM {{ ref('hydro_daily_chroniques') }}
),

features AS (
    SELECT
        code_station,
        code_site,
        date,
        grandeur_hydro_elab,
        resultat_obs_elab,
        temperature_2m,
        total_precipitation,
        potential_evaporation,
        code_departement,

        -- Lags
        LAG(resultat_obs_elab, 1) OVER w AS resultat_lag_1d,
        LAG(resultat_obs_elab, 7) OVER w AS resultat_lag_7d,
        LAG(resultat_obs_elab, 30) OVER w AS resultat_lag_30d,
        LAG(total_precipitation, 1) OVER w AS precip_lag_1d,
        LAG(total_precipitation, 7) OVER w AS precip_lag_7d,
        LAG(total_precipitation, 30) OVER w AS precip_lag_30d,

        -- Rolling windows
        AVG(resultat_obs_elab) OVER w_7d AS resultat_roll_7d,
        AVG(resultat_obs_elab) OVER w_30d AS resultat_roll_30d,
        AVG(total_precipitation) OVER w_7d AS precip_roll_7d,
        AVG(total_precipitation) OVER w_30d AS precip_roll_30d,

        -- Deltas
        resultat_obs_elab - LAG(resultat_obs_elab, 1) OVER w AS resultat_delta_1d,
        resultat_obs_elab - LAG(resultat_obs_elab, 7) OVER w AS resultat_delta_7d,

        -- Calendrier
        EXTRACT(YEAR FROM date)::integer AS year,
        EXTRACT(MONTH FROM date)::integer AS month,
        EXTRACT(DOY FROM date)::integer AS day_of_year,
        EXTRACT(ISODOW FROM date)::integer AS iso_day_of_week

    FROM base
    WINDOW
        w AS (PARTITION BY code_station, grandeur_hydro_elab ORDER BY date),
        w_7d AS (PARTITION BY code_station, grandeur_hydro_elab ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        w_30d AS (PARTITION BY code_station, grandeur_hydro_elab ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
)

SELECT * FROM features
