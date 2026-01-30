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

-- Détection d'anomalies simple (z-score) pour l'hydrométrie
-- Source: hydro_daily_chroniques

WITH base AS (
    SELECT
        code_station,
        date,
        grandeur_hydro_elab,
        resultat_obs_elab
    FROM {{ ref('hydro_daily_chroniques') }}
),

stats AS (
    SELECT
        code_station,
        grandeur_hydro_elab,
        AVG(resultat_obs_elab) AS mean_resultat,
        STDDEV(resultat_obs_elab) AS std_resultat
    FROM base
    GROUP BY code_station, grandeur_hydro_elab
),

scored AS (
    SELECT
        b.code_station,
        b.date,
        b.grandeur_hydro_elab,
        b.resultat_obs_elab,
        s.mean_resultat,
        s.std_resultat,
        CASE
            WHEN s.std_resultat IS NULL OR s.std_resultat = 0 THEN NULL
            ELSE (b.resultat_obs_elab - s.mean_resultat) / s.std_resultat
        END AS z_resultat
    FROM base b
    INNER JOIN stats s
        ON b.code_station = s.code_station
       AND b.grandeur_hydro_elab = s.grandeur_hydro_elab
)

SELECT
    *,
    CASE WHEN z_resultat IS NOT NULL AND ABS(z_resultat) >= 3 THEN TRUE ELSE FALSE END AS is_anomaly
FROM scored
