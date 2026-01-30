{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_bss', 'date'], 'unique': True},
      {'columns': ['code_bss']},
      {'columns': ['date'], 'type': 'brin'}
    ],
    post_hook=[
      "{{ add_primary_key(['code_bss', 'date']) }}"
    ]
  )
}}

-- Features ML journalières pour la piézométrie
-- Source: hubeau_daily_chroniques

WITH base AS (
    SELECT * FROM {{ ref('hubeau_daily_chroniques') }}
),

features AS (
    SELECT
        code_bss,
        date,
        niveau_nappe_eau,
        profondeur_nappe,
        temperature_2m,
        total_precipitation,
        potential_evaporation,
        code_departement,
        code_eh,

        -- Lags
        LAG(niveau_nappe_eau, 1) OVER w AS niveau_lag_1d,
        LAG(niveau_nappe_eau, 7) OVER w AS niveau_lag_7d,
        LAG(niveau_nappe_eau, 30) OVER w AS niveau_lag_30d,
        LAG(total_precipitation, 1) OVER w AS precip_lag_1d,
        LAG(total_precipitation, 7) OVER w AS precip_lag_7d,
        LAG(total_precipitation, 30) OVER w AS precip_lag_30d,

        -- Rolling windows
        AVG(niveau_nappe_eau) OVER w_7d AS niveau_roll_7d,
        AVG(niveau_nappe_eau) OVER w_30d AS niveau_roll_30d,
        AVG(total_precipitation) OVER w_7d AS precip_roll_7d,
        AVG(total_precipitation) OVER w_30d AS precip_roll_30d,

        -- Deltas
        niveau_nappe_eau - LAG(niveau_nappe_eau, 1) OVER w AS niveau_delta_1d,
        niveau_nappe_eau - LAG(niveau_nappe_eau, 7) OVER w AS niveau_delta_7d,

        -- Calendrier
        EXTRACT(YEAR FROM date)::integer AS year,
        EXTRACT(MONTH FROM date)::integer AS month,
        EXTRACT(DOY FROM date)::integer AS day_of_year,
        EXTRACT(ISODOW FROM date)::integer AS iso_day_of_week

    FROM base
    WINDOW
        w AS (PARTITION BY code_bss ORDER BY date),
        w_7d AS (PARTITION BY code_bss ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        w_30d AS (PARTITION BY code_bss ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
)

SELECT * FROM features
