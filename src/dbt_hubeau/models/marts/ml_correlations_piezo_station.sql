{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_bss'], 'unique': True}
    ],
    post_hook=[
      "{{ add_primary_key(['code_bss']) }}"
    ]
  )
}}

-- Corrélations stationnaires entre piézométrie et météo ERA5

SELECT
    code_bss,
    CORR(niveau_nappe_eau, temperature_2m) AS corr_niveau_temperature,
    CORR(niveau_nappe_eau, total_precipitation) AS corr_niveau_precipitation,
    CORR(niveau_nappe_eau, potential_evaporation) AS corr_niveau_evaporation
FROM {{ ref('hubeau_daily_chroniques') }}
GROUP BY code_bss
