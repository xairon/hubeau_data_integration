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

-- Détection d'anomalies simple (z-score) pour la piézométrie
-- Source: hubeau_daily_chroniques

WITH base AS (
    SELECT
        code_bss,
        date,
        niveau_nappe_eau
    FROM {{ ref('hubeau_daily_chroniques') }}
),

stats AS (
    SELECT
        code_bss,
        AVG(niveau_nappe_eau) AS mean_niveau,
        STDDEV(niveau_nappe_eau) AS std_niveau
    FROM base
    GROUP BY code_bss
),

scored AS (
    SELECT
        b.code_bss,
        b.date,
        b.niveau_nappe_eau,
        s.mean_niveau,
        s.std_niveau,
        CASE
            WHEN s.std_niveau IS NULL OR s.std_niveau = 0 THEN NULL
            ELSE (b.niveau_nappe_eau - s.mean_niveau) / s.std_niveau
        END AS z_niveau
    FROM base b
    INNER JOIN stats s ON b.code_bss = s.code_bss
)

SELECT
    *,
    CASE WHEN z_niveau IS NOT NULL AND ABS(z_niveau) >= 3 THEN TRUE ELSE FALSE END AS is_anomaly
FROM scored
