{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_bss', 'date_mesure'], 'unique': True},
      {'columns': ['code_bss']},
      {'columns': ['date_mesure'], 'type': 'brin'}
    ],
    post_hook = [
      "{{ add_primary_key(['code_bss', 'date_mesure']) }}",
      "{{ convert_to_hypertable('date_mesure', '1 year') }}",
      "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"
    ]
  )
}}

-- Mesures quotidiennes agrégées (moyenne par station × date)
-- Source: stg_piezo_chroniques (filtrage des valeurs nulles fait en silver)

WITH chroniques AS (
    SELECT * FROM {{ ref('stg_piezo_chroniques') }}
)

SELECT
    code_bss::text AS code_bss,
    date_mesure::date AS date_mesure,
    (AVG(niveau_nappe_eau))::numeric AS niveau_nappe_eau,
    (AVG(profondeur_nappe))::numeric AS profondeur_nappe
FROM chroniques
GROUP BY code_bss, date_mesure
