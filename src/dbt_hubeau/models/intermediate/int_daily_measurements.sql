{{
  config(
    materialized = 'incremental',
    unique_key = ['code_bss', 'date_mesure'],
    incremental_strategy = 'delete+insert',
    incremental_predicates = [
      "DBT_INTERNAL_DEST.date_mesure >= CURRENT_DATE - INTERVAL '30 days'"
    ],
    indexes = [
      {'columns': ['code_bss']},
      {'columns': ['date_mesure'], 'type': 'brin'}
    ],
    post_hook = [
      "{{ add_primary_key(['code_bss', 'date_mesure']) }}",
      "{{ convert_to_hypertable('date_mesure', '1 year') }}",
      "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}",
      "{{ enable_compression(segment_by=['code_bss'], order_by='date_mesure DESC', compress_after='365 days') }}"
    ]
  )
}}

-- Mesures quotidiennes agrégées (moyenne par station × date)
-- Source: stg_piezo_chroniques. INCREMENTAL: ne traite que la fenêtre (lookback) pour le streaming nocturne.

{% set lookback_days = var('streaming_lookback_days', 7) %}

WITH chroniques AS (
    SELECT * FROM {{ ref('stg_piezo_chroniques') }}
    {% if is_incremental() %}
    WHERE date_mesure >= (
        SELECT COALESCE(MAX(date_mesure), '1900-01-01'::date) - INTERVAL '{{ lookback_days }} days'
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    code_bss::text AS code_bss,
    date_mesure::date AS date_mesure,
    (AVG(niveau_nappe_eau))::numeric AS niveau_nappe_eau,
    (AVG(profondeur_nappe))::numeric AS profondeur_nappe
FROM chroniques
GROUP BY code_bss, date_mesure
