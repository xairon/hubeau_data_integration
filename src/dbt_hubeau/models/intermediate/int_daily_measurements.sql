WITH chroniques AS (
    SELECT * FROM {{ ref('stg_piezo_chroniques') }}
)

{{
  config(
    materialized = 'table',
    post_hook = [
      "{{ convert_to_hypertable('date_mesure', '1 year') }}"
    ]
  )
}}

SELECT
    code_bss,
    date_mesure,
    AVG(niveau_nappe_eau) AS niveau_nappe_eau,
    AVG(profondeur_nappe) AS profondeur_nappe
FROM chroniques
GROUP BY code_bss, date_mesure
-- S'assurer que les deux colonnes piézo sont non-nulles
HAVING AVG(niveau_nappe_eau) IS NOT NULL 
    AND AVG(profondeur_nappe) IS NOT NULL
