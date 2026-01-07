WITH chroniques AS (
    SELECT * FROM {{ ref('stg_piezo_chroniques') }}
)

SELECT
    code_bss,
    date_mesure,
    AVG(niveau_nappe_eau) AS niveau_nappe_eau,
    AVG(profondeur_nappe) AS profondeur_nappe
FROM chroniques
GROUP BY code_bss, date_mesure
