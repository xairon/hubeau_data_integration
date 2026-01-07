WITH source AS (
    SELECT * FROM {{ source('staging', 'piezometry_chroniques_raw') }}
)

SELECT
    code_bss,
    date_mesure::date AS date_mesure,
    niveau_nappe_eau::numeric AS niveau_nappe_eau,
    profondeur_nappe::numeric AS profondeur_nappe,
    mode_obtention,
    statut,
    qualification
FROM source
WHERE date_mesure IS NOT NULL
