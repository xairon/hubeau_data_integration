-- Staging model for piezometry chroniques
-- Source: bronze.piezometry_chroniques_raw
-- Silver layer: autocast + filtrage des observations nulles

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
  AND code_bss IS NOT NULL
  -- Filtrer les lignes où toutes les observations sont nulles
  AND (
    niveau_nappe_eau IS NOT NULL 
    OR profondeur_nappe IS NOT NULL
  )
