-- Staging model for hydrometry observations élaborées
-- Source: bronze.hydrometry_obs_elab_raw
-- Silver layer: autocast + filtrage des observations nulles

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_obs_elab_raw') }}
)

SELECT
    code_entite,
    date_obs_elab::date AS date_obs_elab,
    grandeur_hydro_elab,
    resultat_obs_elab::numeric AS resultat_obs_elab,
    code_qualification,
    libelle_qualification,
    code_statut_elab,
    libelle_statut_elab,
    code_methode_elab,
    libelle_methode_elab
FROM source
WHERE date_obs_elab IS NOT NULL
  AND resultat_obs_elab IS NOT NULL
  AND code_entite IS NOT NULL
