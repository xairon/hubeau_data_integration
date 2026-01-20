-- Staging model for hydrometry observations élaborées
-- Source: bronze.hydrometry_obs_elab_raw
-- Silver layer: autocast + filtrage des observations nulles

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_obs_elab_raw') }}
)

SELECT
    code_site AS code_entite,  -- code_entite n'existe pas, utiliser code_site
    date_obs_elab::date AS date_obs_elab,
    grandeur_hydro_elab,
    resultat_obs_elab::numeric AS resultat_obs_elab,
    code_qualification,
    libelle_qualification,
    code_statut AS code_statut_elab,  -- utiliser code_statut
    libelle_statut AS libelle_statut_elab,  -- utiliser libelle_statut
    code_methode AS code_methode_elab,  -- utiliser code_methode
    libelle_methode AS libelle_methode_elab  -- utiliser libelle_methode
FROM source
WHERE date_obs_elab IS NOT NULL
  AND resultat_obs_elab IS NOT NULL
  AND code_site IS NOT NULL
