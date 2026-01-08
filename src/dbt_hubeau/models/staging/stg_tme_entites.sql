{{
  config(
    materialized = 'view'
  )
}}

-- Staging view for TME (Table des Masses d'Eau / Entités Hydrogéologiques)
-- Source: SANDRE/BDLISA reference data
-- Note: "X" values in source are converted to NULL

SELECT
    "id"::int AS tme_id,
    "CodeEH" AS code_eh,
    "LibelleEH" AS libelle_eh,
    CASE WHEN "OrdreAbsEH" = 'X' OR "OrdreAbsEH" IS NULL THEN NULL ELSE "OrdreAbsEH"::int END AS ordre_abs_eh,
    CASE WHEN "NiveauEH" = 'X' OR "NiveauEH" IS NULL THEN NULL ELSE "NiveauEH"::int END AS niveau_eh,
    CASE WHEN "InclusEH" = 'X' THEN NULL ELSE "InclusEH" END AS inclus_eh,
    CASE WHEN "EtatEH" = 'X' THEN NULL ELSE "EtatEH" END AS etat_eh,
    CASE WHEN "NatureEH" = 'X' THEN NULL ELSE "NatureEH" END AS nature_eh,
    CASE WHEN "MilieuEH" = 'X' THEN NULL ELSE "MilieuEH" END AS milieu_eh,
    CASE WHEN "ThemeEH" = 'X' THEN NULL ELSE "ThemeEH" END AS theme_eh,
    CASE WHEN "OrigineEH" = 'X' THEN NULL ELSE "OrigineEH" END AS origine_eh
FROM {{ ref('tme_entites_hydrogeo') }}
WHERE "CodeEH" IS NOT NULL
  AND "CodeEH" != 'X'
