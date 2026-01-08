{{
  config(
    materialized = 'view'
  )
}}

-- Staging view for TME (Table des Masses d'Eau / Entités Hydrogéologiques)
-- Source: SANDRE/BDLISA reference data

SELECT
    "id"::int AS tme_id,
    "CodeEH" AS code_eh,
    "LibelleEH" AS libelle_eh,
    "OrdreAbsEH"::int AS ordre_abs_eh,
    "NiveauEH"::int AS niveau_eh,
    "InclusEH" AS inclus_eh,
    "EtatEH" AS etat_eh,
    "NatureEH" AS nature_eh,
    "MilieuEH" AS milieu_eh,
    "ThemeEH" AS theme_eh,
    "OrigineEH" AS origine_eh
FROM {{ ref('tme_entites_hydrogeo') }}
WHERE "CodeEH" IS NOT NULL
