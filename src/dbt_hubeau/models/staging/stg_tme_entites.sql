{{
  config(
    materialized = 'table'
  )
}}

-- Staging model for TME (Table des Masses d'Eau / Entités Hydrogéologiques)
-- Source: bronze.tme_entites_hydrogeo (seed)
-- Silver layer: nettoyage des valeurs "X" (converties en NULL)

SELECT
    "id" AS tme_id,
    "CodeEH" AS code_eh,
    "LibelleEH" AS libelle_eh,
    NULLIF("OrdreAbsEH", 'X') AS ordre_abs_eh,
    NULLIF("NiveauEH", 'X') AS niveau_eh,
    NULLIF("InclusEH", 'X') AS inclus_eh,
    NULLIF("EtatEH", 'X') AS etat_eh,
    NULLIF("NatureEH", 'X') AS nature_eh,
    NULLIF("MilieuEH", 'X') AS milieu_eh,
    NULLIF("ThemeEH", 'X') AS theme_eh,
    NULLIF("OrigineEH", 'X') AS origine_eh
FROM {{ ref('tme_entites_hydrogeo') }}
WHERE "CodeEH" IS NOT NULL
  AND "CodeEH" != 'X'
