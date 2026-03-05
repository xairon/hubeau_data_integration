{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['geometry'], 'type': 'gist'}
    ],
    post_hook=["{{ add_primary_key(['code_eh']) }}"]
  )
}}

-- Staging TME / Entités hydrogéologiques
-- Source unique : bronze.tme_entites_hydrogeo (TME.csv) → codes EH + attributs.
-- Libellés Sandre résolus via CASE (nomenclatures stables BDLISA).
-- Géométrie BDLISA désactivée (nécessite chargement CSV national).

WITH tme AS (
    SELECT DISTINCT ON (code_eh)
        tme_id,
        code_eh,
        libelle_eh,
        ordre_abs_eh,
        niveau_eh,
        inclus_eh,
        etat_eh,
        nature_eh,
        milieu_eh,
        theme_eh,
        origine_eh
    FROM {{ source('staging', 'tme_entites_hydrogeo') }}
    WHERE code_eh IS NOT NULL
      AND TRIM(code_eh) != ''
    ORDER BY code_eh, tme_id DESC
)
SELECT
    tme.tme_id,
    tme.code_eh,
    NULLIF(TRIM(tme.libelle_eh), '') AS libelle_eh,
    tme.ordre_abs_eh,
    tme.niveau_eh,
    CASE tme.niveau_eh
        WHEN '1' THEN 'National'
        WHEN '2' THEN 'Régional'
        WHEN '3' THEN 'Local'
    END AS libelle_niveau_eh,
    tme.inclus_eh,
    tme.etat_eh,
    CASE tme.etat_eh
        WHEN '0' THEN 'Indéterminé'
        WHEN '1' THEN 'Libre seul'
        WHEN '2' THEN 'Libre et captif'
        WHEN '3' THEN 'Captif seul'
        WHEN '4' THEN 'Libre recouvert'
        WHEN '5' THEN 'Captif recouvert'
        WHEN '6' THEN 'Libre et captif recouvert'
        WHEN 'X' THEN 'Non qualifié'
    END AS libelle_etat_eh,
    tme.nature_eh,
    CASE tme.nature_eh
        WHEN '0' THEN 'Domaine non aquifère'
        WHEN '3' THEN 'Système aquifère'
        WHEN '4' THEN 'Domaine aquifère'
        WHEN '5' THEN 'Unité aquifère'
        WHEN '6' THEN 'Entité hydrogéologique non définie'
        WHEN '7' THEN 'Alluvions'
    END AS libelle_nature_eh,
    tme.milieu_eh,
    CASE tme.milieu_eh
        WHEN '0' THEN 'Indéterminé'
        WHEN '1' THEN 'Poreux'
        WHEN '2' THEN 'Fissuré'
        WHEN '3' THEN 'Karstique'
        WHEN '4' THEN 'Poreux et fissuré'
        WHEN '5' THEN 'Poreux et karstique'
        WHEN '6' THEN 'Fissuré et karstique'
        WHEN '7' THEN 'Poreux, fissuré et karstique'
        WHEN '8' THEN 'Double porosité (poreux et fissuré)'
        WHEN '9' THEN 'Double porosité (poreux et karstique)'
        WHEN '10' THEN 'Double porosité (fissuré et karstique)'
        WHEN 'X' THEN 'Non qualifié'
    END AS libelle_milieu_eh,
    tme.theme_eh,
    CASE tme.theme_eh
        WHEN '0' THEN 'Indéterminé'
        WHEN '1' THEN 'Sédimentaire'
        WHEN '2' THEN 'Socle'
        WHEN '3' THEN 'Volcanique'
        WHEN '4' THEN 'Alluvial'
        WHEN '5' THEN 'Intensément plissé'
    END AS libelle_theme_eh,
    tme.origine_eh,
    NULL::text AS libelle_origine_eh,
    NULL::geometry AS geometry
FROM tme
