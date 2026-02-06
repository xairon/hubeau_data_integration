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
-- Les libellés Sandre et la géométrie BDLISA sont désactivés temporairement.

WITH tme AS (
    SELECT
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
)
SELECT
    tme.tme_id,
    tme.code_eh,
    NULLIF(TRIM(tme.libelle_eh), '') AS libelle_eh,
    tme.ordre_abs_eh,
    tme.niveau_eh,
    NULL::text AS libelle_niveau_eh,
    tme.inclus_eh,
    tme.etat_eh,
    NULL::text AS libelle_etat_eh,
    tme.nature_eh,
    NULL::text AS libelle_nature_eh,
    tme.milieu_eh,
    NULL::text AS libelle_milieu_eh,
    tme.theme_eh,
    NULL::text AS libelle_theme_eh,
    tme.origine_eh,
    NULL::text AS libelle_origine_eh,
    NULL::geometry AS geometry
FROM tme
