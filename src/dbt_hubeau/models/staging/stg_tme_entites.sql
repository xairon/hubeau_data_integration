{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['geometry'], 'type': 'gist'}],
    post_hook=["{{ add_primary_key(['code_eh']) }}"]
  )
}}

-- Staging TME / Entités hydrogéologiques
-- Source principale : bronze.tme_entites_hydrogeo (TME.csv) → codes EH + attributs.
-- La couche GeoPackage BDLISA chargée ici expose des codes EC (codeec) qui ne matchent pas TME/EH ;
-- la géométrie est donc optionnelle et peut rester NULL tant qu'une couche EH n'est pas chargée.
-- Libellés : jointures sur bronze.ref_*_eh (nomenclatures Sandre).

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
),
geo AS (
    SELECT
        code_eh,
        libelle_eh AS geo_libelle_eh,
        geometry
    FROM {{ source('staging', 'bdlisa_entites') }}
)
SELECT
    tme.tme_id,
    tme.code_eh,
    COALESCE(NULLIF(TRIM(tme.libelle_eh), ''), geo.geo_libelle_eh) AS libelle_eh,
    tme.ordre_abs_eh,
    tme.niveau_eh,
    ref_niveau.libelle AS libelle_niveau_eh,
    tme.inclus_eh,
    tme.etat_eh,
    ref_etat.libelle AS libelle_etat_eh,
    tme.nature_eh,
    ref_nature.libelle AS libelle_nature_eh,
    tme.milieu_eh,
    ref_milieu.libelle AS libelle_milieu_eh,
    tme.theme_eh,
    ref_theme.libelle AS libelle_theme_eh,
    tme.origine_eh,
    ref_origine.libelle AS libelle_origine_eh,
    geo.geometry
FROM tme
LEFT JOIN geo ON TRIM(geo.code_eh) = TRIM(tme.code_eh)
LEFT JOIN {{ source('staging', 'ref_niveau_eh') }} ref_niveau ON tme.niveau_eh = ref_niveau.code
LEFT JOIN {{ source('staging', 'ref_etat_eh') }} ref_etat ON tme.etat_eh = ref_etat.code
LEFT JOIN {{ source('staging', 'ref_nature_eh') }} ref_nature ON tme.nature_eh = ref_nature.code
LEFT JOIN {{ source('staging', 'ref_milieu_eh') }} ref_milieu ON tme.milieu_eh = ref_milieu.code
LEFT JOIN {{ source('staging', 'ref_theme_eh') }} ref_theme ON tme.theme_eh = ref_theme.code
LEFT JOIN {{ source('staging', 'ref_origine_eh') }} ref_origine ON tme.origine_eh = ref_origine.code
