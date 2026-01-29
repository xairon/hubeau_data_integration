{{
  config(
    materialized = 'table',
    post_hook=["{{ add_primary_key(['code_eh']) }}"]
  )
}}

-- Staging TME / Entités hydrogéologiques
-- Source: bronze.bdlisa_entites (vue schéma fixe BDLISA) + bronze.ref_*_eh (Sandre nomenclatures)
-- Données intégrées via pipeline Dagster (BDLISA GeoPackage → PostGIS, Sandre nomenclatures).

WITH base AS (
    SELECT
        tme_id,
        code_eh,
        libelle_eh,
        niveau_eh,
        etat_eh,
        nature_eh,
        milieu_eh,
        theme_eh,
        origine_eh,
        geometry
    FROM {{ source('staging', 'bdlisa_entites') }}
    WHERE code_eh IS NOT NULL
      AND TRIM(code_eh) != ''
      AND TRIM(code_eh) != 'X'
)
SELECT
    base.tme_id,
    base.code_eh,
    base.libelle_eh,
    NULL::text AS ordre_abs_eh,
    base.niveau_eh,
    ref_niveau.libelle AS libelle_niveau_eh,
    NULL::text AS inclus_eh,
    base.etat_eh,
    ref_etat.libelle AS libelle_etat_eh,
    base.nature_eh,
    ref_nature.libelle AS libelle_nature_eh,
    base.milieu_eh,
    ref_milieu.libelle AS libelle_milieu_eh,
    base.theme_eh,
    ref_theme.libelle AS libelle_theme_eh,
    base.origine_eh,
    ref_origine.libelle AS libelle_origine_eh,
    base.geometry
FROM base
LEFT JOIN {{ source('staging', 'ref_niveau_eh') }} ref_niveau ON base.niveau_eh = ref_niveau.code
LEFT JOIN {{ source('staging', 'ref_etat_eh') }} ref_etat ON base.etat_eh = ref_etat.code
LEFT JOIN {{ source('staging', 'ref_nature_eh') }} ref_nature ON base.nature_eh = ref_nature.code
LEFT JOIN {{ source('staging', 'ref_milieu_eh') }} ref_milieu ON base.milieu_eh = ref_milieu.code
LEFT JOIN {{ source('staging', 'ref_theme_eh') }} ref_theme ON base.theme_eh = ref_theme.code
LEFT JOIN {{ source('staging', 'ref_origine_eh') }} ref_origine ON base.origine_eh = ref_origine.code
