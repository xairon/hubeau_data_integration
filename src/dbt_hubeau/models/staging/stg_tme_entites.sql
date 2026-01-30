{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['geometry'], 'type': 'gist'}],
    post_hook=["{{ add_primary_key(['code_eh']) }}"]
  )
}}

-- Staging TME / Entités hydrogéologiques
-- Source: bronze.bdlisa_entites (vue BDLISA, code + libellé + géométrie) + bronze.tme_entites_hydrogeo (TME.csv : niveau, etat, nature, ...) + bronze.ref_*_eh (Sandre nomenclatures).
-- BDLISA V3 layer 0 n'expose souvent que code/libellé ; TME.csv enrichit niveau_eh, etat_eh, nature_eh, milieu_eh, theme_eh, origine_eh.

WITH base AS (
    SELECT
        b.tme_id,
        b.code_eh,
        b.libelle_eh,
        b.niveau_eh,
        b.etat_eh,
        b.nature_eh,
        b.milieu_eh,
        b.theme_eh,
        b.origine_eh,
        b.geometry
    FROM {{ source('staging', 'bdlisa_entites') }} b
    WHERE b.code_eh IS NOT NULL
      AND TRIM(b.code_eh) != ''
      AND TRIM(b.code_eh) != 'X'
),
-- Enrichissement par TME.csv (attributs niveau, etat, nature, milieu, theme, origine) quand le gpkg ne les fournit pas
enriched AS (
    SELECT
        base.tme_id,
        base.code_eh,
        base.libelle_eh,
        NULLIF(TRIM(tme.ordre_abs_eh), '') AS ordre_abs_eh,
        COALESCE(NULLIF(TRIM(tme.niveau_eh), ''), base.niveau_eh) AS niveau_eh,
        NULLIF(TRIM(tme.inclus_eh), '') AS inclus_eh,
        COALESCE(NULLIF(TRIM(tme.etat_eh), ''), base.etat_eh) AS etat_eh,
        COALESCE(NULLIF(TRIM(tme.nature_eh), ''), base.nature_eh) AS nature_eh,
        COALESCE(NULLIF(TRIM(tme.milieu_eh), ''), base.milieu_eh) AS milieu_eh,
        COALESCE(NULLIF(TRIM(tme.theme_eh), ''), base.theme_eh) AS theme_eh,
        COALESCE(NULLIF(TRIM(tme.origine_eh), ''), base.origine_eh) AS origine_eh,
        base.geometry
    FROM base
    LEFT JOIN {{ source('staging', 'tme_entites_hydrogeo') }} tme
      ON TRIM(base.code_eh) = TRIM(tme.code_eh)
)
SELECT
    enriched.tme_id,
    enriched.code_eh,
    enriched.libelle_eh,
    enriched.ordre_abs_eh,
    enriched.niveau_eh,
    ref_niveau.libelle AS libelle_niveau_eh,
    enriched.inclus_eh,
    enriched.etat_eh,
    ref_etat.libelle AS libelle_etat_eh,
    enriched.nature_eh,
    ref_nature.libelle AS libelle_nature_eh,
    enriched.milieu_eh,
    ref_milieu.libelle AS libelle_milieu_eh,
    enriched.theme_eh,
    ref_theme.libelle AS libelle_theme_eh,
    enriched.origine_eh,
    ref_origine.libelle AS libelle_origine_eh,
    enriched.geometry
FROM enriched
LEFT JOIN {{ source('staging', 'ref_niveau_eh') }} ref_niveau ON ref_niveau.code = TRIM(enriched.niveau_eh)
LEFT JOIN {{ source('staging', 'ref_etat_eh') }} ref_etat ON ref_etat.code = TRIM(enriched.etat_eh)
LEFT JOIN {{ source('staging', 'ref_nature_eh') }} ref_nature ON ref_nature.code = TRIM(enriched.nature_eh)
LEFT JOIN {{ source('staging', 'ref_milieu_eh') }} ref_milieu ON ref_milieu.code = TRIM(enriched.milieu_eh)
LEFT JOIN {{ source('staging', 'ref_theme_eh') }} ref_theme ON ref_theme.code = TRIM(enriched.theme_eh)
LEFT JOIN {{ source('staging', 'ref_origine_eh') }} ref_origine ON ref_origine.code = TRIM(enriched.origine_eh)
