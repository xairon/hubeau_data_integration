{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['geometry'], 'type': 'gist'}],
    post_hook=["{{ add_primary_key(['code_eh']) }}"]
  )
}}

-- Staging TME / Entités hydrogéologiques
-- 1) Source géométrie + code/libellé : bronze.bdlisa_entites (vue BDLISA GeoPackage).
-- 2) Enrichissement attributs (niveau, état, nature, ...) : bronze.tme_entites_hydrogeo (TME.csv).
--    Le gpkg layer 0 n'a souvent que code_eh/libelle_eh ; TME.csv fournit les codes eh_*.
-- 3) Libellés : jointures sur bronze.ref_*_eh (nomenclatures Sandre) à partir des codes enrichis.

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
),
enriched AS (
    SELECT
        base.tme_id,
        base.code_eh,
        COALESCE(NULLIF(TRIM(base.libelle_eh), ''), tme.libelle_eh) AS libelle_eh,
        tme.ordre_abs_eh,
        COALESCE(NULLIF(TRIM(base.niveau_eh), ''), NULLIF(TRIM(tme.niveau_eh), '')) AS niveau_eh,
        COALESCE(NULLIF(TRIM(base.etat_eh), ''), NULLIF(TRIM(tme.etat_eh), '')) AS etat_eh,
        COALESCE(NULLIF(TRIM(base.nature_eh), ''), NULLIF(TRIM(tme.nature_eh), '')) AS nature_eh,
        COALESCE(NULLIF(TRIM(base.milieu_eh), ''), NULLIF(TRIM(tme.milieu_eh), '')) AS milieu_eh,
        COALESCE(NULLIF(TRIM(base.theme_eh), ''), NULLIF(TRIM(tme.theme_eh), '')) AS theme_eh,
        COALESCE(NULLIF(TRIM(base.origine_eh), ''), NULLIF(TRIM(tme.origine_eh), '')) AS origine_eh,
        tme.inclus_eh,
        base.geometry
    FROM base
    LEFT JOIN LATERAL (
        SELECT *
        FROM {{ source('staging', 'tme_entites_hydrogeo') }} t
        WHERE t.code_eh IS NOT NULL
          AND TRIM(t.code_eh) != ''
          AND (
            UPPER(TRIM(t.code_eh)) = UPPER(TRIM(base.code_eh))
            OR REGEXP_REPLACE(UPPER(TRIM(t.code_eh)), '^0+', '') = REGEXP_REPLACE(UPPER(TRIM(base.code_eh)), '^0+', '')
          )
        ORDER BY CASE WHEN UPPER(TRIM(t.code_eh)) = UPPER(TRIM(base.code_eh)) THEN 0 ELSE 1 END
        LIMIT 1
    ) tme ON true
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
LEFT JOIN {{ source('staging', 'ref_niveau_eh') }} ref_niveau ON enriched.niveau_eh = ref_niveau.code
LEFT JOIN {{ source('staging', 'ref_etat_eh') }} ref_etat ON enriched.etat_eh = ref_etat.code
LEFT JOIN {{ source('staging', 'ref_nature_eh') }} ref_nature ON enriched.nature_eh = ref_nature.code
LEFT JOIN {{ source('staging', 'ref_milieu_eh') }} ref_milieu ON enriched.milieu_eh = ref_milieu.code
LEFT JOIN {{ source('staging', 'ref_theme_eh') }} ref_theme ON enriched.theme_eh = ref_theme.code
LEFT JOIN {{ source('staging', 'ref_origine_eh') }} ref_origine ON enriched.origine_eh = ref_origine.code
