-- Diagnostic jointure TME (libellés BDLISA) dans int_station_era5_mapping
-- Exécuter après un run dbt (ex. dbt_silver_gold_pipeline).
-- Schéma gold pour intermediate/marts (dbt_project.yml).

-- 1) Mapping : part des lignes avec libelle_eh renseigné
SELECT
  'int_station_era5_mapping' AS tbl,
  COUNT(*) FILTER (WHERE libelle_eh IS NOT NULL) AS avec_libelle_eh,
  COUNT(*) FILTER (WHERE libelle_eh IS NULL)   AS sans_libelle_eh,
  COUNT(*) AS total
FROM gold.int_station_era5_mapping;

-- 2) Stations : part avec codes_bdlisa non vide (jointure par code possible)
SELECT
  COUNT(*) FILTER (WHERE codes_bdlisa IS NOT NULL AND TRIM(codes_bdlisa) != '') AS avec_codes_bdlisa,
  COUNT(*) FILTER (WHERE codes_bdlisa IS NULL OR TRIM(COALESCE(codes_bdlisa, '')) = '') AS sans_codes_bdlisa,
  COUNT(*) AS total
FROM gold.int_station_era5_mapping;

-- 3) Échantillon codes_bdlisa (stations) vs code_eh (TME)
SELECT code_bss, codes_bdlisa, code_eh, libelle_eh
FROM gold.int_station_era5_mapping
ORDER BY (libelle_eh IS NOT NULL) DESC, code_bss
LIMIT 20;
