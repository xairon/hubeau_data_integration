# Audit d’implémentation – BDLISA, Sandre, Superset, Jobs

Vérification effectuée pour s’assurer que l’implémentation est **standard, exploitable en production, sans effet de bord, sans erreur ni élément inventé ou caché**.

---

## 1. BDLISA / Sandre (assets Bronze)

### Vérifié
- **Source** : URL officielle BDLISA (reseau.eaufrance.fr) et doc Sandre (api.sandre.eaufrance.fr) citées dans le code.
- **Config** : `configs/bdlisa/bdlisa_entites.yml` avec `resource.url` et `extraction.schema/table/layer_index` ; pas de secret en dur.
- **Schéma** : table `bronze.bdlisa_entites_raw` + vue `bronze.bdlisa_entites` (schéma fixe pour dbt) ; nomenclatures dans `bronze.ref_*_eh`.
- **Nomenclatures Sandre** : listes codées (REF_*_eh) alignées sur le dictionnaire Sandre PRL / SAQ 2002-1 ; pas d’API appelée à l’exécution, pas de dépendance réseau pour les refs.

### Corrections appliquées
- **Risque d’erreur** : `BDLISAConfig.url.default` (Pydantic `Field`) n’est pas un attribut valide ; remplacé par une constante `DEFAULT_BDLISA_URL` utilisée dans la config et dans `_load_config`.
- **Sécurité** : validation de `schema_name` et `table_name` (regex `^[a-zA-Z_][a-zA-Z0-9_]*$`) avant toute construction SQL pour éviter l’injection (vue + table + CSV fallback).

---

## 2. dbt (sources, staging, mart)

### Vérifié
- **Sources** : `sources.yml` déclare `staging` → `schema: bronze` avec `bdlisa_entites`, `bdlisa_entites_raw`, `ref_*_eh` ; cohérent avec les assets Dagster.
- **stg_tme_entites** : lit `source('staging', 'bdlisa_entites')` et joint les `ref_*_eh` ; pas de seed ; colonne `geometry` documentée dans `schema.yml`.
- **stations_piezo_carte** : mart gold qui joint `int_station_era5_mapping` (geom + BDLISA) et `dim_piezo_stations` (alerte, tendance) ; une ligne par station ; index GiST sur `geom`, clé primaire `code_bss`.
- **Graphe dbt** : pas de cycle (stations_piezo_carte → int_station_era5_mapping, dim_piezo_stations → …).

### Aucune correction nécessaire
- Noms de colonnes et refs cohérents avec les modèles intermédiaires et marts existants.

---

## 3. Jobs Dagster et full_bootstrap

### Vérifié
- **reference_data_bronze_job** : `AssetSelection.assets(bdlisa_entites_raw, sandre_nomenclatures_eh)` ; pas de sélection par groupe qui inclurait d’autres assets.
- **full_bootstrap_job** : ordre `bootstrap_start` → `load_reference_data` → `load_all_stations` → chroniques → ERA5 → dbt → bootstrap_complete ; l’op `load_reference_data` a `required_resource_keys={"pg"}` ; la ressource `pg` est fournie par `Definitions` (RESOURCES).
- **Réutilisation** : `load_reference_data` appelle les mêmes fonctions que les assets (`bdlisa_entites_raw`, `sandre_nomenclatures_eh`) avec `context` et `pg` ; seul le type de contexte change (OpExecutionContext vs AssetExecutionContext), mais l’interface utilisée (`.log`) est compatible.

### Aucune correction nécessaire
- Pas d’effet de bord caché ; les assets peuvent être exécutés seuls ou via le job.

---

## 4. Superset (datasources, doc)

### Vérifié
- **Format YAML** : aligné sur la doc Superset (import_datasources) : `databases` → `database_name`, `sqlalchemy_uri`, `tables` → `table_name`, `schema`, `columns` → `column_name`, etc. Référence ajoutée en en-tête du fichier.
- **Schémas** : `gold` / `silver` cohérents avec les schémas dbt (marts/intermediate → gold, staging → silver).
- **Tables** : noms de tables et de colonnes correspondent aux modèles dbt (gold.hubeau_daily_chroniques, gold.stations_piezo_carte, etc.).

### Corrections appliquées
- **Cohérence avec la BDD** : dans `gold.hubeau_daily_chroniques` la colonne date est `date` (pas `date_mesure`) et la métrique niveau est `niveau_nappe_eau` (pas `niveau_moyen`). `main_dttm_col` et les colonnes dans `datasources.yaml` ont été alignés sur la table réelle (`date`, `niveau_nappe_eau`).

---

## 5. Documentation et références

- **ARCHITECTURE.md** : bronze décrit comme BDLISA + Sandre (plus de seeds) ; section Visualisation pointe vers SUPERSET.md.
- **SUPERSET.md** : objectif (exploitation dans Superset, calques), tableau usage / table, mention des jointures déjà faites en gold.
- **runbook** : bootstrap complet décrit (full_bootstrap inclut la référence, ou lancer reference_data_bronze avant).
- **Aucune API ou format inventé** : URLs BDLISA/Sandre réelles ; format Superset documenté par Apache ; schémas et noms de tables/colonnes dérivés du code dbt existant.

---

## 6. Résumé des corrections

| Fichier / zone | Problème | Correction |
|----------------|----------|------------|
| `bdlisa_assets.py` | `BDLISAConfig.url.default` invalide | Constante `DEFAULT_BDLISA_URL` + utilisation dans `_load_config` |
| `bdlisa_assets.py` | Risque d’injection SQL sur schema/table | `_validate_schema_table(schema_name, table_name)` (regex) avant toute création de table/vue |
| `docker/superset/datasources.yaml` | Colonne date / métrique incohérentes avec la table gold | `main_dttm_col: date`, colonne `date`, métrique `niveau_nappe_eau` |
| `docker/superset/datasources.yaml` | Référence format Superset manquante | Référence à la doc officielle import_datasources en en-tête |

---

## 7. Bonnes pratiques respectées

- **Séparation des responsabilités** : Bronze (assets) → Silver/Gold (dbt) → BI (Superset) ; pas de logique métier dupliquée.
- **Idempotence** : BDLISA/Sandre en replace ; vues recréées avec `CREATE OR REPLACE VIEW`.
- **Traçabilité** : sources et nomenclatures documentées (Sandre PRL, BDLISA V3).
- **Sécurité** : validation des noms de schéma/table ; pas de concaténation SQL avec des entrées utilisateur non validées.
- **Ressources** : connexion Postgres via ressource Dagster et variables d’environnement ; pas de mot de passe en dur dans le code.
