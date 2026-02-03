# Operations Runbook

## Bootstrap complet (base vide)

Pour peupler toute la base from scratch :
1. Lancer le job **full_bootstrap** : il charge d’abord les données de référence (BDLISA + Sandre), puis stations, chroniques, ERA5, puis dbt.
2. Ou lancer dans l’ordre : **reference_data_bronze** puis **full_bootstrap** (si vous préférez séparer la référence).

Les entités hydrogéologiques (stg_tme_entites) dépendent de BDLISA et des nomenclatures Sandre ; le full_bootstrap inclut désormais cette étape en premier.

Variables utiles :
- `BOOTSTRAP_PARTITIONS` : allowlist `job:partition` (rejouer une plage précise).
- `BOOTSTRAP_FORCE_RERUN` : relancer même si déjà complété.
- `BOOTSTRAP_CONTINUE_ON_ERROR` : continuer après erreur (best-effort).

---

## Common Scenarios & Solutions

---

## 🔴 Pipeline Failures

### Hub'Eau API Returns 503 (Service Unavailable)
**Symptoms**: Job fails with `HTTPError 503`
**Cause**: Hub'Eau API is temporarily overloaded or under maintenance
**Solution**:
1. Check [Hub'Eau Status](https://hubeau.eaufrance.fr/) for announcements
2. Wait 15-30 minutes and retry
3. If persistent, check Twitter/X for @eabordeaux announcements

### CDS API Timeout (ERA5)
**Symptoms**: `TimeoutError` or `ConnectionError` during ERA5 download
**Cause**: Large request or CDS server overloaded
**Solution**:
1. Retry the job (has built-in retry with exponential backoff)
2. If 2-year chunk fails, consider running smaller date ranges manually
3. Check [CDS Status](https://cds.climate.copernicus.eu/)

### TimescaleDB "tuple decompression limit exceeded" (stg_piezo_chroniques, etc.)
**Symptoms**: `Database Error in model stg_piezo_chroniques ... tuple decompression limit exceeded by operation. current limit: 100000, tuples decompressed: 23608652`
**Cause**: DML incrémental (delete+insert) sur une hypertable TimescaleDB compressée ; la limite par défaut (100 000 tuples décompressés par transaction) est dépassée.
**Solution (une fois, sur la base existante)** :
```sql
ALTER DATABASE postgres SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
```
Puis **redémarrer le worker** (ou toute connexion existante) pour que les nouvelles connexions prennent le paramètre. Ex. :
```bash
docker compose restart dlt_worker
```
Relancer ensuite le job dbt. Pour les nouveaux déploiements, ce réglage est déjà dans `docker/postgres/init.sql`.

---

## 🟠 Data Issues

### Libellés TME (libelle_eh, etc.) NULL dans stations_piezo_carte / mapping
**Symptoms**: `libelle_eh` (et éventuellement `code_eh`) toujours NULL dans `gold.stations_piezo_carte` ou `gold.int_station_era5_mapping`.

**Cause**: 
- `int_station_era5_mapping` est en **incrémental** : les lignes déjà présentes ne sont pas recalculées.
- Ou jointure TME (code / spatial) qui ne matche pas (stations sans `codes_bdlisa`, ou hors géométries TME).
- Ou le fichier TME.csv source ne contient pas les libellés complets.

**Solution**:
1. Vérifier que `bronze.tme_entites_hydrogeo` contient des données :
   ```sql
   SELECT COUNT(*), COUNT(libelle_eh) FROM bronze.tme_entites_hydrogeo;
   ```
2. Forcer un recalcul complet du mapping :
   ```bash
   docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ --vars '{"recompute_station_era5_mapping": true}'
   ```
3. Si toujours NULL après run : exécuter `scripts/diagnose_tme_mapping.sql` (sur la base) pour vérifier `avec_libelle_eh` vs `sans_libelle_eh`, et `codes_bdlisa` renseigné ou non.
4. Vérifier que `silver.stg_tme_entites` contient bien `libelle_eh` (et `code_eh`). Voir `docs/BDLISA_INTEGRATION.md` pour plus de détails sur les sources TME.

### Données daily : dernière date (piézo / hydro)
**Symptoms**: Données piézométriques ou hydrométriques qui s'arrêtent avant « aujourd'hui » (ex. on est le 3 février, dernière date = 30 janvier).

**Diagnostic** :
```sql
-- Dernière date piézo (bronze)
SELECT MAX(date_mesure) AS derniere_date_piezo FROM bronze.piezometry_chroniques_raw;
-- Dernière date hydro (bronze)
SELECT MAX(date_obs_elab::date) AS derniere_date_hydro FROM bronze.hydrometry_obs_elab_raw;
```

**Causes possibles** :
1. Le job **daily_piezometry_bronze** (ou **daily_hydrometry_bronze**) n'a pas tourné ou a échoué les derniers jours (scheduler désactivé, container arrêté, erreur).
2. L'API Hub'Eau publie avec un délai (peu fréquent pour les chroniques).

**Solution** :
1. Dans Dagster UI → **Runs** : filtrer par job `daily_piezometry_bronze` (ou `daily_hydrometry_bronze`) et vérifier qu'un run a réussi chaque jour (ex. 31/01, 01/02, 02/02).
2. Si des runs ont échoué : corriger la cause (logs du worker), puis relancer le job manuellement pour rattraper.
3. Le job demande **(aujourd'hui − 7 jours) → aujourd'hui** (heure Paris, 4h UTC = 5h Paris). S'assurer que le daemon Dagster et le worker tournent bien à 4h UTC.

### Dernière date ERA5 (job weekly)
**Symptoms**: Données ERA5 qui s'arrêtent à « aujourd'hui − 5 jours » (ex. le 3 février, dernière date = 29 janvier).

**Explication** : C'est **volontaire**. Le job `era5_weekly_update` charge jusqu'à **(aujourd'hui − N)** avec N = 5 par défaut (délai de publication Copernicus CDS). Voir `ERA5_AVAILABILITY_LAG_DAYS` dans `docs/CONFIGURATION.md`.

**Pour aller plus près du temps réel** (si CDS est à jour) :
```bash
# Dans .env ou variables d'environnement du worker
ERA5_AVAILABILITY_LAG_DAYS=3
```
Puis redémarrer le worker et relancer le job ERA5 weekly.

### Gap in ERA5 Data
**Symptoms**: Missing dates in `bronze.era5_france_timeseries`
**Diagnosis**:
```sql
SELECT date_trunc('month', time), COUNT(*) 
FROM bronze.era5_france_timeseries 
GROUP BY 1 ORDER BY 1;
```
**Solution**:
1. Identify missing period
2. Manually trigger historical partition: `dagster job launch --job era5_historical_load --partition 2006_2007`

### Duplicates in Bronze Tables
**Symptoms**: More rows than expected after daily load
**Cause**: Overlap window (7 days) without dedup
**Solution**: Run dbt to deduplicate into Silver:
```bash
docker exec brgm-dlt-worker dbt run --select stg_piezo_chroniques stg_hydrometry_obs_elab
```

---

## 🟡 Infrastructure Issues

### Container Won't Start
**Diagnosis**:
```powershell
docker-compose logs dagster_webserver
docker-compose logs dlt_worker
```
**Common fixes**:
- Port conflict: Change port in `docker-compose.yml`
- Volume permissions: `docker-compose down -v` and restart
- Image issue: `docker-compose build --no-cache`

### Database Connection Refused
**Diagnosis**:
```powershell
docker exec brgm-postgres pg_isready
```
**Solution**:
1. Check if container is running: `docker ps`
2. Check logs: `docker logs brgm-postgres`
3. Restart: `docker-compose restart postgres`

### Disk Space Full
**Symptoms**: Write errors, container crashes
**Diagnosis**:
```powershell
docker system df
```
**Solution**:
```powershell
docker system prune -a --volumes  # WARNING: Removes unused data!
```

---

## 🟢 Routine Operations

### Mise à jour du code (Silver / Gold uniquement)

Après un **commit**, **git pull** et modification des modèles dbt (silver/gold) :

1. **Rebuild** l’image du worker (pour prendre les nouveaux modèles dbt / manifest) :
   ```powershell
   docker compose build dlt_worker
   docker compose up -d
   ```

2. **Supprimer** les schémas silver et gold (Bronze reste intact) :
   ```powershell
   docker exec -i brgm-postgres psql -U postgres -d postgres -c "DROP SCHEMA IF EXISTS silver CASCADE; DROP SCHEMA IF EXISTS gold CASCADE; DROP SCHEMA IF EXISTS silver_rejects CASCADE;"
   ```
   dbt recréera les schémas au prochain run.

3. **Relancer** le job dbt dans Dagster : **dbt_silver_gold_pipeline** (UI ou API).

Résumé : **commit → pull → build worker → drop silver/gold/silver_rejects → lancer dbt_silver_gold_pipeline**.

### Full Reload (Clean Slate)

Tout repart de zéro (y compris Bronze) :

```powershell
docker-compose down -v
docker-compose up -d
# Attendre que les services soient prêts, puis dans l’UI Dagster :
# 1. Run full_bootstrap (séquentiel, très long)
```

### Relancer une plage précise (bootstrap)

Relancer uniquement certaines partitions sans tout refaire :

```powershell
# Exemple: rejouer piezo 2020 et ERA5 1990-1991
BOOTSTRAP_PARTITIONS=chroniques:piezometry:2020,era5:1990-1991
```

Pour forcer la relance de tout :
```powershell
BOOTSTRAP_FORCE_RERUN=true
```

### Reprocess ciblé en Silver/Gold (dbt)

Rejouer une fenêtre historique sans full refresh :

```bash
# Piézo : rejouer depuis une date (incluse)
dbt run --select stg_piezo_chroniques --vars '{"piezometry_reprocess_from_date": "2020-01-01"}'

# Hydro : rejouer depuis une date (incluse)
dbt run --select stg_hydrometry_obs_elab --vars '{"hydrometry_reprocess_from_date": "2020-01-01"}'
```

### Check Data Freshness
```sql
SELECT 
  'piezometry' as domain,
  MAX(date_mesure) as latest_date,
  NOW() - MAX(date_mesure) as lag
FROM bronze.piezometry_chroniques_raw
UNION ALL
SELECT 
  'hydrometry',
  MAX(date_obs_elab),
  NOW() - MAX(date_obs_elab)
FROM bronze.hydrometry_obs_elab_raw
UNION ALL
SELECT 
  'era5',
  MAX(time),
  NOW() - MAX(time)
FROM bronze.era5_france_timeseries;
```

### Verify dbt Models
```bash
docker exec brgm-dlt-worker dbt test
```
