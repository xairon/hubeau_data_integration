# Operations & Maintenance

> Procédures d'exploitation, dépannage et sauvegarde du pipeline Hub'Eau.

---

## Table des matières

1. [Bootstrap initial](#1-bootstrap-initial)
2. [Opérations quotidiennes](#2-opérations-quotidiennes)
3. [Mise à jour du code](#3-mise-à-jour-du-code)
4. [Retraitement de données](#4-retraitement-de-données)
5. [Incidents courants](#5-incidents-courants)
6. [Sauvegarde et restauration](#6-sauvegarde-et-restauration)

---

## 1. Bootstrap initial

### Bootstrap complet (base vide)

Lancer le job `full_bootstrap_job` depuis Dagster UI :
1. Ouvrir http://localhost:49500 → Jobs → `full_bootstrap_job`
2. Cliquer sur "Launchpad" → "Launch Run"

Ce job charge dans l'ordre : référentiels (BDLISA + SANDRE) → stations → chroniques (par année) → ERA5 → dbt.

**Attention** : le bootstrap complet prend plusieurs heures (toutes les données depuis 1967/2000).

### Chargement progressif (pour tester)

1. `reference_data_bronze_job` — données TME/SANDRE
2. `all_stations_job` — métadonnées stations
3. Un job de chroniques pour une année récente
4. `dbt_full_pipeline_job` — transformations Silver + Gold

### Variables de contrôle du bootstrap

| Variable | Effet |
|----------|-------|
| `BOOTSTRAP_PARTITIONS` | Allowlist `job:partition` (ex: `chroniques:piezometry:2020,era5:1990-1991`) |
| `BOOTSTRAP_FORCE_RERUN` | Relancer même si déjà complété |
| `BOOTSTRAP_CONTINUE_ON_ERROR` | Continuer après erreur (best-effort) |

---

## 2. Opérations quotidiennes

### Vérifier la fraîcheur des données

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

### Vérifier les tests dbt

```bash
docker exec brgm-dlt-worker dbt test
```

### Vérifier les logs Dagster

```bash
# Logs du worker (exécution des jobs)
docker compose logs -f dlt_worker

# Logs du daemon (schedules/sensors)
docker compose logs -f dagster_daemon
```

---

## 3. Mise à jour du code

### Après modification des modèles dbt (Silver/Gold)

```bash
# 1. Rebuild le worker (régénère le manifest dbt)
docker compose build dlt_worker
docker compose up -d

# 2. (Optionnel) Drop et recréer les schémas si le schéma a changé
docker exec -i brgm-postgres psql -U postgres -d postgres -c \
  "DROP SCHEMA IF EXISTS silver CASCADE; DROP SCHEMA IF EXISTS gold CASCADE; DROP SCHEMA IF EXISTS silver_rejects CASCADE;"

# 3. Relancer le pipeline dbt complet
# Via Dagster UI : Jobs → dbt_full_pipeline_job → Launch Run
```

### Après modification du code Python

```bash
docker compose restart dlt_worker
```

### Après modification des dépendances (pyproject.toml)

```bash
docker compose down
docker compose build --no-cache dlt_worker
docker compose up -d
```

### Après modification des configs YAML

Rien à faire — les fichiers sont montés en volume et lus à chaque exécution.

---

## 4. Retraitement de données

### Retraitement d'une fenêtre temporelle (dbt)

```bash
# Piézométrie : rejouer depuis une date
docker exec brgm-dlt-worker dbt run --select stg_piezo_chroniques \
  --vars '{"piezometry_reprocess_from_date": "2020-01-01"}'

# Hydrométrie : rejouer depuis une date
docker exec brgm-dlt-worker dbt run --select stg_hydrometry_obs_elab \
  --vars '{"hydrometry_reprocess_from_date": "2020-01-01"}'
```

### Full-refresh d'un modèle incrémental

```bash
docker exec brgm-dlt-worker dbt run --full-refresh --select hubeau_daily_chroniques
```

### Recalculer le mapping stations-ERA5

Nécessaire après modification des données TME ou ajout de stations :

```bash
docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ \
  --vars '{"recompute_station_era5_mapping": true}'
```

### Relancer une partition de bootstrap

```bash
# Exemple : rejouer piézo 2020 et ERA5 1990-1991
BOOTSTRAP_PARTITIONS=chroniques:piezometry:2020,era5:1990-1991
BOOTSTRAP_FORCE_RERUN=true
# Puis relancer full_bootstrap_job depuis Dagster UI
```

---

## 5. Incidents courants

### Hub'Eau API 503 (Service Unavailable)

**Symptômes** : Job échoue avec `HTTPError 503`
**Cause** : API temporairement surchargée
**Solution** :
1. Vérifier [hubeau.eaufrance.fr](https://hubeau.eaufrance.fr/) pour les annonces
2. Attendre 15-30 min et relancer le job depuis Dagster UI
3. Le retry automatique (5 tentatives, backoff exponentiel) gère la plupart des cas

### ERA5 CDS Timeout

**Symptômes** : `TimeoutError` ou `ConnectionError` pendant le téléchargement ERA5
**Cause** : Requête trop volumineuse ou serveur CDS surchargé
**Solution** :
1. Relancer le job (retry intégré)
2. Vérifier le [statut CDS](https://cds.climate.copernicus.eu/)
3. Si un chunk de 2 ans échoue, relancer en plages plus petites

### TimescaleDB "tuple decompression limit exceeded"

**Symptômes** : `tuple decompression limit exceeded by operation`
**Cause** : DML incrémental sur hypertable compressée, limite par défaut dépassée
**Solution** :
```sql
ALTER DATABASE postgres SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
```
Puis `docker compose restart dlt_worker`. Ce réglage est normalement déjà dans `docker/postgres/init.sql`.

### Libellés TME NULL dans les tables Gold

**Symptômes** : `libelle_eh`, `code_eh` NULL dans `stations_piezo_carte` ou `int_station_era5_mapping`
**Cause** : Le mapping incrémental ne recalcule pas les lignes existantes
**Solution** :
```bash
# 1. Vérifier que bronze.tme_entites_hydrogeo contient des données
docker exec -it brgm-postgres psql -U postgres -d postgres -c \
  "SELECT COUNT(*), COUNT(libelle_eh) FROM bronze.tme_entites_hydrogeo;"

# 2. Forcer le recalcul complet
docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ \
  --vars '{"recompute_station_era5_mapping": true}'
```

### Données piézo/hydro qui s'arrêtent avant aujourd'hui

**Cause** : Le job daily n'a pas tourné (scheduler désactivé, container arrêté, erreur)
**Diagnostic** :
```sql
SELECT MAX(date_mesure) FROM bronze.piezometry_chroniques_raw;
SELECT MAX(date_obs_elab::date) FROM bronze.hydrometry_obs_elab_raw;
```
**Solution** : Vérifier les runs dans Dagster UI → Runs, et relancer les jobs daily manuellement.

### Dernière date ERA5 = aujourd'hui - 5 jours

C'est **normal**. Le Copernicus CDS publie les données avec un délai de ~5 jours.
Configurable via `ERA5_AVAILABILITY_LAG_DAYS` (défaut: 5).

### Trous dans les données ERA5

**Diagnostic** :
```sql
SELECT date_trunc('month', time), COUNT(*)
FROM bronze.era5_france_timeseries
GROUP BY 1 ORDER BY 1;
```
**Solution** : Identifier la période manquante et relancer le job `era5_historical_load` avec la partition correspondante.

### Duplicates dans les tables Bronze

**Cause** : Fenêtre de chevauchement de 7 jours, normal
**Solution** : Lancer dbt — la couche Silver déduplique automatiquement :
```bash
docker exec brgm-dlt-worker dbt run --select stg_piezo_chroniques stg_hydrometry_obs_elab
```

### Container qui ne démarre pas

```bash
docker compose logs <service_name>
# Causes fréquentes : conflit de port, image corrompue
docker compose build --no-cache <service_name>
docker compose up -d <service_name>
```

### Connexion PostgreSQL refusée

```bash
docker exec brgm-postgres pg_isready
docker compose logs postgres
docker compose restart postgres
```

### Disque plein

```bash
docker system df    # Diagnostic
docker system prune # Nettoyage (images/containers inutilisés)
```

---

## 6. Sauvegarde et restauration

### Backup automatisé quotidien (recommandé)

Ajouter au crontab :
```bash
# Backup quotidien à 2h, rétention 7 jours
0 2 * * * docker exec brgm-postgres pg_dumpall -c -U postgres | gzip > /backups/hubeau_$(date +\%Y\%m\%d).sql.gz
find /backups -name "hubeau_*.sql.gz" -mtime +7 -delete
```

### Backup manuel

```bash
# Dump complet
docker exec brgm-postgres pg_dumpall -c -U postgres | gzip > backup_$(date +%Y%m%d).sql.gz

# Dump d'un schéma spécifique (plus rapide)
docker exec brgm-postgres pg_dump -U postgres -n bronze postgres | gzip > backup_bronze.sql.gz
docker exec brgm-postgres pg_dump -U postgres -n gold postgres | gzip > backup_gold.sql.gz
```

### Restauration

```bash
# Restore complet
gunzip -c backup_20260305.sql.gz | docker exec -i brgm-postgres psql -U postgres

# Restore d'un schéma
gunzip -c backup_bronze.sql.gz | docker exec -i brgm-postgres psql -U postgres postgres
```

### Backup des volumes Docker

```bash
# Arrêter les containers
docker compose stop

# Backup du volume PostgreSQL
docker run --rm -v brgm_postgres_data:/data -v $(pwd):/backup alpine \
  tar czf /backup/postgres_data.tar.gz /data

# Redémarrer
docker compose up -d
```

### Restauration d'un volume

```bash
docker compose down
docker volume rm brgm_postgres_data
docker volume create brgm_postgres_data
docker run --rm -v brgm_postgres_data:/data -v $(pwd):/backup alpine \
  tar xzf /backup/postgres_data.tar.gz -C /
docker compose up -d
```

### Objectifs de reprise

| Scénario | RTO | RPO | Méthode |
|----------|-----|-----|---------|
| Crash container | 1 min | 0 | Auto-restart Docker |
| Corruption volume | 30 min | 1 jour | Restauration backup |
| Reconstruction complète | 4-8 h | N/A | Re-run tous les pipelines |
