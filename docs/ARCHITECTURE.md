# Architecture Hub'Eau Pipeline

## Vue d'Ensemble

Pipeline d'ingestion des données Hub'Eau vers PostgreSQL avec orchestration Dagster.

```
┌─────────────────────────────────────────────┐
│           APIs Hub'Eau (8 APIs)             │
│  Piézométrie | Hydrométrie | Qualité | ...  │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│              DLT (Ingestion)                │
│  - Extraction API → CSV                     │
│  - Déduplication (MERGE/UPSERT)             │
│  - Retry automatique                        │
│  - 3 modes: FULL / YEAR / INCREMENTAL       │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│           PostgreSQL Database               │
│  Schema: staging (par défaut)               │
│  - 22 tables Hub'Eau                        │
│  - Tables référence (SANDRE, BD-LISA)       │
│  - Métadonnées DLT (_dlt_*)                 │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│        Dagster (Orchestration)              │
│  - UI Web (port 8080)                       │
│  - 21 jobs (16 Hub'Eau + 3 référence + 2)   │
│  - 2 sensors (CSV auto-ingestion)           │
└─────────────────────────────────────────────┘
```

## Composants

### 1. DLT - Ingestion

**Rôle** : Extraction Hub'Eau → Chargement PostgreSQL

**Fonctionnalités** :
- Format CSV direct
- Déduplication automatique (MERGE/UPSERT)
- Retry sur erreur API
- Rate limiting
- 3 modes : FULL / YEAR / INCREMENTAL

**Configuration** : 22 fichiers YAML dans `configs/hubeau/`

### 2. PostgreSQL - Stockage

**Rôle** : Base de données unique

**Structure** :
- Schema `staging` (par défaut) pour données Hub'Eau
- 22 tables Hub'Eau (11 stations + 11 chroniques)
- Tables référence : SANDRE + BD-LISA
- Métadonnées DLT : `_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`
- PostGIS activé pour données spatiales

**Initialisation** : Script `/docker/init-scripts/postgres/01_init_minimal.sql`

**Tables créées automatiquement** par DLT au premier run (pas de script SQL requis)

### 3. Dagster - Orchestration

**Rôle** : Planification et monitoring

**Composants** :
- **Webserver UI** : http://localhost:8080
- **Daemon** : Exécution jobs et sensors
- **Worker** : Exécution des pipelines DLT

**Jobs définis** : 21 jobs
- 8 jobs stations (par API) : FULL load
- 8 jobs chroniques (par API) : Partitioned (full, 2020-2025)
- 2 jobs globaux : `all_stations_bronze`, `all_chroniques_bronze`
- 3 jobs référence : SANDRE, BD-LISA, full load

**Sensors** : 2 sensors
- `csv_file_watcher_sensor` : Détection nouveaux CSV dans inbox
- `csv_archive_cleaner_sensor` : Archivage CSV traités

**Base métadonnées** : PostgreSQL séparé (dagster_postgres)

### 4. Docker - Déploiement

**Services** :
```yaml
- postgres           # Base Hub'Eau (PostGIS, port 5432)
- dagster_postgres   # Base métadonnées Dagster
- dlt_worker         # Worker DLT (exécution pipelines)
- dagster_webserver  # UI Dagster (port 8080)
- dagster_daemon     # Daemon (jobs/sensors)
- adminer            # PostgreSQL UI (port 8081)
```

## Flux de Données

### Ingestion Hub'Eau

```
1. Job Dagster déclenché (manuel ou schedule)
2. Asset Dagster exécuté
3. DLT source appelé (hubeau_stations ou hubeau_chroniques_*)
4. Requêtes API Hub'Eau (pagination automatique)
5. Transformation données → format DLT
6. Chargement PostgreSQL (MERGE/UPSERT)
7. Dagster log métriques (rows loaded, duration)
```

### Modes d'Ingestion

Voir [MODES_INGESTION.md](MODES_INGESTION.md)

## Configuration

### Variables d'Environnement

Voir [CONFIGURATION.md](CONFIGURATION.md)

### Configurations YAML

**Hub'Eau** : 22 fichiers dans `configs/hubeau/`
- 1 fichier = 1 endpoint API
- Configuration endpoint, pagination, filtres
- Clés primaires pour déduplication

Exemple `piezometry_chroniques.yml` :
```yaml
name: piezometry_chroniques
base_url: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes
path: /chroniques
primary_keys: [code_bss, date_mesure]
replication_key: date_mesure
page_size: 20000
```

**CSV** : Fichiers dans `configs/csv_ingestion/`
- Configuration par CSV
- Pattern de fichier, table destination, primary keys

## Assets Dagster

### Bronze Layer (22 assets Hub'Eau)

**Stations** (11 assets) :
- `piezometry_stations_raw`
- `hydrometry_sites_raw`, `hydrometry_stations_raw`
- `quality_rivers_stations_raw`
- `quality_groundwater_stations_raw`
- `temperature_stations_raw`
- `hydrobio_stations_raw`
- `ecoulement_stations_raw`, `ecoulement_campagnes_raw`
- `prelevements_ouvrages_raw`, `prelevements_points_raw`

**Chroniques** (11 assets) :
- `piezometry_chroniques_raw`
- `hydrometry_obs_elab_raw`
- `quality_rivers_analyses_raw`, `quality_rivers_conditions_raw`, `quality_rivers_operations_raw`
- `quality_groundwater_analyses_raw`
- `temperature_chroniques_raw`
- `hydrobio_indices_raw`, `hydrobio_taxons_raw`
- `ecoulement_observations_raw`
- `prelevements_chroniques_raw`

### Reference Data (6 multi-assets)

**SANDRE** : 4 multi-assets (17 tables)
**BD-LISA** : 2 assets (2 tables)

Voir [SANDRE_BDLISA_INTEGRATION.md](SANDRE_BDLISA_INTEGRATION.md)

### CSV Ingestion

- `ingest_all_csvs_asset` : Asset universel pour tous CSVs
- `csv_*` assets : Générés dynamiquement depuis configs

## Jobs Dagster

### Jobs par API (16 jobs)

**Stations** (8 jobs) :
- `piezometry_stations_bronze`
- `quality_rivers_stations_bronze`
- `quality_groundwater_stations_bronze`
- `hydrometry_stations_bronze`
- `temperature_stations_bronze`
- `hydrobio_stations_bronze`
- `ecoulement_stations_bronze`
- `prelevements_stations_bronze`

**Chroniques** (8 jobs) :
- `piezometry_chroniques_bronze`
- `quality_rivers_chroniques_bronze`
- `quality_groundwater_chroniques_bronze`
- `hydrometry_chroniques_bronze`
- `temperature_chroniques_bronze`
- `hydrobio_chroniques_bronze`
- `ecoulement_chroniques_bronze`
- `prelevements_chroniques_bronze`

### Jobs Globaux (2 jobs)

- `all_stations_bronze` : 11 assets stations
- `all_chroniques_bronze` : 11 assets chroniques (partitioned)

### Jobs Référence (3 jobs)

- `sandre_full_load_job` : Toutes tables SANDRE
- `bdlisa_spatial_load_job` : Tables BD-LISA
- `reference_data_full_load_job` : Tout (SANDRE + BD-LISA)

## Monitoring

### Dagster UI (http://localhost:8080)

- Vue d'ensemble runs (succès/échec)
- Logs détaillés par asset
- Métriques d'exécution (duration, rows loaded)
- Graphe de dépendances assets
- Sensors status
- Partitions status

### PostgreSQL

**Adminer** : http://localhost:8081
- Connexion : postgres / postgres / <password>
- Requêtes SQL
- Volumétrie tables

**Métriques DLT** :
```sql
-- Runs DLT récents
SELECT * FROM staging._dlt_loads
ORDER BY inserted_at DESC LIMIT 10;

-- État pipelines
SELECT * FROM staging._dlt_pipeline_state;
```

## Déploiement

### Local (Docker Compose)

```bash
docker-compose up -d
```

**Services démarrés** :
- PostgreSQL Hub'Eau (port 5432)
- PostgreSQL Dagster (interne)
- Dagster Webserver (port 8080)
- Dagster Daemon
- DLT Worker
- Adminer (port 8081)

### Production (GitLab CI/CD)

Push sur `main` → déploiement automatique

**Pipeline GitLab** :
1. Build images Docker (worker + orchestrator)
2. Push images vers registry
3. Deploy sur serveur
4. Health checks
5. Rollback si échec

Voir [GITLAB_CI_VARIABLES_SETUP.md](GITLAB_CI_VARIABLES_SETUP.md)

## Optimisations

### Performance

- **Parallélisme limité** : Max 3 assets concurrents (évite OOM)
- **Rate limiting** : 0.3s entre requêtes API Hub'Eau
- **Batch size adaptatif** : Pagination optimisée par API
- **COPY PostgreSQL** : Bulk insert (100k rows en 1-2s)

### Résilience

- **Retry automatique** : DLT retry sur erreurs API (3 tentatives)
- **Health checks** : Docker containers monitored
- **Logs structurés** : Dagster + DLT logging
- **État persisté** : DLT state pour incremental

### Sécurité

- Mots de passe dans `.env` (gitignored)
- GitLab CI/CD Variables (masked)
- Pas de secrets dans le code
- Réseau Docker interne
- `.dlt/*.toml` gitignored

## Tables PostgreSQL

### Création Automatique

**DLT gère automatiquement** :
1. Création tables au premier run
2. Inférence types depuis données
3. Ajout colonnes si nouveau champ API
4. Gestion métadonnées (`_dlt_id`, `_dlt_load_id`)

**Aucun script SQL requis** pour tables Hub'Eau.

### Schema

**Par défaut** : `staging`

**Configurable via** :
```bash
# .env
DLT_BRONZE_DATASET=staging  # ou "hubeau", "bronze", etc.
```

### Métadonnées DLT

- `_dlt_loads` : Historique chargements
- `_dlt_pipeline_state` : État incremental
- `_dlt_version` : Version DLT

## Troubleshooting

### "Database directory appears to contain a database"

✅ **Ce n'est PAS une erreur !**

Message PostgreSQL normal au redémarrage. La base existe déjà, scripts d'init skippés.

### Container `brgm-dlt-worker` unhealthy

**Diagnostic** :
```bash
docker compose logs dlt_worker
docker compose ps
```

**Causes fréquentes** :
- Port 4000 déjà utilisé
- PostgreSQL pas accessible
- Erreur Python au démarrage

**Solution** :
```bash
docker compose restart dlt_worker
```

### Erreur DLT connection

Vérifier variables dans `.env` :
```bash
PG_HOST=postgres
PG_PASSWORD=xxx
DESTINATION__POSTGRES__CREDENTIALS__HOST=postgres
```

### Reset complet

**ATTENTION : Perte données**

```bash
docker compose down -v
docker compose up -d
```

### Logs

```bash
# Worker DLT
docker compose logs -f dlt_worker

# Daemon Dagster
docker compose logs -f dagster_daemon

# PostgreSQL
docker compose logs -f postgres

# Tous services
docker compose logs -f
```

## Références

- [Hub'Eau APIs](https://hubeau.eaufrance.fr)
- [Dagster Docs](https://docs.dagster.io)
- [DLT Docs](https://dlthub.com/docs)
- [PostGIS](https://postgis.net)
