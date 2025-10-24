# Architecture Hub'Eau Pipeline

## Vue d'Ensemble

Pipeline simple d'ingestion des données Hub'Eau vers PostgreSQL avec orchestration Dagster.

```
┌─────────────────────────────────────────────┐
│           APIs Hub'Eau (8 APIs)             │
│  Piézométrie | Hydrométrie | Qualité | ...  │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│              DLT (Ingestion)                │
│  - Format CSV                               │
│  - Déduplication (MERGE)                    │
│  - Retry automatique                        │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│           PostgreSQL Database               │
│  Schema: hubeau                             │
│  - Tables stations (9)                      │
│  - Tables chroniques (11)                   │
│  - Métadonnées DLT (3)                      │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│        Dagster (Orchestration)              │
│  - UI Web (port 8080)                       │
│  - Jobs par API                             │
│  - Sensors monitoring                       │
│  - Schedules automatiques                   │
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

**Configuration** : 28 fichiers YAML dans `configs/hubeau/`

### 2. PostgreSQL - Stockage

**Rôle** : Base de données unique

**Structure** :
- Schema `hubeau` avec toutes les données
- Tables pré-créées via script SQL
- Index optimisés (temporels, géographiques)
- Métadonnées DLT (_dlt_loads, _dlt_pipeline_state)

**Initialisation** : Script `/docker/init-scripts/postgres/01_create_schema.sql`

### 3. Dagster - Orchestration

**Rôle** : Planification et monitoring

**Composants** :
- Webserver UI (http://localhost:8080)
- Daemon (exécution jobs/sensors)
- Jobs par API (11 jobs)
- Sensors (alertes, monitoring)

**Base métadonnées** : PostgreSQL Dagster (séparée)

### 4. Docker - Déploiement

**Services** :
```yaml
- postgres           # Base données Hub'Eau
- dagster_postgres   # Base métadonnées Dagster
- dagster_webserver  # UI Dagster
- dagster_daemon     # Daemon Dagster
- worker             # Worker DLT
```

## Flux de Données

### Ingestion Standard

```
1. Dagster Job déclenché (manuel ou automatique)
2. DLT récupère données depuis Hub'Eau API
3. Transformation CSV
4. Chargement PostgreSQL (MERGE)
5. Dagster log résultats
```

### Modes d'Ingestion

Voir [MODES_INGESTION.md](MODES_INGESTION.md)

## Configuration

### Variables d'Environnement

Voir [CONFIGURATION.md](CONFIGURATION.md)

### Configurations YAML

28 fichiers dans `configs/hubeau/` :
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
```

## Jobs Dagster

Un job par API :
- `piezometry_job`
- `quality_rivers_job`
- `hydrometry_job`
- `temperature_job`
- `ecoulement_job`
- `hydrobio_job`
- `prelevements_job`
- `quality_groundwater_job`

Jobs globaux :
- `all_stations_job` (9 assets stations)
- `all_chroniques_job` (11 assets chroniques)
- `all_hubeau_job` (tous assets)

## Sensors

**Monitoring** :
- `error_detection_sensor` : Détection erreurs
- `pipeline_failure_alert_sensor` : Alertes échecs
- `long_running_pipeline_sensor` : Pipelines lents
- `repeated_failure_sensor` : Échecs répétés

## Monitoring

**Dagster UI** :
- Vue d'ensemble des runs
- Logs détaillés par asset
- Métriques d'exécution
- Graphe de dépendances

**PostgreSQL** :
- Adminer (http://localhost:8081)
- Requêtes SQL directes
- Volumétrie tables

## Déploiement

### Local (Docker Compose)

```bash
docker-compose up -d
```

Services démarrés :
- PostgreSQL données (port 5432)
- PostgreSQL Dagster (interne)
- Dagster Webserver (port 8080)
- Dagster Daemon
- Worker DLT
- Adminer (port 8081)

### Production (GitLab CI/CD)

Push sur `main` → déploiement automatique VPS

Pipeline :
1. Build images Docker
2. Deploy sur VPS
3. Health checks
4. Rollback si échec

Voir [.gitlab-ci.yml](../.gitlab-ci.yml)

## Optimisations

### Performance
- Exécution séquentielle (1 asset à la fois) pour éviter OOM
- Index PostgreSQL optimisés
- Rate limiting API Hub'Eau
- Batch size adaptatif

### Résilience
- Retry automatique sur erreurs
- Health checks Docker
- Logs structurés
- État DLT persisté

## Sécurité

- Mots de passe dans GitLab CI/CD Variables (masked)
- Pas de secrets dans le code
- Réseau Docker interne
- `.dlt/*.toml` gitignored

## Troubleshooting

### Erreur : "could not translate host name postgres"
→ Vérifier service `postgres` dans docker-compose

### Erreur : DLT connection failed
→ Vérifier variables `PG_HOST`, `PG_PASSWORD`

### Erreur : Dagster UI inaccessible
→ Vérifier `dagster_webserver` container status

### Logs
```bash
# Daemon Dagster
docker logs -f brgm-dagster-daemon

# Worker
docker logs -f brgm-worker

# PostgreSQL
docker logs -f brgm-postgres
```
