# Architecture Technique

Documentation de l'architecture du pipeline Hub'Eau Data Integration.

## 📋 Table des Matières

- [Vue d'ensemble](#vue-densemble)
- [Architecture Actuelle (Phase Bronze)](#architecture-actuelle-phase-bronze)
- [Stack Technique](#stack-technique)
- [Architecture des Données](#architecture-des-données)
- [Workflow d'Exécution](#workflow-dexécution)
- [Déploiement](#déploiement)
- [Roadmap](#roadmap)

---

## Vue d'ensemble

Le projet suit une architecture **multi-couches** (Bronze → Silver → Gold) inspirée du **Medallion Architecture** de Databricks.

```
┌─────────────────────────────────────────────────────────────────┐
│                         COUCHE GOLD                            │
│             Analytics, ML/IA, Jumeau Numérique                 │
│                    (Phase 3 - Roadmap)                         │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────────┐
│                        COUCHE SILVER                           │
│         Données nettoyées, harmonisées, enrichies              │
│                    (Phase 2 - En cours)                        │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────────┐
│                        COUCHE BRONZE ✅                        │
│              Données brutes intégrées depuis APIs              │
│                   (Phase 1 - Production)                       │
│                                                                 │
│  Hub'Eau (8 APIs) → DLT Pipeline → MinIO (Parquet)            │
└─────────────────────────────────────────────────────────────────┘
```

**État actuel** : Phase Bronze complète et en production

---

## Architecture Actuelle (Phase Bronze)

### Schéma d'Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION                            │
│                                                                 │
│  ┌──────────────┐        ┌──────────────┐                      │
│  │   Dagster    │        │   Dagster    │                      │
│  │  Webserver   │◄──────►│    Daemon    │                      │
│  │  (UI/API)    │        │  (Scheduler) │                      │
│  └──────────────┘        └──────────────┘                      │
│         │                        │                              │
│         │         gRPC           │                              │
│         └────────────┬───────────┘                              │
└──────────────────────┼──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        EXECUTION LAYER                          │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    DLT Worker                            │  │
│  │                                                          │  │
│  │  Hub'Eau API ──► DLT Pipeline ──► Parquet ──► MinIO    │  │
│  │                                                          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        STORAGE LAYER                            │
│                                                                 │
│  ┌────────────┐   ┌────────────┐   ┌────────────┐             │
│  │   MinIO    │   │ PostgreSQL │   │  PostGIS   │             │
│  │  (Bronze)  │   │ (Metadata) │   │   (Geo)    │             │
│  └────────────┘   └────────────┘   └────────────┘             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Séparation Orchestrator / Worker

**Principe** : Architecture distribuée pour scalabilité

| Composant | Rôle | Image Docker |
|-----------|------|--------------|
| **Orchestrator** | UI, API, Scheduling | `hubeau-orchestrator:latest` |
| **Worker** | Exécution DLT | `hubeau-worker:latest` |

**Avantages** :
- ✅ Worker lourd (GDAL, GEOS) séparé de l'orchestrateur léger
- ✅ Scaling horizontal possible (plusieurs workers)
- ✅ Orchestrateur redémarre rapidement
- ✅ Workers peuvent tourner sur machines différentes

---

## Stack Technique

### Technologies de Production ✅

| Composant | Version | Rôle |
|-----------|---------|------|
| **Python** | 3.11+ | Langage principal |
| **Dagster** | 1.11.14 | Orchestration, UI, scheduling |
| **DLT** | 0.4.12 | Data loading, transformations |
| **MinIO** | latest | Object storage S3-compatible (Bronze) |
| **PostgreSQL** | 16 | Base de données relationnelle |
| **PostGIS** | 16-3.4 | Extension géospatiale PostgreSQL |
| **Docker** | 24+ | Containerisation |
| **Portainer CE** | latest | Gestion containers |

### Technologies en Roadmap 🚧

| Composant | Usage prévu | Phase |
|-----------|-------------|-------|
| **TimescaleDB** | Time-series (> 100M lignes) | Silver |
| **Neo4j** | Graphe SOSA/SANDRE | Gold |
| **Prometheus** | Métriques détaillées | Silver |
| **Grafana** | Dashboards monitoring | Silver |

### Dépendances Python (Principales)

```python
# Orchestration
dagster==1.11.14
dagster-webserver==1.11.14
dagster-postgres==0.27.14
dagster-dlt==0.27.14

# Data Loading
dlt[postgres,filesystem,duckdb]==0.4.12

# Database
psycopg[binary]==3.1.13

# S3/MinIO
boto3==1.34.0
s3fs==2024.3.1

# Data Processing
pandas==2.2.0
pyarrow==15.0.0

# Geospatial
geopandas==0.14.0
shapely==2.0.2
```

**Fichier complet** : [pyproject.toml](../pyproject.toml)

---

## Architecture des Données

### Couche Bronze (Actuelle) ✅

**Format** : Parquet (columnar, compressé)
**Stockage** : MinIO (S3-compatible)
**Structure** : 1 table = 1 endpoint API

```
s3://bronze/
├── hydrometry_api/
│   ├── hydrometry_stations_reference/
│   │   └── year=2024/
│   │       └── *.parquet
│   ├── hydrometry_observations/
│   │   └── year=2024/
│   │       └── *.parquet
│   └── ...
├── piezometry_api/
├── quality_rivers_api/
└── ...
```

**Partitionnement** :
- **Annuel** : `year={year}/` pour toutes les données temporelles
- **Pas de partitioning** : Pour données de référence (stations)

**Schema Management** : DLT gère automatiquement l'évolution du schéma

### Couche Silver (En développement) 🚧

**Cibles** :
- **PostgreSQL** : Données relationnelles harmonisées
- **PostGIS** : Données géospatiales (stations, bassins)
- **TimescaleDB** : Chroniques optimisées (> 100M lignes)

**Transformations** :
1. Nettoyage (valeurs nulles, outliers)
2. Harmonisation (unités, formats dates)
3. Enrichissement (référentiels SANDRE, BDLISA)
4. Déduplication

### Couche Gold (Roadmap) 📋

**Cibles** :
- **Neo4j** : Graphe de connaissances (ontologie SOSA)
- **Vues analytiques** : Agrégations pour BI/ML

---

## Workflow d'Exécution

### 1. Scheduling (Dagster Daemon)

```
┌──────────────────┐
│ Dagster Daemon   │
│                  │
│ ┌──────────────┐ │
│ │  Schedules   │ │──► Vérifie toutes les 30s
│ └──────────────┘ │
│ ┌──────────────┐ │
│ │   Sensors    │ │──► Écoute événements
│ └──────────────┘ │
└──────────────────┘
         │
         ▼
    Crée Run
```

### 2. Exécution Asset (Worker)

```
1. Worker reçoit job via gRPC
         │
         ▼
2. Charge config YAML
   configs/hubeau/api.yml
         │
         ▼
3. DLT Source génère requêtes
   - Applique slicing strategy
   - Gère pagination automatique
         │
         ▼
4. Extract (HTTP → JSON)
   - Retry automatique
   - Rate limiting
   - Gestion erreurs
         │
         ▼
5. Transform (optionnel)
   - Normalisation
   - Type casting
         │
         ▼
6. Load (Parquet → MinIO)
   - Compression SNAPPY
   - Partitioning annuel
         │
         ▼
7. État DLT (metadata → MinIO)
   - _dlt_loads
   - _dlt_version
```

### 3. Monitoring

```
Dagster UI ──► Statut runs, logs
             │
             ▼
MinIO Console ──► Données brutes
             │
             ▼
Portainer ──► Santé containers
```

---

## Déploiement

### Environnements

| Environnement | Infra | URL | Déploiement |
|---------------|-------|-----|-------------|
| **Dev Local** | Docker Compose | localhost:8080 | Manuel |
| **Production** | VPS Hostinger | srv991054.hstgr.cloud:8080 | GitLab CI/CD |

### CI/CD Pipeline (GitLab)

```
┌─────────────────────────────────────────────────────────┐
│                      Stage: BUILD                       │
│                                                         │
│  1. rsync code vers /srv/brgm                          │
│  2. Build hubeau-orchestrator:latest                   │
│  3. Build hubeau-worker:latest                         │
│                                                         │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│                     Stage: DEPLOY                       │
│                                                         │
│  1. Génère .env.production (secrets GitLab)            │
│  2. docker compose down                                │
│  3. docker compose up -d                               │
│  4. Attente 90s (healthchecks)                         │
│  5. Vérification logs                                  │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Trigger** : Push sur `main`
**Durée** : ~10-15 minutes

### Configuration Multi-Environnements

**Variables externalisées** :

```bash
# Dagster
DAGSTER_PG_HOST=dagster_postgres
DAGSTER_PG_PASSWORD=***

# Data Storage
PG_HOST=postgres
PG_PASSWORD=***
POSTGIS_HOST=postgis

# Object Storage
MINIO_ENDPOINT=http://minio:9000
MINIO_USER=admin
MINIO_PASS=***
```

**Voir** : [ENVIRONMENT_CONFIGURATION.md](ENVIRONMENT_CONFIGURATION.md)

---

## Roadmap

### Phase 2 : Silver Layer 🚧

**Objectif** : Données propres, harmonisées, enrichies

| Tâche | Status | Description |
|-------|--------|-------------|
| TimescaleDB | Planifié | Optimisation chroniques (> 100M lignes) |
| Enrichissement SANDRE | En cours | Référentiels codes paramètres |
| Géocodage BDLISA | Planifié | Rattachement bassins versants |
| Dédoublonnage | Planifié | Consolidation stations multi-sources |

### Phase 3 : Gold Layer & Ontologie 📋

**Objectif** : Jumeau numérique avec ontologie SOSA

| Tâche | Status | Description |
|-------|--------|-------------|
| Neo4j | Planifié | Graphe de connaissances |
| Ontologie SOSA | Planifié | Sensor, Observation, Sample, Actuator |
| Vues analytiques | Planifié | Agrégations pour ML/IA |
| API GraphQL | Vision | Requêtes sémantiques |

**Voir** : [PROJET_JUNON_VISION.md](PROJET_JUNON_VISION.md)

---

## Performance & Scalabilité

### Capacité Actuelle

- **APIs Hub'Eau** : 8 APIs, 24 endpoints
- **Volume traité** : ~50 GB de données Parquet
- **Fréquence** : Quotidienne (stations) / Hebdomadaire (chroniques)
- **Durée run** : 2-5 minutes par endpoint

### Optimisations Implémentées

1. **Slicing intelligent** : Découpage temporel/géographique automatique
2. **Pagination** : Gestion automatique par DLT
3. **Compression** : Parquet SNAPPY (~5x vs CSV)
4. **Partitioning** : Partitions annuelles pour requêtes rapides
5. **Retry automatique** : Résilience face aux timeouts API
6. **Incremental loading** : DLT merge mode sur primary keys

### Limites Connues

| Limite | Valeur | Workaround |
|--------|--------|------------|
| Max records/requête | 20 000 | Slicing `station_month_chunked` |
| Timeout API | 30s | Retry avec backoff exponentiel |
| Taille container worker | 2 GB RAM | Ajuster dans docker-compose |

---

## Sécurité

### Credentials Management

- **Local** : `.env` (gitignored)
- **Production** : GitLab CI/CD Variables (Protected + Masked)
- **Pas de secrets hardcodés** : Uniquement via env vars

### Réseau

- **Orchestrator** : Expose port 8080 (UI)
- **Worker** : Expose port 4000 (gRPC interne uniquement)
- **MinIO** : Ports 9000 (API) + 9001 (Console)
- **Databases** : Ports internes au réseau Docker

### Backups

- **MinIO data** : `/srv/brgm-data/minio` (persistant)
- **PostgreSQL** : `/srv/brgm-data/dagster_pg` (persistant)
- **Stratégie** : Snapshots serveur + réplication MinIO (à venir)

---

## Troubleshooting

### Worker ne démarre pas

```bash
# Check logs
docker logs brgm-dlt-worker --tail 50

# Causes communes :
# - Import error (module manquant)
# - Port 4000 déjà utilisé
# - PYTHONPATH incorrect
```

### Pipeline échoue

```bash
# Via Dagster UI
http://localhost:8080 → Runs → [Run ID] → Logs

# Causes communes :
# - API Hub'Eau indisponible (503, 504)
# - Timeout (slicing insuffisant)
# - Credentials MinIO incorrects
```

### MinIO 403 Forbidden

```bash
# Vérifier credentials
docker exec brgm-dlt-worker env | grep MINIO

# Vérifier bucket existe
docker exec brgm-minio mc ls minio/bronze
```

**Guide complet** : [GITLAB_CI_SETUP.md](../GITLAB_CI_SETUP.md)

---

## Ressources

- **Code source** : https://scm.univ-tours.fr/ringuet/hubeau_data_integration
- **Dagster Docs** : https://docs.dagster.io
- **DLT Docs** : https://dlthub.com/docs
- **Hub'Eau** : https://hubeau.eaufrance.fr
