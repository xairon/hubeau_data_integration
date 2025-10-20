# Architecture Technique

Documentation de l'architecture du pipeline Hub'Eau Data Integration.

## 📋 Table des Matières

- [Vue d'ensemble](#vue-densemble)
- [Architecture Actuelle](#architecture-actuelle)
- [Stack Technique](#stack-technique)
- [Architecture des Données](#architecture-des-données)
- [Workflow d'Exécution](#workflow-dexécution)
- [Déploiement](#déploiement)
- [Roadmap](#roadmap)

---

## Vue d'ensemble

Le projet utilise une architecture simple et directe : **Hub'Eau APIs → DLT → PostgreSQL**.

```
┌─────────────────────────────────────────────────────────────────┐
│                     SOURCES DE DONNÉES                          │
│                                                                 │
│  8 APIs Hub'Eau (24 endpoints, 778 attributs)                 │
│  - Piézométrie                                                 │
│  - Hydrométrie                                                 │
│  - Qualité des eaux (rivières + nappes)                       │
│  - Température                                                 │
│  - Écoulement (ONDE)                                           │
│  - Hydrobiologie                                               │
│  - Prélèvements                                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DLT PIPELINE                               │
│  - Extraction (httpx + tenacity)                               │
│  - Pagination automatique                                       │
│  - Gestion d'erreurs et retries                                │
│  - Chargement incrémental (merge sur primary keys)             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    POSTGRESQL DATABASE                          │
│                                                                 │
│  Schema: hubeau                                                │
│  - Tables par endpoint (ex: hydrometry_stations)               │
│  - État DLT (_dlt_pipeline_state)                             │
│  - Gestion schéma automatique (DLT)                           │
│                                                                 │
│  Administration:                                               │
│  - Adminer (http://localhost:8081) - Requêtes rapides         │
│  - PgAdmin (http://localhost:5050) - Admin avancée            │
└─────────────────────────────────────────────────────────────────┘
```

**État actuel** : Pipeline d'ingestion en production. Les optimisations (hypertables, index, agrégations) seront ajoutées itérativement.

---

## Architecture Actuelle

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
│  │  Hub'Eau API ──► DLT Pipeline ──► PostgreSQL           │  │
│  │                                                          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        STORAGE LAYER                            │
│                                                                 │
│  ┌────────────────┐   ┌────────────────┐                       │
│  │  PostgreSQL    │   │     PostGIS    │                       │
│  │ (schema:hubeau)│   │   (extension)  │                       │
│  └────────────────┘   └────────────────┘                       │
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
- ✅ Worker lourd séparé de l'orchestrateur léger
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
| **PostgreSQL** | 16 | Base de données relationnelle |
| **PostGIS** | 16-3.4 | Extension géospatiale PostgreSQL |
| **Adminer** | latest | DB admin lightweight |
| **PgAdmin** | latest | DB admin full-featured |
| **Docker** | 24+ | Containerisation |
| **Portainer CE** | latest | Gestion containers |

### Technologies en Roadmap 🚧

| Composant | Usage prévu |
|-----------|-------------|
| **TimescaleDB** | Extension PostgreSQL pour time-series optimisées |
| **Prometheus** | Métriques détaillées |
| **Grafana** | Dashboards monitoring |

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

# HTTP Client
httpx==0.27.0
tenacity==8.2.3

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

### PostgreSQL Schema: `hubeau`

**Format** : Tables PostgreSQL gérées par DLT
**Structure** : 1 table = 1 endpoint API

```
PostgreSQL Database
└── Schema: hubeau
    ├── _dlt_loads              # DLT metadata (historique chargements)
    ├── _dlt_pipeline_state     # DLT state (incremental loading)
    ├── _dlt_version            # DLT schema versions
    │
    ├── hydrometry_stations     # Stations hydrométrie
    ├── hydrometry_obs_elab     # Observations élaborées
    ├── piezometry_stations     # Stations piézométrie
    ├── piezometry_chroniques   # Chroniques piézométriques
    ├── quality_rivers_stations # Stations qualité rivières
    ├── quality_rivers_analyses # Analyses qualité rivières
    ├── temperature_stations    # Stations température
    ├── temperature_chroniques  # Chroniques température
    ├── ecoulement_stations     # Stations écoulement
    ├── ecoulement_observations # Observations écoulement
    ├── hydrobio_stations       # Stations hydrobiologie
    ├── hydrobio_indices        # Indices biologiques
    └── ...                     # 24 tables au total
```

**Partitionnement** :
- **Assets Dagster** : Partitions annuelles pour données temporelles
- **Tables PostgreSQL** : Pas de partitionnement natif (à ajouter si besoin)

**Schema Management** :
- DLT gère automatiquement la création et l'évolution du schéma
- `write_disposition=merge` : Upsert basé sur primary keys
- `write_disposition=replace` : Remplacement complet (données de référence)

**Accès aux données** :
```python
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='your_password'
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM hubeau.hydrometry_stations LIMIT 10")
stations = cursor.fetchall()
```

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
   - Pagination automatique
   - Filtrage temporel (partitions annuelles)
   - Filtrage par stations actives
         │
         ▼
4. Extract (HTTP → JSON)
   - httpx + tenacity pour retry automatique
   - Rate limiting respectueux
   - Gestion erreurs API
         │
         ▼
5. Transform (optionnel)
   - Normalisation types
   - Nettoyage valeurs nulles
         │
         ▼
6. Load (PostgreSQL)
   - Insertion/upsert via DLT
   - Gestion schéma automatique
   - Merge sur primary keys
         │
         ▼
7. État DLT (metadata → PostgreSQL)
   - _dlt_loads (historique)
   - _dlt_pipeline_state (état incrémental)
```

### 3. Monitoring

```
Dagster UI ──► Statut runs, logs
             │
             ▼
Adminer ──► Requêtes SQL rapides
             │
             ▼
PgAdmin ──► Administration avancée
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

# Hub'Eau Data Storage
PG_HOST=postgres
PG_PORT=5432
PG_DB=postgres
PG_USER=postgres
PG_PASSWORD=***
HUBEAU_SCHEMA=hubeau
```

**Voir** : [ENVIRONMENT_CONFIGURATION.md](ENVIRONMENT_CONFIGURATION.md)

---

## Roadmap

### Optimisations PostgreSQL 🚧

**Objectif** : Optimiser la base de données itérativement selon les besoins

| Tâche | Priorité | Description |
|-------|----------|-------------|
| **Hypertables TimescaleDB** | Moyenne | Conversion tables chroniques en hypertables (> 10M lignes) |
| **Index spatiaux** | Haute | Index PostGIS sur colonnes géométriques (stations) |
| **Index temporels** | Haute | Index sur colonnes date pour requêtes temporelles |
| **Vues matérialisées** | Basse | Agrégations pré-calculées (moyennes mensuelles, etc.) |
| **Partitionnement natif** | Basse | Partitionnement PostgreSQL par année/région |

### Analytics & Visualisation 📋

**Objectif** : Exploiter les données Hub'Eau

| Tâche | Status | Description |
|-------|--------|-------------|
| **Grafana** | Planifié | Dashboards de monitoring et visualisations |
| **Metabase** | Vision | BI self-service pour explorations |
| **API REST** | Vision | Exposer données Hub'Eau via API |
| **Neo4j (optionnel)** | Vision | Graphe de connaissances (ontologie SOSA) |

**Voir** : [PROJET_JUNON_VISION.md](PROJET_JUNON_VISION.md)

---

## Performance & Scalabilité

### Capacité Actuelle

- **APIs Hub'Eau** : 8 APIs, 24 endpoints
- **Volume estimé** : ~10-50 GB de données PostgreSQL
- **Fréquence** : Selon schedules (quotidien/hebdomadaire/mensuel)
- **Durée run** : 2-10 minutes par asset selon volume

### Optimisations Implémentées

1. **Pagination automatique** : DLT gère la pagination Hub'Eau
2. **Chargement incrémental** : Merge sur primary keys (pas de doublons)
3. **Filtrage intelligent** : Extraction stations actives depuis PostgreSQL
4. **Retry automatique** : Résilience face aux erreurs API (tenacity)
5. **Partitions Dagster** : Parallélisation des assets par année
6. **HTTP async** : httpx pour requêtes non-bloquantes

### Limites Connues

| Limite | Valeur | Workaround |
|--------|--------|------------|
| Max records/page Hub'Eau | Variable | DLT gère automatiquement |
| Timeout API | 30-60s | Retry avec backoff exponentiel |
| Mémoire container worker | 2 GB RAM | Ajuster dans docker-compose |

---

## Sécurité

### Credentials Management

- **Local** : `.env` (gitignored)
- **Production** : GitLab CI/CD Variables (Protected + Masked)
- **Pas de secrets hardcodés** : Uniquement via env vars

### Réseau

- **Orchestrator** : Expose port 8080 (UI)
- **Worker** : Expose port 4000 (gRPC interne uniquement)
- **PostgreSQL** : Port 5432 (interne réseau Docker)
- **Adminer** : Port 8081 (HTTP)
- **PgAdmin** : Port 5050 (HTTP)

### Backups

- **PostgreSQL data** : `/srv/brgm-data/postgres` (persistant)
- **Dagster metadata** : `/srv/brgm-data/dagster_pg` (persistant)
- **Stratégie** : Snapshots serveur + backups PostgreSQL (pg_dump)

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
# - Timeout (retry automatique épuisé)
# - Credentials PostgreSQL incorrects
```

### PostgreSQL connection refused

```bash
# Vérifier credentials
docker exec brgm-dlt-worker env | grep PG_

# Vérifier que PostgreSQL est up
docker ps | grep postgres

# Tester connexion
docker exec postgres psql -U postgres -c "SELECT 1"
```

**Guide complet** : [GITLAB_CI_SETUP.md](../GITLAB_CI_SETUP.md)

---

## Ressources

- **Code source** : https://scm.univ-tours.fr/ringuet/hubeau_data_integration
- **Dagster Docs** : https://docs.dagster.io
- **DLT Docs** : https://dlthub.com/docs
- **Hub'Eau** : https://hubeau.eaufrance.fr
- **PostgreSQL Docs** : https://www.postgresql.org/docs/
- **PostGIS Docs** : https://postgis.net/documentation/
