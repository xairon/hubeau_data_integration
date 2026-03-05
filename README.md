# Hub'Eau Data Pipeline

Pipeline de données hydrologiques françaises : ingestion automatique, transformation et visualisation.

Collecte les données piézométriques (nappes souterraines), hydrométriques (débits des rivières), climatiques (ERA5) et les référentiels associés. Architecture Medallion (Bronze → Silver → Gold) avec PostgreSQL/TimescaleDB, orchestrée par Dagster.

## Stack technique

| Composant | Version | Rôle |
|-----------|---------|------|
| **Dagster** | 1.11.14 | Orchestration des pipelines |
| **DLT** | 0.4.12 | Ingestion des données (APIs, fichiers) |
| **dbt** | 1.7.0 | Transformation SQL (staging, marts) |
| **PostgreSQL** | 16 | Base de données |
| **TimescaleDB** | - | Extension séries temporelles (compression, hypertables) |
| **PostGIS** | 3.4 | Extension géospatiale (jointures spatiales) |
| **Superset** | 4.0 | Business Intelligence (dashboards) |

## Démarrage rapide

### Prérequis

- Docker (avec Docker Compose v2)
- ~10 Go de RAM disponibles
- ~50 Go de disque (données complètes)

### Installation

```bash
# 1. Cloner le projet
git clone <repository-url>
cd hubeau_data_integration

# 2. Créer les volumes Docker (OBLIGATOIRE, une seule fois)
bash scripts/init_volumes.sh

# 3. Configurer l'environnement
cp .env.example .env
# Editer .env avec vos mots de passe

# 4. Lancer la stack
docker compose up -d --build

# 5. Vérifier que tout est healthy (~60 secondes)
docker compose ps
```

### Interfaces

| Service | URL | Notes |
|---------|-----|-------|
| Dagster UI | http://localhost:49500 | Orchestration et monitoring |
| Adminer | http://localhost:49501 | Admin PostgreSQL (leger) |
| PostgreSQL | localhost:49502 | Connexion directe |
| CloudBeaver | http://localhost:49503 | Client SQL avance |
| Superset | http://localhost:49504 | Dashboards BI |
| Grafana | http://localhost:49507 | Monitoring (admin/admin) |
| Prometheus | http://localhost:49508 | Metriques |

### Chargement initial

**Option A : Bootstrap complet** (recommande, plusieurs heures)

Dagster UI → Jobs → `full_bootstrap_job` → Launchpad → Launch Run

**Option B : Chargement progressif** (pour tester)

1. `reference_data_bronze_job` (referentiels)
2. `all_stations_job` (metadonnees)
3. Un job de chroniques pour une annee recente
4. `dbt_full_pipeline_job` (transformations)

### Verification

```bash
# Compter les lignes
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
SELECT schemaname, tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY schemaname, n_live_tup DESC;"

# Tests de qualite dbt
docker exec brgm-dlt-worker dbt test
```

## Commandes utiles

```bash
# dbt
docker exec brgm-dlt-worker dbt run                         # Pipeline complet
docker exec brgm-dlt-worker dbt run --select model_name     # Un modele
docker exec brgm-dlt-worker dbt test                        # Tests qualite
docker exec brgm-dlt-worker dbt docs generate               # Documentation

# Docker
docker compose logs -f dlt_worker     # Logs du worker
docker compose restart dlt_worker     # Redemarrer apres modif Python
docker compose build --no-cache dlt_worker && docker compose up -d  # Rebuild complet

# PostgreSQL
docker exec -it brgm-postgres psql -U postgres -d postgres
# \dt bronze.*   \dt silver.*   \dt gold.*
```

## Documentation

| Document | Contenu |
|----------|---------|
| [Guide d'onboarding](docs/ONBOARDING.md) | Guide complet pour nouveaux arrivants : concepts, architecture, code, procedures |
| [Configuration](docs/CONFIGURATION.md) | Variables d'environnement et parametrage |
| [Schema BDD](docs/SCHEMA_BDD.md) | Structure detaillee des tables PostgreSQL |
| [Operations](docs/OPERATIONS.md) | Runbook, depannage, sauvegarde et restauration |
| [Monitoring](docs/MONITORING.md) | Stack Grafana/Prometheus, metriques, alertes |
| [Superset](docs/SUPERSET.md) | Configuration BI et dashboards |
| [ERA5](docs/ERA5.md) | Architecture d'ingestion des donnees climatiques |
| [TimescaleDB](docs/TIMESCALEDB.md) | Hypertables, compression, indexation |
| [CLAUDE.md](CLAUDE.md) | Instructions pour Claude Code (developpement assiste) |

## Licence

MIT
