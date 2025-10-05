# Hub'Eau Data Integration Pipeline

Pipeline d'intégration des données Hub'Eau (8 APIs) avec architecture medallion et orchestration Dagster.

> **Dépôt GitLab :** https://scm.univ-tours.fr/ringuet/hubeau_data_integration

## Architecture

```
Hub'Eau APIs → MinIO (Bronze) → Specialized DBs (Silver) → Analytics (Gold)
```

**Stack technique :**
- **Orchestration :** Dagster
- **Data Lake :** MinIO (S3-compatible)
- **Time Series :** TimescaleDB
- **Geospatial :** PostGIS
- **Graph :** Neo4j
- **Infrastructure :** Docker Compose

## APIs Intégrées

| API | Endpoints | Volume | Partitioning |
|-----|-----------|--------|--------------|
| Hydrométrie | stations, observations_tr, obs_elab | ~50M records | Daily |
| Piézométrie | stations, chroniques_tr, chroniques | ~30M records | Daily |
| Qualité Cours d'Eau | station_pc, analyse_pc | ~5M records | Annual |
| Qualité Eaux Souterraines | stations, analyses | ~2M records | Annual |
| Température | station, chronique | ~10M records | Daily |
| ONDE | stations, observations | ~500K records | Annual |
| Hydrobiologie | stations_hydrobio, indices, taxons | ~1M records | Annual |
| Prélèvements | points_prelevement, chroniques | ~20M records | Annual |

## Installation

```bash
# Prérequis
docker-compose
python 3.11+

# Configuration
cp env.example .env
# Éditer .env avec les mots de passe

# Démarrage
docker-compose up -d
```

## Accès

- **Dagster UI :** http://localhost:8080
- **MinIO :** http://localhost:9001 (admin/your_minio_password)
- **TimescaleDB :** localhost:5432
- **PostGIS :** localhost:5433
- **Neo4j :** http://localhost:7474 (neo4j/your_neo4j_password)
- **pgAdmin :** http://localhost:5050

## Configuration

### Dagster
```yaml
# dagster.yaml
execution:
  multiprocess:
    max_concurrent: 3
```

### MinIO Buckets
- `bronze` : Données brutes Hub'Eau
- `silver` : Données transformées
- `gold` : Données analytiques

## Jobs Dagster

### Ingestion Bronze
```bash
# Ingestion complète
dagster job execute -j hubeau_bronze_ingestion_job

# Ingestion par API
dagster job execute -j hubeau_hydrometry_bronze_job
dagster job execute -j hubeau_piezometry_bronze_job
dagster job execute -j hubeau_quality_bronze_job
dagster job execute -j hubeau_temperature_bronze_job
dagster job execute -j hubeau_onde_bronze_job
dagster job execute -j hubeau_hydrobiology_bronze_job
dagster job execute -j hubeau_prelevements_bronze_job
```

### Backfill
```bash
# Backfill hydrométrie (derniers 30 jours)
dagster asset materialize -a hubeau_hydrometry_bronze --partition 2024-09-01

# Backfill piézométrie (derniers 7 jours)
dagster asset materialize -a hubeau_piezometry_bronze --partition 2024-09-30
```

## Restrictions APIs Hub'Eau

- **Rate limiting :** 0.5s entre requêtes
- **Timeout :** 60s par requête
- **Retry :** 3 tentatives avec backoff exponentiel
- **Concurrence :** Max 10 requêtes simultanées (toutes APIs confondues)
- **Pagination :** 1000 records par page
- **Limites :** Certaines APIs limitées à 10K records (hydrobiologie, qualité)

## Structure Projet

```
src/hubeau_pipeline/
├── assets/
│   ├── bronze/          # Ingestion Hub'Eau → MinIO
│   ├── silver/          # Transformation → Specialized DBs
│   └── gold/            # Analytics et dashboards
├── jobs/                # Jobs Dagster
├── schedules/           # Planification
├── sensors/            # Monitoring
└── resources.py         # Configuration ressources
```

## Connexions Bases de Données

```bash
# TimescaleDB
psql -h localhost -p 5432 -U postgres -d water_timeseries

# PostGIS
psql -h localhost -p 5433 -U postgres -d water_geo

# Neo4j
cypher-shell -u neo4j -p $NEO4J_PASSWORD
```

## Monitoring

- **Logs :** `docker-compose logs -f dagster_webserver`
- **Métriques :** Dagster UI → Assets → Metrics
- **Erreurs :** Dagster UI → Runs → Failed