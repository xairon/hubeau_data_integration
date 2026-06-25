# Hub'Eau Data Pipeline

Entrepôt de données hydrologiques françaises : ingestion automatique, transformation et
visualisation. Collecte les niveaux piézométriques (nappes souterraines), les débits
hydrométriques (rivières), les données climatiques ERA5 et les référentiels associés,
selon une architecture Medallion (Bronze → Silver → Gold) sur PostgreSQL/TimescaleDB,
orchestrée par Dagster.

## Stack technique

| Composant | Version | Rôle |
|-----------|---------|------|
| Dagster | 1.11 | Orchestration (schedules + sensors) |
| DLT | 0.4.12 | Ingestion (APIs Hub'Eau, ERA5) |
| dbt | 1.7.0 | Transformation SQL (staging → marts) |
| PostgreSQL | 16 | Base de données |
| TimescaleDB | pg16 | Séries temporelles (hypertables, compression) |
| PostGIS | 3.4 | Géospatial (jointures spatiales) |
| Superset | 4.0 | Business Intelligence (dashboards) |

## Démarrage

### Prérequis

- Docker avec Docker Compose v2
- ~10 Go de RAM, ~50 Go de disque (jeu de données complet)
- Une clé API [Copernicus CDS](https://cds.climate.copernicus.eu/) pour l'ingestion ERA5

### Installation

```bash
git clone <repository-url>
cd hubeau_data_integration

# 1. Créer les volumes Docker externes (obligatoire, une seule fois)
bash scripts/init_volumes.sh

# 2. Configurer l'environnement
cp .env.example .env
# Éditer .env : mots de passe et clé Copernicus

# 3. Construire et lancer la stack
docker compose up -d --build

# 4. Vérifier l'état des services (~60 s de démarrage)
docker compose ps
```

### Interfaces

| Service | URL | Rôle |
|---------|-----|------|
| Dagster | http://localhost:49500 | Orchestration et supervision |
| Adminer | http://localhost:49501 | Administration PostgreSQL |
| PostgreSQL | localhost:49502 | Accès direct à la base |
| Superset | http://localhost:49504 | Dashboards BI |
| dbt docs | http://localhost:49505 | Documentation dbt (à lancer manuellement, voir ci-dessous) |

### Chargement initial des données

Le job `full_bootstrap` charge l'ensemble (référentiels → stations → chroniques par
année → ERA5 → dbt). Il est restartable (état persisté dans `ops.bootstrap_state`).

Dagster UI → **Jobs** → `full_bootstrap` → **Launchpad** → **Launch Run**.

Le bootstrap complet dure plusieurs heures (données depuis 1967 pour la piézométrie,
2000 pour l'hydrométrie). Pour un test rapide, restreindre le périmètre via
`BOOTSTRAP_PARTITIONS` (voir [docs/CONFIGURATION.md](docs/CONFIGURATION.md)).

### Vérification

```bash
# Volume des tables par schéma
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
SELECT schemaname, tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY schemaname, n_live_tup DESC;"

# Tests de qualité dbt
docker exec brgm-dlt-worker dbt test
```

## Commandes courantes

```bash
# dbt (dans le conteneur worker)
docker exec brgm-dlt-worker dbt run                       # Pipeline complet
docker exec brgm-dlt-worker dbt run --select model_name   # Un modèle
docker exec brgm-dlt-worker dbt test                      # Tests qualité
docker exec brgm-dlt-worker dbt docs generate             # Documentation
docker exec brgm-dlt-worker dbt docs serve --port 8080    # Servir les docs sur :49505

# Docker
docker compose logs -f dlt_worker     # Logs du worker
docker compose restart dlt_worker     # Redémarrer après modification du code Python
docker compose build --no-cache dlt_worker && docker compose up -d  # Rebuild complet

# PostgreSQL
docker exec -it brgm-postgres psql -U postgres -d postgres
```

## Documentation

| Document | Contenu |
|----------|---------|
| [Architecture](docs/ARCHITECTURE.md) | Couches Medallion, orchestration, infrastructure Docker |
| [Configuration](docs/CONFIGURATION.md) | Variables d'environnement, paramétrage, déploiement |
| [Schéma BDD](docs/SCHEMA_BDD.md) | Structure des tables PostgreSQL (Bronze, Silver, Gold) |
| [Opérations](docs/OPERATIONS.md) | Runbook : bootstrap, exploitation, incidents, sauvegarde |
| [Superset](docs/SUPERSET.md) | Tables BI disponibles et cartographie |
| [ERA5](docs/ERA5.md) | Ingestion des données climatiques |
| [TimescaleDB](docs/TIMESCALEDB.md) | Hypertables, compression, types d'index |
| [Déploiement sandbox](docs/deploy-sandbox.md) | Déploiement Portainer + GitOps |

## Licence

MIT
