# Hub'Eau Data Pipeline

Pipeline d'ingestion et de transformation de données hydrologiques françaises, orchestré par Dagster.

## Architecture

```
Hub'Eau APIs ──┐
               ├──▶ DLT ──▶ PostgreSQL (staging) ──▶ dbt ──▶ PostgreSQL (hubeau)
ERA5 API ──────┘
```

**Couches de données :**
- `staging` : Données brutes (géré par DLT)
- `hubeau` : Données transformées et prêtes pour l'analyse (géré par dbt)

## Démarrage Rapide

```bash
# 1. Cloner et démarrer
git clone <repository-url>
cd hubeau_data_integration
docker compose up -d --build

# 2. Accès interfaces
# Dagster UI : http://localhost:49500
# Adminer    : http://localhost:49501
```

## Sources de Données

| Source | Type | Description |
|--------|------|-------------|
| Hub'Eau Piézométrie | API | Niveaux nappes phréatiques |
| Hub'Eau Hydrométrie | API | Hauteur/débit cours d'eau |
| ERA5 (Copernicus) | API | Données météo (température, précipitations) |

## Jobs Dagster

### Ingestion (DLT)
| Job | Description |
|-----|-------------|
| `piezometry_stations_job` | Stations piézométriques |
| `piezometry_chroniques_job` | Mesures piézométriques (partitionné) |
| `hydrometry_stations_job` | Stations hydrométriques |
| `hydrometry_chroniques_job` | Mesures hydrométriques (partitionné) |
| `era5_meteo_job` | Données météo ERA5 |
| `era5_timeseries_job` | Extraction time series ERA5 |

### Transformation (dbt)
| Job | Description |
|-----|-------------|
| `dbt_pipeline_transform` | Pipeline complet de transformation |

## Tables Produites

### Schéma `staging` (DLT)
- `piezometry_stations_raw`
- `piezometry_chroniques_raw`
- `hydrometry_stations_raw`
- `hydrometry_obs_elab_raw`
- `era5_france_timeseries`

### Schéma `hubeau` (dbt)
- `hubeau_daily_chroniques` — Table finale combinant piézométrie + météo ERA5

## Structure du Projet

```
hubeau_data_integration/
├── src/
│   ├── hubeau_pipeline/       # Code Dagster
│   │   ├── assets/            # Assets (DLT + dbt)
│   │   ├── jobs/              # Définition des jobs
│   │   └── definitions.py     # Point d'entrée
│   └── dbt_hubeau/            # Projet dbt
│       ├── models/
│       │   ├── staging/       # Vues sur données raw
│       │   ├── intermediate/  # Mapping, agrégation
│       │   └── marts/         # Tables finales
│       └── dbt_project.yml
├── docker/                    # Dockerfiles
├── configs/                   # Configuration YAML
└── docker-compose.yml
```

## Technologies

| Composant | Version |
|-----------|---------|
| Dagster | 1.11 |
| DLT | 0.4 |
| dbt | 1.7 |
| PostgreSQL | 16 |

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Configuration](docs/CONFIGURATION.md)
- [ERA5 Data Storage](docs/ERA5_DATA_STORAGE.md)
