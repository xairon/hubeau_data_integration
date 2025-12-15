# Hub'Eau Data Pipeline

Pipeline d'ingestion de données hydrologiques françaises depuis les APIs Hub'Eau vers PostgreSQL, orchestré par Dagster.

## Aperçu

Ce projet ingère automatiquement les données hydrologiques dans PostgreSQL pour analyse et exploitation.

**Architecture:** Hub'Eau APIs → DLT → PostgreSQL (orchestré par Dagster)

## APIs Supportées

| API | Stations | Chroniques | Description |
|-----|----------|------------|-------------|
| **Piézométrie** | ✓ | ✓ | Niveaux nappes phréatiques |
| **Hydrométrie** | ✓ | ✓ | Hauteur/débit cours d'eau |
| **ERA5** | - | ✓ | Données météo Copernicus |

**Total : 7 assets** (5 Hub'Eau + 2 ERA5)

## Démarrage Rapide

### Prérequis

- Docker & Docker Compose
- 4 GB RAM minimum

### Installation

```bash
# 1. Cloner et démarrer
git clone <repository-url>
cd hubeau_data_integration
docker compose up -d --build

# 2. Accès Web UI
open http://localhost:8080
```

**Interfaces:**
- Dagster UI : http://localhost:8080
- Adminer (PostgreSQL) : http://localhost:8081

## Structure du Projet

```
brgm/
├── src/hubeau_pipeline/          # Code source
│   ├── assets/bronze/            # Assets DLT (piezometry, hydrometry, ERA5)
│   ├── jobs/                     # Jobs d'orchestration  
│   ├── sources/                  # DLT sources (hubeau_csv_source, era5_source)
│   └── definitions.py            # Point d'entrée Dagster
│
├── configs/hubeau/               # Configuration YAML (5 fichiers)
├── docker/                       # Dockerfiles
└── scripts/                      # Scripts maintenance
```

## Jobs Disponibles

### Stations (FULL load)
- `piezometry_stations_bronze` - Stations piézométriques
- `hydrometry_stations_bronze` - Sites + stations hydrométriques

### Chroniques (Partitioned 1967-2025)
- `piezometry_chroniques_bronze` - Niveaux nappes
- `hydrometry_chroniques_bronze` - Observations cours d'eau

### Globaux
- `all_stations_bronze` - Toutes les stations
- `all_chroniques_bronze` - Toutes les chroniques

### ERA5
- `era5_meteo_bronze` - Données météo Copernicus

## Technologies

| Composant | Version |
|-----------|---------|
| Dagster | 1.11+ |
| dagster-dlt | 0.27+ |
| DLT | 0.4+ |
| PostgreSQL | 16 + PostGIS |
| Docker | 24+ |

## Best Practices

Ce projet suit les best practices officielles:
- [dagster-dlt](https://docs.dagster.io/api/libraries/dagster-dlt) - `@dlt_assets` + `DagsterDltResource`
- [dagster-postgres](https://docs.dagster.io/api/libraries/dagster-postgres) - Stockage interne Dagster

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Configuration](docs/CONFIGURATION.md)  
- [DLT Best Practices](docs/DLT_BEST_PRACTICES.md)
- [ERA5 Data Storage](docs/ERA5_DATA_STORAGE.md)
