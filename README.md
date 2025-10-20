# Hub'Eau Data Pipeline

Pipeline de données pour l'ingestion et le traitement des données hydrologiques françaises depuis les APIs Hub'Eau.

## Architecture

- **Orchestration**: Dagster
- **Ingestion**: DLT (Data Load Tool)
- **Base de données**: PostgreSQL
- **Déploiement**: Docker & GitLab CI/CD

## Structure du projet

```
brgm/
├── src/hubeau_pipeline/     # Code source principal
│   ├── assets/              # Assets Dagster (pipelines DLT)
│   ├── jobs/               # Jobs Dagster
│   ├── sensors/            # Sensors Dagster
│   └── utils/              # Utilitaires
├── configs/hubeau/         # Configurations YAML des sources
├── docker/                 # Dockerfiles et scripts d'init
├── dagster_home/           # Configuration Dagster
└── docs/                   # Documentation détaillée
```

## Démarrage rapide

### Local

```bash
# 1. Créer le fichier .env
cp .env.example .env

# 2. Démarrer les services
docker-compose up -d

# 3. Accéder à Dagster UI
open http://localhost:8080
```

### Production

Le déploiement se fait automatiquement via GitLab CI/CD sur push vers `main`.

## APIs Hub'Eau supportées

- **Piézométrie** - Niveaux des nappes phréatiques
- **Hydrométrie** - Hauteur et débit des cours d'eau
- **Qualité des eaux** - Analyses physicochimiques (rivières et nappes)
- **Prélèvements** - Volumes d'eau prélevés
- **Écoulements** - État des cours d'eau (assec, écoulement visible, etc.)
- **Température** - Température des cours d'eau
- **Hydrobiologie** - Indices biologiques

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Schéma base de données](docs/SCHEMA_BDD_HUBEAU.md)
- [Configuration](docs/ENVIRONMENT_CONFIGURATION.md)
- [Guide de démarrage](docs/QUICK_START_LOCAL.md)

## License

Propriétaire - BRGM
