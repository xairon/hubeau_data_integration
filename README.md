# Hub'Eau Data Pipeline

Pipeline d'ingestion et de transformation de données hydrologiques françaises, orchestré par Dagster.

## 🚀 Démarrage Rapide

### Prérequis
- Docker Desktop (Windows/Mac) ou Docker + Docker Compose (Linux)
- 8 GB RAM minimum
- 20 GB espace disque

### Installation

```bash
# 1. Cloner le projet
git clone <repository-url>
cd brgm

# 2. (Optionnel) Créer un fichier .env pour personnaliser les mots de passe
cp .env.example .env
# Éditer .env avec vos mots de passe

# 3. Démarrer la stack
docker compose up -d --build

# 4. Attendre que tous les services soient healthy (30-60 secondes)
docker compose ps
```

### Accès aux Interfaces

| Service | URL | Description |
|---------|-----|-------------|
| **Dagster UI** | http://localhost:49500 | Orchestration et monitoring des pipelines |
| **Adminer** | http://localhost:49501 | Interface web PostgreSQL |

**Identifiants Adminer** :
- Système : PostgreSQL
- Serveur : `postgres`
- Utilisateur : `postgres`
- Mot de passe : (celui défini dans `.env` ou `REDACTED` par défaut)
- Base de données : `postgres`

## 📊 Architecture

```
┌─────────────────┐     ┌─────────────────┐
│  APIs Hub'Eau   │     │  ERA5 (CDS)     │
│  Piézo/Hydro    │     │  Météo          │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │   DLT (Ingestion)     │
         │   → bronze.*_raw      │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │   dbt (Transformation)│
         │   → silver.stg_*      │
         │   → gold.int_*        │
         │   → gold.marts        │
         └───────────────────────┘
```

**Couches de données** :
- **`bronze`** : Données brutes (DLT + seeds dbt)
- **`silver`** : Données nettoyées (dbt staging)
- **`gold`** : Données transformées prêtes pour l'analyse (dbt intermediate + marts)

## 🔄 Utilisation

### Premier Run - Ingestion des Données

1. **Ouvrir Dagster UI** : http://localhost:49500

2. **Lancer les jobs d'ingestion** (dans l'ordre) :
   - `piezometry_stations_job` (sans partition)
   - `hydrometry_stations_job` (sans partition)
   - `piezometry_chroniques_job` → sélectionner partition `2024`
   - `hydrometry_chroniques_job` → sélectionner partition `2024`
   - `era5_meteo_job` → sélectionner partition `2024_2025`
   - `era5_timeseries_job` (extraction des time series depuis les NetCDF)

3. **Lancer la transformation** :
   - `dbt_silver_gold_pipeline_job` (crée les tables silver et gold)

### Jobs Disponibles

#### Ingestion (DLT)

| Job | Description | Partitions |
|-----|-------------|------------|
| `piezometry_stations_job` | Stations piézométriques BSS | Non |
| `piezometry_chroniques_job` | Mesures piézométriques | Oui (par année) |
| `hydrometry_stations_job` | Stations hydrométriques | Non |
| `hydrometry_chroniques_job` | Observations hydrométriques | Oui (par année) |
| `era5_meteo_job` | Téléchargement NetCDF ERA5 | Oui (chunks 2 ans) |
| `era5_timeseries_job` | Extraction time series ERA5 | Non |

#### Transformation (dbt)

| Job | Description |
|-----|-------------|
| `dbt_silver_gold_pipeline_job` | Pipeline complet bronze → silver → gold |

### Partitions

Les jobs partitionnés permettent de traiter les données par période :

- **Piézométrie/Hydrométrie** : Partitions par année (ex: `2024`, `2023`)
- **ERA5** : Partitions par chunks de 2 ans (ex: `2024_2025`, `2022_2023`)

**Pour lancer un job avec partition** :
1. Dans Dagster UI, cliquer sur le job
2. Cliquer sur "Launch Run"
3. Sélectionner la partition dans le dropdown
4. Cliquer sur "Launch"

## 📁 Structure du Projet

```
brgm/
├── src/
│   ├── hubeau_pipeline/          # Code Dagster
│   │   ├── assets/               # Assets (DLT + dbt)
│   │   │   ├── bronze/           # Assets d'ingestion
│   │   │   └── dbt_assets.py     # Assets dbt
│   │   ├── jobs/                 # Définition des jobs
│   │   ├── sources/              # Sources de données (APIs)
│   │   └── definitions.py        # Point d'entrée Dagster
│   └── dbt_hubeau/               # Projet dbt
│       ├── models/
│       │   ├── staging/          # → silver
│       │   ├── intermediate/     # → gold
│       │   └── marts/            # → gold (tables finales)
│       └── seeds/                # Données de référence
├── configs/                      # Configuration YAML
│   ├── hubeau/                   # Configs APIs Hub'Eau
│   └── era5/                     # Config ERA5
├── docker/                       # Dockerfiles
├── docs/                         # Documentation
└── docker-compose.yml            # Configuration Docker
```

## 📊 Tables Principales

### Bronze (Données brutes)

| Table | Description | Volume estimé |
|-------|-------------|---------------|
| `piezometry_stations_raw` | Stations BSS | ~23k |
| `piezometry_chroniques_raw` | Mesures piézo | ~23M |
| `hydrometry_stations_raw` | Stations hydro | ~5k |
| `hydrometry_obs_elab_raw` | Observations hydro | ~15M |
| `era5_france_meteo_raw` | Fichiers NetCDF ERA5 | ~38 fichiers |
| `era5_france_timeseries` | Time series ERA5 extraites | ~300M |
| `tme_entites_hydrogeo` | Référentiel TME (seed) | ~2k |

### Silver (Données nettoyées)

| Table | Description |
|-------|-------------|
| `stg_piezo_chroniques` | Chroniques piézo nettoyées |
| `stg_piezo_stations` | Stations piézo nettoyées |
| `stg_hydrometry_stations` | Stations hydro nettoyées |
| `stg_hydrometry_obs_elab` | Observations hydro nettoyées |
| `stg_era5_timeseries` | Time series ERA5 nettoyées |
| `stg_tme_entites` | TME nettoyé |

### Gold (Données transformées)

| Table | Description |
|-------|-------------|
| `int_daily_measurements` | Mesures quotidiennes agrégées |
| `int_station_era5_mapping` | Mapping stations → grille ERA5 |
| `int_era5_for_stations` | ERA5 filtré pour stations |
| **`hubeau_daily_chroniques`** | **Table finale : Piézo + Météo + TME** |

**Table principale** : `gold.hubeau_daily_chroniques`
- Combine piézométrie + météo ERA5 + métadonnées TME
- Toutes les colonnes d'observation sont non-nulles (INNER JOIN)
- Prête pour l'analyse

## 🔧 Commandes Utiles

### Docker

```bash
# Vérifier l'état des services
docker compose ps

# Voir les logs
docker compose logs -f dlt_worker
docker compose logs -f dagster_webserver

# Redémarrer un service
docker compose restart dlt_worker

# Rebuild complet
docker compose down
docker compose build --no-cache
docker compose up -d

# Arrêter tout
docker compose down

# Supprimer les volumes (⚠️ supprime les données)
docker compose down -v
```

### PostgreSQL

```bash
# Se connecter à PostgreSQL
docker exec -it brgm-postgres psql -U postgres -d postgres

# Vérifier les tables
\dt bronze.*
\dt silver.*
\dt gold.*

# Compter les lignes
SELECT schemaname, tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY n_live_tup DESC;
```

## 📚 Documentation

- [Architecture détaillée](docs/ARCHITECTURE.md) - Architecture complète du système
- [Configuration](docs/CONFIGURATION.md) - Variables d'environnement et configuration
- [Schéma BDD](docs/SCHEMA_BDD.md) - Structure des tables PostgreSQL
- [Stockage ERA5](docs/ERA5_DATA_STORAGE.md) - Détails sur le stockage des données ERA5

## 🛠️ Technologies

| Composant | Version | Rôle |
|-----------|---------|------|
| Dagster | 1.11.14 | Orchestration |
| DLT | 0.4.12 | Ingestion |
| dbt | 1.7.0 | Transformation |
| PostgreSQL | 16 | Base de données |
| PostGIS | 3.4 | Extension géospatiale |

## 🐛 Dépannage

### Les services ne démarrent pas

```bash
# Vérifier les logs
docker compose logs

# Vérifier les ports (doivent être libres)
netstat -an | grep 49500
netstat -an | grep 49501
netstat -an | grep 49502
```

### Erreur "Module not found"

```bash
# Rebuild le worker
docker compose build --no-cache dlt_worker
docker compose up -d dlt_worker
```

### Erreur de connexion PostgreSQL

```bash
# Vérifier que PostgreSQL est healthy
docker compose ps postgres

# Vérifier les variables d'environnement
docker exec brgm-dlt-worker env | grep PG_
```

## 📝 Licence

MIT
