# Hub'Eau Data Pipeline

Pipeline d'ingestion de données hydrologiques françaises depuis les APIs Hub'Eau vers PostgreSQL, orchestré par Dagster.

## Aperçu

Ce projet ingère automatiquement les données de 8 APIs Hub'Eau (piézométrie, hydrométrie, qualité des eaux, etc.) dans PostgreSQL pour analyse et exploitation. Il supporte également l'ingestion de fichiers CSV et de données de référence (SANDRE, BD-LISA).

**Architecture:** Hub'Eau APIs → DLT → PostgreSQL (orchestré par Dagster)

## Fonctionnalités

- **8 APIs Hub'Eau** : 22 endpoints configurés (stations + chroniques)
- **Modes d'ingestion** : FULL, YEAR (2020-2025), INCREMENTAL
- **CSV Ingestion** : Système config-driven sans code
- **Données de référence** : SANDRE & BD-LISA
- **Hot Reload** : Modification code en 2-3 secondes (sans rebuild Docker)
- **Déduplication** : MERGE/UPSERT automatique
- **CI/CD** : Déploiement automatique via GitLab

## Démarrage Rapide

### Prérequis

- Docker & Docker Compose
- 4 GB RAM minimum
- Git

### Installation ZERO-CONFIG

```bash
# 1. Cloner le projet
git clone <repository-url>
cd hubeau_data_integration

# 2. Build les images Docker
docker compose build

# 3. Démarrage
docker compose up -d

# 4. Accès Web UI
open http://localhost:8080
```

**✅ C'est tout !** Le système utilise des valeurs par défaut et fonctionne immédiatement.

**Interfaces disponibles:**
- Dagster UI : http://localhost:8080
- Adminer (PostgreSQL) : http://localhost:8081
- Portainer (Docker) : http://localhost:9000

### Configuration personnalisée (optionnel)

Pour personnaliser les credentials ou paramètres :

```bash
# Créer un fichier .env personnalisé (optionnel)
cp .env.example .env
# Éditer .env avec vos valeurs
nano .env

# Redémarrer les services
docker compose restart
```

**Credentials par défaut** (si pas de .env) :
- PostgreSQL : `postgres` / `REDACTED`
- Dagster DB : `postgres` / `REDACTED`

### Premier Pipeline

Dans l'interface Dagster (http://localhost:8080) :

1. Aller dans **Assets**
2. Sélectionner un asset (ex: `piezometry_stations_raw`)
3. Cliquer **Materialize**

## Structure du Projet

```
brgm/
├── src/hubeau_pipeline/          # Code source
│   ├── assets/                   # Assets Dagster (Bronze layer)
│   │   ├── bronze/               # Hub'Eau APIs assets (22)
│   │   ├── csv_universal.py      # CSV ingestion
│   │   └── monitoring/           # Data quality
│   ├── jobs/                     # Jobs d'orchestration
│   ├── sensors/                  # Sensors (CSV auto-detect)
│   ├── sources/                  # DLT sources
│   └── definitions.py            # Point d'entrée Dagster
│
├── configs/                      # Configuration YAML
│   ├── hubeau/                   # 22 configs Hub'Eau
│   └── csv_ingestion/            # Configs CSV
│
├── data/                         # Données
│   ├── csv_inbox/                # Dépôt CSV (auto-ingestion)
│   └── csv_archive/              # CSV archivés
│
├── docker/                       # Dockerfiles
├── docs/                         # Documentation
└── scripts/                      # Scripts maintenance
```

## APIs Hub'Eau Supportées

| API | Stations | Chroniques | Description |
|-----|----------|------------|-------------|
| Piézométrie | ✓ | ✓ | Niveaux nappes phréatiques |
| Hydrométrie | ✓ | ✓ | Hauteur/débit cours d'eau |
| Température | ✓ | ✓ | Température cours d'eau |
| Qualité rivières | ✓ | ✓ | Analyses physicochimiques |
| Qualité nappes | ✓ | ✓ | Analyses eaux souterraines |
| Hydrobiologie | ✓ | ✓ | Indices biologiques |
| Écoulements | ✓ | ✓ | État cours d'eau (assec) |
| Prélèvements | ✓ | ✓ | Volumes prélevés |

**Total : 22 assets** (11 stations + 11 chroniques)

## Modes d'Ingestion

### 1. Stations (FULL)

Remplace toutes les données à chaque exécution.

```yaml
# Exemple : piezometry_stations_raw
Mode: FULL
Partitions: Non
Usage: Première installation, refresh complet
```

### 2. Chroniques (Partitioned)

3 modes disponibles via partitions :

| Partition | Description | Usage |
|-----------|-------------|-------|
| **full** | Tout l'historique | Installation initiale |
| **2020-2025** | Année spécifique | Backfill ciblé |
| *(sans partition)* | Incremental (N derniers jours) | MAJ quotidienne |

**Configuration dans Dagster UI :** Launchpad → Partition → Sélectionner

## Ingestion CSV

### Quick Start

```yaml
# 1. Créer configs/csv_ingestion/mon_fichier.yml
source:
  file_pattern: "mon_fichier*.csv"
destination:
  table_name: mon_fichier
  write_disposition: replace
  primary_key: [id]
```

```bash
# 2. Déposer le CSV
docker cp mon_fichier.csv brgm-dlt-worker:/app/data/csv_inbox/

# 3. Matérialiser l'asset "csv_mon_fichier" dans Dagster UI
```

**Asset automatiquement généré** : `csv_mon_fichier` → Table : `staging.staging_mon_fichier`

Voir [configs/csv_ingestion/README.md](configs/csv_ingestion/README.md) pour plus de détails.

## Développement

### Hot Reload

Modifiez le code sans rebuild Docker :

1. Éditer `src/hubeau_pipeline/**/*.py`
2. Sauvegarder
3. Dans Dagster UI : cliquer **Reload definitions** (en haut à droite)
4. **Prêt en 2-3 secondes** ✓

### Structure d'un Asset

```python
# src/hubeau_pipeline/assets/bronze/dlt_assets.py
@asset(compute_kind="dlt", group_name="piezometry")
def piezometry_stations_raw(context):
    """Piezometry stations - FULL load"""
    config = yaml.safe_load(open("configs/hubeau/piezometry_stations.yml"))
    pipeline = create_dlt_pipeline("hubeau_piezometry_stations", context)
    return run_dlt_resource(pipeline, hubeau_stations(config), context)
```

### Ajouter une API Hub'Eau

1. Créer `configs/hubeau/nouvelle_api.yml` (copier un exemple existant)
2. Ajouter asset dans `src/hubeau_pipeline/assets/bronze/dlt_assets.py`
3. Ajouter job dans `src/hubeau_pipeline/jobs/hubeau_jobs.py`
4. Reload dans Dagster UI

## Production

### Déploiement CI/CD

Le déploiement est automatique via GitLab CI sur push vers `main`.

**Variables GitLab CI à configurer** (Settings → CI/CD → Variables) :
- `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASSWORD`
- `DAGSTER_PG_PASSWORD`

Voir [docs/GITLAB_CI_VARIABLES_SETUP.md](docs/GITLAB_CI_VARIABLES_SETUP.md)

### Déploiement Manuel

```bash
# Build
docker compose build

# Démarrage
docker compose up -d

# Vérification
docker compose ps
docker compose logs -f dlt_worker
```

## Troubleshooting

### "Database directory appears to contain a database"

**Ce n'est PAS une erreur.** Message normal de PostgreSQL au démarrage quand la DB existe déjà. Si les conteneurs démarrent, tout va bien.

### Container `dlt_worker` unhealthy

```bash
# Diagnostic
docker compose logs dlt_worker

# Solution
docker compose restart dlt_worker
```

### Reset complet

**ATTENTION : Supprime toutes les données**

```bash
docker compose down -v
docker compose up -d
```

## Accès Distant (SSH Port Forwarding)

Si le projet tourne sur un serveur distant, utilisez **Tabby** pour vous connecter avec redirection automatique des ports :

### Configuration Tabby (RECOMMANDÉ)

1. Ouvrir Tabby → **Settings** → **Profiles & connections**
2. **New profile** → **SSH connection**
3. **Importer le profil** : `scripts/tabby-profile-hubeau.json`

Ou configurer manuellement :
- **Host:** `dib-2019006065`
- **Username:** `ringuet`
- **Port forwarding:** 18080→8080, 18081→8081, 19000→9000, 15432→5432

✅ Une fois connecté, accédez aux interfaces via :
- Dagster UI : http://localhost:18080
- Adminer : http://localhost:18081
- Portainer : http://localhost:19000

Voir [docs/PORT_FORWARDING.md](docs/PORT_FORWARDING.md) pour plus de détails.

## Documentation

- [Port Forwarding](docs/PORT_FORWARDING.md) - SSH tunneling & configuration Tabby
- [Architecture](docs/ARCHITECTURE.md) - Architecture détaillée
- [Configuration](docs/CONFIGURATION.md) - Variables d'environnement
- [APIs Hub'Eau](docs/APIS_HUBEAU.md) - Liste complète des endpoints
- [Schéma BDD](docs/SCHEMA_BDD.md) - Structure PostgreSQL
- [CSV Ingestion](configs/csv_ingestion/README.md) - Guide CSV complet
- [GitLab CI/CD](docs/GITLAB_CI_VARIABLES_SETUP.md) - Configuration CI/CD

## Technologies

- **Orchestration** : Dagster 1.11.14
- **Ingestion** : DLT 0.4.12 (Data Load Tool)
- **Base de données** : PostgreSQL 16 + PostGIS
- **Conteneurisation** : Docker + Docker Compose
- **CI/CD** : GitLab CI/CD

