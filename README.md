# Hub'Eau Data Pipeline

Pipeline simple d'ingestion des données hydrologiques françaises depuis les APIs Hub'Eau vers PostgreSQL.

## Architecture

```
Hub'Eau APIs → DLT → PostgreSQL → Dagster (orchestration)
```

**Technologies** :
- **Orchestration** : Dagster
- **Ingestion** : DLT (Data Load Tool)
- **Base de données** : PostgreSQL
- **Déploiement** : Docker + GitLab CI/CD

## Fonctionnalités

- ✅ **8 APIs Hub'Eau** : Piézométrie, Hydrométrie, Qualité, Température, etc.
- ✅ **28 endpoints** configurés
- ✅ **3 modes d'ingestion** : FULL, YEAR, INCREMENTAL
- ✅ **Generic CSV Ingestion** : Ingestion config-driven sans code
- ✅ **Hot Reload** : Modifier le code sans rebuild (2-3 secondes)
- ✅ **Déduplication automatique** (MERGE/UPSERT)
- ✅ **Monitoring qualité** données
- ✅ **CI/CD GitLab** automatique

## Structure du Projet

```
brgm/
├── src/hubeau_pipeline/      # Code source
│   ├── assets/               # Assets Dagster
│   ├── jobs/                 # Jobs d'orchestration
│   ├── sensors/              # Monitoring
│   ├── destinations/         # PostgreSQL
│   └── definitions.py        # Point d'entrée Dagster
│
├── configs/
│   ├── hubeau/               # 28 configurations YAML (APIs)
│   └── csv_ingestion/        # Configs CSV (ingestion générique)
├── data/
│   ├── csv_inbox/            # Drop-zone pour CSVs
│   └── csv_archive/          # CSVs archivés
├── docker/                   # Dockerfiles
├── dagster_home/             # Config Dagster
├── docs/                     # Documentation
└── scripts/                  # Scripts maintenance
```

## Démarrage Rapide

### Développement Local

```bash
# 1. Configuration
cp .env.example .env
# Éditer .env avec vos credentials PostgreSQL

# 2. Démarrage
docker-compose up -d

# 3. Accès Dagster UI
open http://localhost:8080
```

### Production

Le déploiement se fait automatiquement via GitLab CI/CD sur push vers `main`.

## Modes d'Ingestion

Chaque asset peut être exécuté en 3 modes :

| Mode | Description | Usage |
|------|-------------|-------|
| **FULL** | Tout l'historique | Première installation |
| **YEAR** | Année spécifique | Backfill ciblé |
| **INCREMENTAL** | Derniers N jours | Mise à jour quotidienne |

**Configuration** : Via Dagster UI Launchpad ou CLI

Voir [Documentation complète](docs/MODES_INGESTION.md)

## APIs Hub'Eau Supportées

- **Piézométrie** : Niveaux nappes phréatiques
- **Hydrométrie** : Hauteur et débit cours d'eau
- **Qualité rivières** : Analyses physicochimiques
- **Qualité nappes** : Analyses eaux souterraines
- **Température** : Température cours d'eau
- **Écoulements** : État cours d'eau (assec, etc.)
- **Hydrobiologie** : Indices biologiques
- **Prélèvements** : Volumes prélevés

## Documentation

### Ingestion Hub'Eau
- [Modes d'ingestion](docs/MODES_INGESTION.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Schéma base de données](docs/SCHEMA_BDD.md)
- [Création automatique de schéma](docs/AUTO_SCHEMA_CREATION.md)
- [Configuration](docs/CONFIGURATION.md)
- [APIs Hub'Eau](docs/APIS_HUBEAU.md)

### Ingestion CSV ⭐ Nouveau !
- **[Solution Finale - Asset Universel](CSV_FINAL_SOLUTION.md)** ← Commencer ici !
- [Quick Start - Upload un CSV](QUICKSTART_CSV.md)
- [Asset Universel (recommandé)](docs/CSV_UNIVERSAL_ASSET.md)
- [Comment uploader un CSV](docs/CSV_UPLOAD_GUIDE.md)
- [Système config-driven (optionnel)](docs/CSV_INGESTION_SYSTEM.md)
- [Migration depuis ancien système](MIGRATION_CSV.md)

### Développement 🔥 Nouveau !
- **[Hot Reload Fix](HOT_RELOAD_FIX.md)** ← Modifier code sans rebuild !
- [Guide Hot Reload complet](docs/HOT_RELOAD_GUIDE.md)

### Projet
- [Vision Projet JUNON](docs/PROJET_JUNON_VISION.md)

## Troubleshooting

### "Database directory appears to contain a database"

✅ **Ce n'est PAS une erreur !**

Ce message PostgreSQL est **normal** et signifie que la base existe déjà. PostgreSQL skip l'init, c'est attendu.

**Actions** :
- Si les conteneurs démarrent → Tout va bien, ignorer le message
- Si le worker `brgm-dlt-worker` est unhealthy → Vérifier les logs : `docker compose logs dlt_worker`

### Container `dlt_worker` unhealthy

**Causes possibles** :
1. Port 4000 déjà utilisé
2. Erreur Python au démarrage
3. PostgreSQL pas accessible

**Diagnostic** :
```bash
docker compose logs dlt_worker
docker compose ps
```

**Solution** :
```bash
docker compose down
docker compose up -d
```

### Reset complet de la base

**ATTENTION** : Supprime toutes les données !

```bash
docker compose down -v  # Supprime volumes Docker
docker compose up -d    # Recréation complète
```

### Vérification santé services

```bash
# Script automatique
./scripts/check_services.sh

# Manuel
docker compose ps

# Logs d'un service
docker compose logs -f dlt_worker
```

## License

Propriétaire - BRGM
