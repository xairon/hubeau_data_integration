#!/usr/bin/env python3
"""Create clean wiki with proper structure - no duplicates."""
import requests
import urllib3
from pathlib import Path

urllib3.disable_warnings()

URL = "https://scm.univ-tours.fr"
PID = 1219
TOKEN = "REDACTED"
H = {"PRIVATE-TOKEN": TOKEN, "Content-Type": "application/json"}

def load(path):
    p = Path(__file__).parent.parent / path
    return p.read_text(encoding='utf-8') if p.exists() else None

def wiki(title, content=None, fpath=None):
    if fpath:
        content = load(fpath)
        if not content:
            print(f"[SKIP] {title} - file missing")
            return False

    print(f"{title}...", end=" ", flush=True)

    r = requests.post(f"{URL}/api/v4/projects/{PID}/wikis",
                     headers=H, json={"title": title, "content": content},
                     verify=False, timeout=30)

    if r.status_code == 201:
        print("[OK]")
        return True
    else:
        print(f"[ERR {r.status_code}]")
        return False

print("Creating clean wiki structure...\n")

# HOME
wiki("home", content="""# Hub'Eau Data Integration

Pipeline de donnees pour l'integration des APIs Hub'Eau (8 APIs, 24 endpoints, 778 attributs).

## Demarrage Rapide

- **[Quick Start Local](quick-start-local)** - Demarrer avec Docker (10 min)
- **[Quick Start Production](quick-start-production)** - Deployer sur VPS avec GitLab CI/CD

## Guides

- **[Configuration](configuration)** - Configurer les environnements
- **[Environment Setup](environment-setup)** - Variables d'environnement detaillees

## Architecture

- **[Overview](architecture-overview)** - Vue d'ensemble du systeme
- **[Stack Technique](architecture-stack)** - Dagster + DLT + MinIO + PostgreSQL/PostGIS
- **[Deployment](architecture-deployment)** - Architecture de deploiement

## Reference Technique (5500+ lignes)

Documentation exhaustive des APIs Hub'Eau et schemas de donnees :

- **[APIs Hub'Eau](reference-apis)** - 8 APIs, 24 endpoints, 778 attributs
- **[Schema BDD](reference-schema)** - Modele de donnees complet
- **[Autres Referentiels](reference-referentiels)** - SANDRE, BDLISA, BRGM
- **[Observabilite](reference-observability)** - Metriques Dagster

## Developpement

- **[DLT Tutorial](dev-dlt)** - Creer un pipeline DLT
- **[Tests](dev-tests)** - Guide de tests

## CI/CD

- **[Pipeline](cicd-pipeline)** - Configuration .gitlab-ci.yml
- **[Variables GitLab](cicd-variables)** - Variables a configurer

## Projet

- **[Vision JUNON](project-junon)** - Vision long terme BRGM

---

**Repository**: [gitlab.com/ringuet/hubeau_data_integration](https://scm.univ-tours.fr/ringuet/hubeau_data_integration)
**README**: [Documentation principale](https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/blob/main/README.md)
**Contributing**: [Guide de contribution](https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/blob/main/CONTRIBUTING.md)
""")

# SIDEBAR
wiki("_sidebar", content="""### Navigation

**Demarrage**
- [Quick Start Local](quick-start-local)
- [Quick Start Production](quick-start-production)

**Guides**
- [Configuration](configuration)
- [Environment Setup](environment-setup)

**Architecture**
- [Overview](architecture-overview)
- [Stack](architecture-stack)
- [Deployment](architecture-deployment)

**Reference**
- [APIs Hub'Eau](reference-apis)
- [Schema BDD](reference-schema)
- [Referentiels](reference-referentiels)
- [Observabilite](reference-observability)

**Dev**
- [DLT Tutorial](dev-dlt)
- [Tests](dev-tests)

**CI/CD**
- [Pipeline](cicd-pipeline)
- [Variables](cicd-variables)

**Projet**
- [JUNON](project-junon)

---
[Accueil](home)
""")

# QUICK START
wiki("quick-start-local", fpath="docs/QUICK_START_LOCAL.md")

wiki("quick-start-production", content="""# Quick Start - Production

Deploiement automatique sur VPS avec GitLab CI/CD.

## Configuration GitLab

**GitLab > Settings > CI/CD > Variables**

Ajouter 10 variables :

**Dagster**
- `DAGSTER_PG_HOST` = dagster_postgres
- `DAGSTER_PG_PASSWORD` = (mot de passe fort, 20+ caracteres)

**Data Storage**
- `PG_HOST` = postgres
- `PG_PASSWORD` = (mot de passe fort)
- `POSTGIS_HOST` = postgis

**MinIO (Object Storage)**
- `MINIO_ENDPOINT` = http://minio:9000
- `MINIO_USER` = admin
- `MINIO_PASS` = (mot de passe fort)
- `MINIO_REGION` = us-east-1
- `MINIO_BRONZE_BUCKET` = bronze

Voir [Variables GitLab](cicd-variables) pour details complets.

## Deploiement

```bash
git push origin main
```

Le pipeline GitLab se declenche automatiquement (5-10 min) :
1. Build des images Docker (orchestrator + worker)
2. Deploiement sur VPS
3. Health checks

## Acces

Une fois deploye :
- **Dagster UI**: http://srv991054.hstgr.cloud:8080
- **MinIO Console**: http://srv991054.hstgr.cloud:9001
- **Portainer**: https://srv991054.hstgr.cloud:9443

## Logs

Sur le serveur :
```bash
docker compose -f docker-compose.production.yml logs -f dagster_webserver
docker compose -f docker-compose.production.yml logs -f dlt_worker
```

Voir [Pipeline](cicd-pipeline) pour plus de details.

---
[Retour](home)
""")

# GUIDES
wiki("configuration", content="""# Configuration

Configuration du projet pour differents environnements.

## Fichiers

**Local**: `.env` (copier depuis `.env.template`)
**Production**: GitLab CI/CD Variables

## Variables principales

```bash
# Dagster Database
DAGSTER_PG_HOST=dagster_postgres
DAGSTER_PG_PASSWORD=xxx

# Data Storage
PG_HOST=postgres
PG_PASSWORD=xxx
POSTGIS_HOST=postgis

# Object Storage
MINIO_ENDPOINT=http://minio:9000
MINIO_USER=admin
MINIO_PASS=xxx
MINIO_BRONZE_BUCKET=bronze
```

## Validation

```bash
# Local
docker compose config

# Production (sur le serveur)
cd /srv/brgm
docker compose -f docker-compose.production.yml config
```

Voir [Environment Setup](environment-setup) pour tous les details.

---
[Retour](home)
""")

wiki("environment-setup", fpath="docs/ENVIRONMENT_CONFIGURATION.md")

# ARCHITECTURE
wiki("architecture-overview", fpath="docs/ARCHITECTURE.md")

wiki("architecture-stack", content="""# Stack Technique

## Vue d'ensemble

```
Orchestration: Dagster 1.11.14
    |
    v
ETL: DLT 0.4.12 (Data Load Tool)
    |
    v
Storage: MinIO (Bronze) + PostgreSQL/PostGIS (Silver/Gold)
```

## Orchestration - Dagster 1.11.14

**Pourquoi Dagster ?**
- Data-aware orchestration
- Testable (unit + integration tests)
- Monitoring built-in (UI, logs, metriques)
- Type-safe Python

**Architecture**:
- **Webserver**: UI (port 8080)
- **Daemon**: Scheduler, sensors
- **Worker**: Execute code via gRPC (port 4000)

## ETL - DLT 0.4.12

**Pourquoi DLT ?**
- Concu pour extraire des APIs REST
- Schema inference automatique
- Incremental loading
- Multiple destinations (Parquet, PostgreSQL, etc.)

**Pipeline**:
1. Extract: APIs Hub'Eau (requests)
2. Transform: Python (pandas, validation)
3. Load: Parquet > MinIO (Bronze layer)

## Storage - Medallion Architecture

**Bronze Layer**: MinIO (S3-compatible)
- Format: Parquet (colonnaire, compresse)
- Donnees brutes extraites des APIs
- Immuable

**Silver/Gold Layers**: PostgreSQL 16 + PostGIS 3.4
- Donnees nettoyees (Silver)
- Donnees aggregees (Gold)
- Geospatial support (PostGIS)

## Conteneurisation

**Docker Compose**:
- Local: `docker-compose.yml` (dev)
- Production: `docker-compose.production.yml` (optimized)

**Images**:
- Orchestrator: ~500 MB (Dagster)
- Worker: ~800 MB (Dagster + DLT)
- PostgreSQL: 16-alpine (~80 MB)
- PostGIS: 16-3.4 (~300 MB)
- MinIO: latest (~100 MB)

## Versions

| Composant | Version | Notes |
|-----------|---------|-------|
| Python | 3.11 | Performance + typing |
| Dagster | 1.11.14 | LTS |
| DLT | 0.4.12 | Stable |
| PostgreSQL | 16 | LTS |
| PostGIS | 3.4 | Compatible PG16 |
| MinIO | latest | Rolling |
| Docker | 24+ | Compose v2 |

---
[Retour](home)
""")

wiki("architecture-deployment", content="""# Architecture Deployment

## Local Development

```
Docker Desktop (localhost)
|
+-- Orchestrator :8080
+-- Worker :4000 (gRPC)
+-- PostgreSQL :5432 (data)
+-- PostGIS :5433 (geospatial)
+-- MinIO :9000/:9001
+-- Dagster Postgres (metadata)

Volumes: ./data/ (gitignored)
```

**Lancer**:
```bash
docker compose up -d
```

## Production VPS (Hostinger)

```
srv991054.hstgr.cloud
|
+-- Orchestrator :8080 (public)
+-- Worker :4000 (internal gRPC)
+-- MinIO :9001 (public console)
+-- Portainer :9443 (HTTPS)

Volumes: /srv/brgm-data/ (persistent)
```

**Deploiement**: Automatique via GitLab CI/CD

## CI/CD Pipeline

```
Git Push (main)
    |
    v
GitLab Runner (on VPS)
    |
    +-- Build Stage
    |   +-- Sync code to /srv/brgm
    |   +-- Build orchestrator image
    |   +-- Build worker image
    |   +-- Tag with commit SHA
    |
    v
    +-- Deploy Stage
        +-- Export CI/CD variables
        +-- docker compose down
        +-- docker compose up -d
        +-- Wait 90s (health checks)
        +-- Verify all healthy
```

**Duree**: 5-10 minutes

Voir [Pipeline](cicd-pipeline) pour configuration detaillee.

## Backup Strategy

**Code**: Git (automatique)

**Dagster metadata**: PostgreSQL backups
```bash
docker exec brgm-dagster-postgres pg_dump -U postgres dagster > backup.sql
```

**Bronze layer**: MinIO snapshots
```bash
mc mirror minio/bronze /backup/bronze/
```

## Scaling

**Vertical**: Augmenter RAM/CPU des conteneurs
**Horizontal**: Ajouter des workers (Dagster supporte)

```yaml
dlt_worker:
  deploy:
    replicas: 3  # 3 workers en parallele
```

---
[Retour](home)
""")

# REFERENCE
wiki("reference-apis", fpath="docs/APIS_HUBEAU_REFERENCE_COMPLETE.md")
wiki("reference-schema", fpath="docs/SCHEMA_BDD_HUBEAU.md")
wiki("reference-referentiels", fpath="docs/AUTRES_REFERENTIELS.md")
wiki("reference-observability", fpath="docs/OBSERVABILITY.md")

# DEVELOPMENT
wiki("dev-dlt", fpath="docs/TUTORIEL_DLT.md")

wiki("dev-tests", content="""# Tests

Guide pour tester le pipeline Hub'Eau Data Integration.

## Types de tests

### Unit Tests

Tests des fonctions individuelles.

```python
# tests/test_utils.py
from hubeau_pipeline.utils import clean_station_code

def test_clean_station_code():
    assert clean_station_code("  A123  ") == "A123"
    assert clean_station_code(None) == None
```

### Integration Tests

Tests des assets Dagster avec DLT.

```python
# tests/test_assets.py
from dagster import build_asset_context
from hubeau_pipeline.assets.bronze.hydrometrie import stations_hydrometrie

def test_stations_hydrometrie():
    context = build_asset_context()
    result = stations_hydrometrie(context)
    assert len(result['data']) > 0
```

### End-to-End Tests

Tests du pipeline complet.

```bash
docker compose up -d
dagster job execute -j hydrometrie_job
pytest tests/test_e2e.py
```

## Lancer les tests

```bash
# Tous les tests
pytest

# Avec coverage
pytest --cov=src/hubeau_pipeline

# Tests specifiques
pytest tests/test_utils.py
pytest -k "test_stations"
```

## Dans Docker

```bash
docker compose run --rm dlt_worker pytest
```

## Mocking APIs

Pour ne pas surcharger Hub'Eau :

```python
from unittest.mock import patch

@patch('requests.get')
def test_fetch_stations(mock_get):
    mock_get.return_value.json.return_value = {"data": [...]}
    mock_get.return_value.status_code = 200

    stations = fetch_stations()
    assert len(stations) == 2
```

---
[Retour](home)
""")

# CI/CD
wiki("cicd-pipeline", content="""# CI/CD Pipeline

Pipeline GitLab pour deploiement automatique.

## Configuration

Fichier `.gitlab-ci.yml` avec 2 stages :

### Stage 1: Build

```yaml
build:image:
  - rsync code to /srv/brgm
  - docker build orchestrator image
  - docker build worker image
  - tag with commit SHA
```

### Stage 2: Deploy

```yaml
deploy:production:
  - export GitLab CI/CD variables
  - docker compose down
  - docker compose up -d
  - wait 90s for health checks
  - verify all services healthy
```

## Declenchement

```bash
git push origin main
```

Pipeline automatique (5-10 min).

## Rollback

**Manuel via GitLab**:
CI/CD > Pipelines > [Pipeline] > Rollback

**Ou sur le serveur**:
```bash
docker compose -f docker-compose.production.yml restart
```

## Logs

**GitLab**: CI/CD > Pipelines > [Job]

**Serveur**:
```bash
docker compose -f docker-compose.production.yml logs -f dagster_webserver
docker compose -f docker-compose.production.yml logs -f dlt_worker
```

Voir [Variables](cicd-variables) pour configuration des variables.

---
[Retour](home)
""")

wiki("cicd-variables", content="""# Variables GitLab

Configuration : **GitLab > Settings > CI/CD > Variables**

## Variables requises (10)

### Dagster Orchestration

```
DAGSTER_PG_HOST
  Value: dagster_postgres
  Masked: Non

DAGSTER_PG_PASSWORD
  Value: <mot_de_passe_fort_20+_caracteres>
  Masked: Oui
  Protected: Oui
```

### Data Storage

```
PG_HOST
  Value: postgres
  Masked: Non

PG_PASSWORD
  Value: <mot_de_passe_fort>
  Masked: Oui

POSTGIS_HOST
  Value: postgis
  Masked: Non
```

### Object Storage (MinIO)

```
MINIO_ENDPOINT
  Value: http://minio:9000
  Masked: Non

MINIO_USER
  Value: admin
  Masked: Non

MINIO_PASS
  Value: <mot_de_passe_fort>
  Masked: Oui

MINIO_REGION
  Value: us-east-1
  Masked: Non

MINIO_BRONZE_BUCKET
  Value: bronze
  Masked: Non
```

## Generer des mots de passe securises

```bash
# Linux/Mac
openssl rand -base64 32

# Python
python -c "import secrets; print(secrets.token_urlsafe(32))"

# PowerShell
-join((48..57)+(65..90)+(97..122)|Get-Random -Count 32|%{[char]$_})
```

## Securite

**A FAIRE**:
- Passwords 20+ caracteres
- Masquer toutes les variables sensibles
- Protected variables pour production
- Rotation tous les 6 mois

**A NE PAS FAIRE**:
- Passwords simples (admin, password123)
- Reutiliser passwords entre environnements
- Commit fichiers .env
- Logger passwords en clair

## Utilisation

Les variables sont automatiquement disponibles dans le pipeline :

```bash
export DAGSTER_PG_PASSWORD
docker compose up -d
```

Docker Compose lit les variables depuis l'environnement :

```yaml
environment:
  DAGSTER_PG_PASSWORD: ${DAGSTER_PG_PASSWORD}
```

---
[Retour](home)
""")

# PROJECT
wiki("project-junon", fpath="docs/PROJET_JUNON_VISION.md")

print("\n" + "="*60)
print("Wiki complete cree!")
print("="*60)
print("\nWiki: https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/wikis/home")
