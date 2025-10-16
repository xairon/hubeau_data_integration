#!/usr/bin/env python3
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

def wiki(slug, title, content=None, fpath=None):
    if fpath:
        content = load(fpath)
        if not content:
            print(f"[SKIP] {slug} - fichier manquant")
            return False

    print(f"{slug}...", end=" ", flush=True)

    # Creer
    r = requests.post(f"{URL}/api/v4/projects/{PID}/wikis",
                     headers=H, json={"title": title, "content": content},
                     verify=False, timeout=30)

    if r.status_code == 201:
        print("[OK]")
        return True

    # Mettre a jour
    r = requests.put(f"{URL}/api/v4/projects/{PID}/wikis/{slug}",
                    headers=H, json={"title": title, "content": content},
                    verify=False, timeout=30)

    if r.status_code == 200:
        print("[MAJ]")
        return True

    print(f"[ERR {r.status_code}]")
    return False

# HOME
wiki("home", "Wiki - Hub'Eau Data Integration", content="""# Hub'Eau Data Integration - Wiki

Pipeline de données pour l'intégration des APIs Hub'Eau (8 APIs, 24 endpoints, 778 attributs).

## Démarrage Rapide
- [Quick Start Local](quick-start-local) - Démarrer avec Docker (10 min)
- [Quick Start Production](quick-start-production) - Déployer sur VPS

## Guides
- [Configuration](guides-configuration) - Configurer les environnements
- [Environment Setup](guides-environment-setup) - Variables d'environnement

## Architecture
- [Overview](architecture-overview) - Vue d'ensemble du système
- [Stack](architecture-stack) - Dagster + DLT + MinIO + PostgreSQL
- [Deployment](architecture-deployment) - Architecture de déploiement

## Référence (5500+ lignes)
- [APIs Hub'Eau](reference-apis-hubeau) - 8 APIs, 24 endpoints, 778 attributs
- [Schéma BDD](reference-schema-bdd) - Modèle de données complet
- [Autres Référentiels](reference-autres-referentiels) - SANDRE, BDLISA
- [Observabilité](reference-observability) - Métriques Dagster

## Développement
- [DLT Tutorial](development-dlt-tutorial) - Créer un pipeline DLT
- [Tests](development-tests) - Guide de tests

## CI/CD
- [Pipeline](cicd-pipeline) - Configuration .gitlab-ci.yml
- [Variables GitLab](cicd-variables) - Variables à configurer

## Projet
- [Vision JUNON](project-junon) - Vision BRGM

---
**Repository**: [gitlab.com/ringuet/hubeau_data_integration](https://scm.univ-tours.fr/ringuet/hubeau_data_integration)
""")

# SIDEBAR
wiki("_sidebar", "_sidebar", content="""### Navigation

**Démarrage**
- [Quick Start Local](quick-start-local)
- [Quick Start Production](quick-start-production)

**Guides**
- [Configuration](guides-configuration)
- [Environment Setup](guides-environment-setup)

**Architecture**
- [Overview](architecture-overview)
- [Stack](architecture-stack)
- [Deployment](architecture-deployment)

**Référence**
- [APIs Hub'Eau](reference-apis-hubeau)
- [Schéma BDD](reference-schema-bdd)
- [Référentiels](reference-autres-referentiels)
- [Observabilité](reference-observability)

**Dev**
- [DLT Tutorial](development-dlt-tutorial)
- [Tests](development-tests)

**CI/CD**
- [Pipeline](cicd-pipeline)
- [Variables](cicd-variables)

**Projet**
- [JUNON](project-junon)

---
[🏠 Accueil](home)
""")

# QUICK START
wiki("quick-start-local", "Quick Start - Local", fpath="docs/QUICK_START_LOCAL.md")

wiki("quick-start-production", "Quick Start - Production", content="""# Quick Start - Production

Déploiement automatique sur VPS avec GitLab CI/CD.

## Configuration

**GitLab → Settings → CI/CD → Variables** - Ajouter 10 variables :
- DAGSTER_PG_HOST, DAGSTER_PG_PASSWORD
- PG_HOST, PG_PASSWORD, POSTGIS_HOST
- MINIO_ENDPOINT, MINIO_USER, MINIO_PASS, MINIO_REGION, MINIO_BRONZE_BUCKET

Voir [Variables GitLab](cicd-variables) pour détails.

## Déploiement

```bash
git push origin main
```

Pipeline (5-10 min) :
1. Build images (orchestrator + worker)
2. Deploy sur VPS
3. Health checks

## Accès

- Dagster UI: http://srv991054.hstgr.cloud:8080
- MinIO: http://srv991054.hstgr.cloud:9001

[🏠 Accueil](home)
""")

# GUIDES
wiki("guides-configuration", "Guide - Configuration", content="""# Configuration

## Fichiers

**Local** : `.env` (copier depuis `.env.template`)
**Production** : GitLab CI/CD Variables

## Variables principales

```bash
# Dagster
DAGSTER_PG_HOST=dagster_postgres
DAGSTER_PG_PASSWORD=xxx

# Data
PG_HOST=postgres
PG_PASSWORD=xxx
POSTGIS_HOST=postgis

# MinIO
MINIO_ENDPOINT=http://minio:9000
MINIO_USER=admin
MINIO_PASS=xxx
```

Voir [Environment Setup](guides-environment-setup) pour tous les détails.

[🏠 Accueil](home)
""")

wiki("guides-environment-setup", "Guide - Environment Setup", fpath="docs/ENVIRONMENT_CONFIGURATION.md")

# ARCHITECTURE
wiki("architecture-overview", "Architecture - Overview", fpath="docs/ARCHITECTURE.md")

wiki("architecture-stack", "Architecture - Stack", content="""# Stack Technique

## Orchestration - Dagster 1.11.14
- Webserver (UI)
- Daemon (scheduler)
- Worker (gRPC)

## ETL - DLT 0.4.12
- Extract: APIs Hub'Eau
- Transform: Python
- Load: Parquet → MinIO

## Storage
- **Bronze**: MinIO (Parquet)
- **Silver/Gold**: PostgreSQL 16 + PostGIS 3.4
- **Metadata**: PostgreSQL (Dagster)

## Pourquoi ?

**Dagster** : Data-aware, testable, monitoring built-in
**DLT** : Conçu pour APIs REST, schema inference, incremental loading
**MinIO** : S3-compatible, self-hosted, léger
**Parquet** : Colonnaire, compressé, standard

[🏠 Accueil](home)
""")

wiki("architecture-deployment", "Architecture - Deployment", content="""# Deployment

## Local
```
Docker Desktop
├─ Orchestrator :8080
├─ Worker gRPC:4000
├─ PostgreSQL :5432
├─ PostGIS :5433
└─ MinIO :9000
```

## Production VPS
```
srv991054.hstgr.cloud
├─ Orchestrator :8080 (public)
├─ Worker :4000 (internal)
├─ MinIO :9001 (public)
└─ Portainer :9443 (public)

Volumes: /srv/brgm-data/
```

## CI/CD Flow
```
Git Push → GitLab Runner → Build → Deploy → Health Checks
```

Voir [Pipeline](cicd-pipeline) pour détails.

[🏠 Accueil](home)
""")

# REFERENCE
wiki("reference-apis-hubeau", "Reference - APIs Hub'Eau", fpath="docs/APIS_HUBEAU_REFERENCE_COMPLETE.md")
wiki("reference-schema-bdd", "Reference - Schema BDD", fpath="docs/SCHEMA_BDD_HUBEAU.md")
wiki("reference-autres-referentiels", "Reference - Autres Referentiels", fpath="docs/AUTRES_REFERENTIELS.md")
wiki("reference-observability", "Reference - Observability", fpath="docs/OBSERVABILITY.md")

# DEVELOPMENT
wiki("development-dlt-tutorial", "Development - DLT Tutorial", fpath="docs/TUTORIEL_DLT.md")

wiki("development-tests", "Development - Tests", content="""# Tests

## Types

**Unit** : Fonctions individuelles
```python
def test_clean_code():
    assert clean_station_code("  A123  ") == "A123"
```

**Integration** : Assets Dagster
```python
def test_asset():
    context = build_asset_context()
    result = stations_hydrometrie(context)
    assert len(result) > 0
```

**E2E** : Pipeline complet
```bash
docker compose up -d
dagster job execute -j hydrometrie_job
pytest tests/test_e2e.py
```

## Lancer

```bash
pytest                          # Tous
pytest tests/test_utils.py      # Specific
pytest --cov=src/hubeau_pipeline  # Coverage
```

[🏠 Accueil](home)
""")

# CI/CD
wiki("cicd-pipeline", "CI/CD - Pipeline", content="""# Pipeline CI/CD

## Stages

**1. Build**
- Sync code → /srv/brgm
- Build orchestrator + worker images
- Tag with commit SHA

**2. Deploy**
- Export variables GitLab
- docker compose down
- docker compose up -d
- Wait 90s (health checks)
- Verify

## Rollback

**GitLab** : CI/CD → Pipelines → Rollback (manuel)

**Serveur** :
```bash
docker compose -f docker-compose.production.yml restart
```

Voir [.gitlab-ci.yml](https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/blob/main/.gitlab-ci.yml)

[🏠 Accueil](home)
""")

wiki("cicd-variables", "CI/CD - Variables", content="""# Variables GitLab

**GitLab → Settings → CI/CD → Variables**

## Requises (10)

**Dagster**
- DAGSTER_PG_HOST = dagster_postgres
- DAGSTER_PG_PASSWORD = (fort, 20+ chars)

**Data**
- PG_HOST = postgres
- PG_PASSWORD = (fort)
- POSTGIS_HOST = postgis

**MinIO**
- MINIO_ENDPOINT = http://minio:9000
- MINIO_USER = admin
- MINIO_PASS = (fort)
- MINIO_REGION = us-east-1
- MINIO_BRONZE_BUCKET = bronze

## Sécurité

✅ Passwords forts (20+ chars)
✅ Masked dans GitLab
✅ Protected variables
❌ Jamais commit .env
❌ Jamais log passwords

## Générer passwords

```bash
openssl rand -base64 32
```

[🏠 Accueil](home)
""")

# PROJECT
wiki("project-junon", "Project - JUNON Vision", fpath="docs/PROJET_JUNON_VISION.md")

print("\n" + "="*80)
print("WIKI COMPLET - https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/wikis/home")
print("="*80)
