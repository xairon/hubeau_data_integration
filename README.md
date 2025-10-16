# 🌊 Hub'Eau Data Integration

> **Pipeline d'intégration des données Hub'Eau pour le programme JUNON** (Jumeaux Numériques - BRGM Centre-Val de Loire)

[![Pipeline](https://img.shields.io/badge/status-production-success)](https://scm.univ-tours.fr/ringuet/hubeau_data_integration)
[![Dagster](https://img.shields.io/badge/Dagster-1.11.14-blue)](https://dagster.io)
[![Python](https://img.shields.io/badge/Python-3.11+-blue)](https://python.org)

---

## 🎯 Qu'est-ce que c'est ?

Ce projet est **la fondation data** de l'axe EAU du [programme JUNON](https://www.junon-cvl.fr/fr) (12,3M€, BRGM) visant à créer des **jumeaux numériques** pour la gestion des ressources en eau.

**En bref** : On intègre automatiquement **8 APIs Hub'Eau** (24 endpoints, 778 attributs) dans un data lake unifié pour servir de base au jumeau numérique hydrologique.

```
Hub'Eau (8 APIs) → DLT Pipeline → MinIO (Parquet) → [Futur: ML/IA/Jumeau Numérique]
                                                      Phase Bronze ✅
```

### Données intégrées

- 🌊 **Hydrométrie** : Stations, sites, débits, hauteurs d'eau
- 🏔️ **Piézométrie** : Stations, chroniques nappes phréatiques
- 🧪 **Qualité** : Stations, analyses physico-chimiques (rivières + nappes)
- 🌡️ **Température** : Stations, chroniques température continue
- 💧 **Écoulement (ONDE)** : Stations, observations assecs
- 🦠 **Hydrobiologie** : Stations, indices biologiques, taxons
- 💦 **Prélèvements** : Ouvrages, points, volumes prélevés

**Total** : 24 endpoints, 778 attributs documentés

---

## 🚀 Démarrage Rapide

### Pour les utilisateurs (sans Docker)

Explorez les données Hub'Eau directement en Python :

```bash
# Installation
pip install -r requirements.txt

# Tester
python test_local_simple.py  # → 3 CSV dans data/local_tests/

# Ou avec Jupyter
jupyter notebook notebooks/test_hubeau_wrapper.ipynb
```

### Pour les développeurs (avec Docker)

```bash
# 1. Configurer
cp .env.template .env
# Éditer .env avec vos mots de passe

# 2. Lancer
docker-compose up -d

# 3. Accéder
# Dagster UI: http://localhost:8080
# MinIO Console: http://localhost:9001
```

**Guide détaillé** : [docs/QUICK_START_LOCAL.md](docs/QUICK_START_LOCAL.md)

---

## 📁 Structure du Projet

```
hubeau_data_integration/
├── configs/hubeau/          # Configurations DLT (24 fichiers YAML)
├── src/
│   ├── dlt_pipeline/        # Pipeline DLT générique
│   └── hubeau_pipeline/     # Code Dagster (assets, jobs, schedules)
├── docker/                  # Dockerfiles (orchestrator + worker)
├── docs/                    # Documentation (voir ci-dessous)
└── scripts/                 # Scripts de déploiement
```

---

## 📚 Documentation

### 🚀 Guides de Démarrage

| Document | Description |
|----------|-------------|
| **[Quick Start Local](docs/QUICK_START_LOCAL.md)** | Installation et premiers pas (dev local) |
| **[Tutoriel DLT](docs/TUTORIEL_DLT.md)** | Configurer et personnaliser le pipeline |
| **[GitLab CI/CD Setup](GITLAB_CI_SETUP.md)** | Déploiement automatique en production |

### 📖 Documentation Technique

| Document | Description |
|----------|-------------|
| **[Architecture](docs/ARCHITECTURE.md)** | Stack technique actuelle (Dagster + DLT + MinIO) |
| **[Environment Configuration](docs/ENVIRONMENT_CONFIGURATION.md)** | Configuration multi-environnements (dev/staging/prod) |
| **[Contributing](CONTRIBUTING.md)** | Guide de contribution |

### 📊 Référence Données

| Document | Description |
|----------|-------------|
| **[APIs Hub'Eau - Référence Complète](docs/APIS_HUBEAU_REFERENCE_COMPLETE.md)** | Schémas des 8 APIs (778 attributs) |
| **[Autres Référentiels](docs/AUTRES_REFERENTIELS.md)** | SANDRE, BDLISA, COG, NQE, TAXREF |
| **[Schéma BDD](docs/SCHEMA_BDD_HUBEAU.md)** | Design de la base de données Silver/Gold |

### 🔮 Vision & Roadmap

| Document | Description |
|----------|-------------|
| **[Vision JUNON](docs/PROJET_JUNON_VISION.md)** | Contexte programme JUNON, jumeau numérique, ontologie SOSA |
| **[Observability](docs/OBSERVABILITY.md)** | Monitoring avancé (Prometheus, Grafana) |

---

## 🏗️ Architecture

### Stack Actuel ✅ (Phase Bronze - Production)

| Composant | Technologie | Rôle |
|-----------|-------------|------|
| **Orchestration** | Dagster 1.11.14 | Workflow, scheduling, monitoring |
| **Data Loading** | DLT 0.4.12 | Extraction Hub'Eau → Parquet |
| **Data Lake** | MinIO (S3) | Stockage Parquet (Bronze) |
| **Databases** | PostgreSQL + PostGIS | Métadonnées & données géospatiales |
| **Monitoring** | Portainer CE | Gestion containers Docker |

### Roadmap 🚧 (Phases Silver/Gold)

| Composant | Technologie | Status | Rôle |
|-----------|-------------|--------|------|
| **Time Series** | TimescaleDB | Roadmap | Chroniques optimisées (> 100M lignes) |
| **Graph DB** | Neo4j | Roadmap | Ontologie SOSA/SANDRE |
| **ML/IA** | TBD | Roadmap | Modèles prédictifs |

**Architecture détaillée** : [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

---

## 🎮 Utilisation

### Lancer un Job Dagster

```bash
# Via UI: http://localhost:8080 → Jobs → Execute

# Via CLI:
dagster job execute -j sync_all_stations  # Toutes les stations de référence
dagster job execute -j hubeau_hydrometry_job  # Job hydrométrie complet

# Par partition annuelle:
dagster asset materialize -a temperature_chroniques --partition 2024
```

### Accès aux Données (MinIO)

```python
import boto3

s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='your_password'
)

# Lister les fichiers
s3.list_objects_v2(Bucket='bronze', Prefix='hydrometry_api/')
```

---

## 🤝 Contribution

1. Fork du projet
2. Créer une branche : `git checkout -b feature/ma-feature`
3. Commit : `git commit -m "feat: description"`
4. Push : `git push origin feature/ma-feature`
5. Créer une Merge Request

**Guide complet** : [CONTRIBUTING.md](CONTRIBUTING.md)

---

## 🔗 Liens Utiles

- **Programme JUNON** : https://www.junon-cvl.fr/fr
- **Hub'Eau** : https://hubeau.eaufrance.fr
- **SANDRE** : https://www.sandre.eaufrance.fr
- **GitLab** : https://scm.univ-tours.fr/ringuet/hubeau_data_integration

---

## 📄 Licence

Projet développé dans le cadre du **programme JUNON** (BRGM) et de l'**Université de Tours**.

**Auteur** : Nicolas Ringuet
**Contact** : [Formulaire de contact JUNON](https://www.junon-cvl.fr/fr/contact)
