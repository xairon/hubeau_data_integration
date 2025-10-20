# 🌊 Hub'Eau Data Integration

> **Pipeline d'intégration des données Hub'Eau pour le programme JUNON** (Jumeaux Numériques - BRGM Centre-Val de Loire)

[![Pipeline](https://img.shields.io/badge/status-production-success)](https://scm.univ-tours.fr/ringuet/hubeau_data_integration)
[![Dagster](https://img.shields.io/badge/Dagster-1.11.14-blue)](https://dagster.io)
[![Python](https://img.shields.io/badge/Python-3.11+-blue)](https://python.org)

---

## 🎯 Qu'est-ce que c'est ?

Ce projet est **la fondation data** de l'axe EAU du [programme JUNON](https://www.junon-cvl.fr/fr) (12,3M€, BRGM) visant à créer des **jumeaux numériques** pour la gestion des ressources en eau.

**En bref** : On intègre automatiquement **8 APIs Hub'Eau** (24 endpoints, 778 attributs) dans une base de données PostgreSQL unifiée pour servir de base au jumeau numérique hydrologique.

```
Hub'Eau (8 APIs) → DLT Pipeline → PostgreSQL (schema: hubeau) → [Futur: ML/IA/Jumeau Numérique]
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
# Adminer: http://localhost:8081
# PgAdmin: http://localhost:5050
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
| **[Architecture](docs/ARCHITECTURE.md)** | Stack technique actuelle (Dagster + DLT + PostgreSQL) |
| **[Environment Configuration](docs/ENVIRONMENT_CONFIGURATION.md)** | Configuration multi-environnements (dev/staging/prod) |
| **[Contributing](CONTRIBUTING.md)** | Guide de contribution |

### 📊 Référence Données

| Document | Description |
|----------|-------------|
| **[APIs Hub'Eau - Référence Complète](docs/APIS_HUBEAU_REFERENCE_COMPLETE.md)** | Schémas des 8 APIs (778 attributs) |
| **[Autres Référentiels](docs/AUTRES_REFERENTIELS.md)** | SANDRE, BDLISA, COG, NQE, TAXREF |
| **[Schéma BDD](docs/SCHEMA_BDD_HUBEAU.md)** | Design de la base de données PostgreSQL |

### 🔮 Vision & Roadmap

| Document | Description |
|----------|-------------|
| **[Vision JUNON](docs/PROJET_JUNON_VISION.md)** | Contexte programme JUNON, jumeau numérique, ontologie SOSA |
| **[Observability](docs/OBSERVABILITY.md)** | Monitoring avancé (Prometheus, Grafana) |

---

## 🏗️ Architecture

### Data Flow

```
Hub'Eau APIs → DLT → PostgreSQL (schema: hubeau)
```

### Services

- **Dagster**: Orchestration (http://localhost:8080)
- **PostgreSQL**: Hub'Eau data + metadata
- **Adminer**: Lightweight DB admin (http://localhost:8081)
- **PgAdmin**: Full-featured DB admin (http://localhost:5050)
- **PostGIS**: Geospatial transformations
- **Prometheus + Grafana**: Monitoring

### Database Structure

All Hub'Eau data is stored in PostgreSQL under the `hubeau` schema:

- **DLT manages**: Schema creation, migrations, and incremental loading
- **write_disposition=merge**: Upsert based on primary keys (default for most tables)
- **write_disposition=replace**: Full table replacement (for reference data)
- **State tracking**: `_dlt_pipeline_state` table tracks pipeline state and incremental loads

### Roadmap 🚧

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

### Accès aux Données (PostgreSQL)

```python
import psycopg2

# Connection
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='hubeau_db',
    user='hubeau_user',
    password='your_password'
)

# Query Hub'Eau data
cursor = conn.cursor()
cursor.execute("SELECT * FROM hubeau.hydrometry_stations LIMIT 10")
stations = cursor.fetchall()
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
