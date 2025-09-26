# 🌊 Hub'Eau Data Integration Pipeline
## Pipeline de Données Hydrologiques Françaises - Architecture Moderne

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/release/python-311/)
[![Dagster](https://img.shields.io/badge/orchestrator-Dagster-orange.svg)](https://dagster.io/)
[![Docker](https://img.shields.io/badge/deployment-Docker-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## 🎯 **Vision & Objectifs**

Ce pipeline **modernise l'intégration des données hydrologiques françaises** en unifiant **8 APIs Hub'Eau officielles** avec les **référentiels nationaux** (BDLISA géologique, Sandre thématique) et les **standards internationaux** (SOSA/SSN W3C).

### **🌟 Pourquoi ce Projet ?**

#### **🔄 Défis Actuels**
- **Fragmentation** : Données éparpillées sur 8+ APIs différentes
- **Hétérogénéité** : Formats, unités, nomenclatures variables
- **Volume** : 140K+ observations/jour en production
- **Complexité** : Croisement spatial, temporel, thématique difficile

#### **🚀 Solutions Apportées**
- **Pipeline unifié** : Ingestion automatisée 8 APIs Hub'Eau
- **Architecture moderne** : Dagster + Docker + bases spécialisées
- **Modèle sémantique** : Standards W3C pour interopérabilité
- **Architecture modulaire** : Scaling et maintenance optimisés

---

## 📊 **Sources de Données Intégrées**

### **🌊 8 APIs Hub'Eau Officielles**
| **API** | **Stations** | **Données** | **Fréquence** |
|---------|--------------|-------------|---------------|
| **🏔️ Piézométrie** | ~1,500 | Niveaux nappes | Horaire |
| **🌊 Hydrométrie** | ~3,000 | Débits/hauteurs | Temps réel |
| **🧪 Qualité Surface** | ~2,000 | Analyses physicochimiques | Hebdomadaire |
| **🧪 Qualité Nappes** | ~1,200 | Analyses souterraines | Mensuelle |
| **🌡️ Température** | ~800 | Température continue | Horaire |
| **🌊 Écoulement ONDE** | ~3,200 | Observations visuelles | Saisonnière |
| **🐟 Hydrobiologie** | ~1,500 | Indices biologiques | Campagnes |
| **🚰 Prélèvements** | National | Volumes prélevés | Déclarations |

### **🗺️ Référentiels Nationaux**
- **BDLISA** (BRGM) : Formations aquifères, contexte hydrogéologique
- **Sandre** (OFB) : Nomenclatures officielles, thésaurus eau

### **🔗 Standards Internationaux**
- **SOSA/SSN** (W3C) : Ontologie capteurs/observations pour interopérabilité

---

## 🚀 **Quick Start**

### **⚡ Démarrage Rapide (5 minutes)**

```bash
# 1. Clone du repository
git clone https://github.com/your-org/hubeau-pipeline
cd hubeau-pipeline

# 2. Configuration environnement
cp env.example .env
# Éditer .env avec vos credentials

# 3. Démarrage infrastructure complète
docker-compose up -d

# 4. Vérification santé
docker-compose exec timescaledb pg_isready
docker-compose exec neo4j cypher-shell "RETURN 'Neo4j ready'"
docker-compose exec postgis pg_isready -p 5432

# 5. Interface Dagster
open http://localhost:3000
```

### **🎛️ Services Disponibles**
| **Service** | **URL** | **Usage** |
|-------------|---------|-----------|
| **Dagster UI** | http://localhost:3000 | Interface orchestration |
| **MinIO Console** | http://localhost:9001 | Stockage objets |
| **Neo4j Browser** | http://localhost:7474 | Exploration graphe |
| **pgAdmin** | http://localhost:5050 | Administration PostgreSQL |
| **TimescaleDB** | localhost:5432 | Connexion directe |
| **PostGIS** | localhost:5433 | Connexion directe |

---

## 📊 **Architecture & Performance**

### **🥉🥈🥇 Medallion Architecture**
- **Bronze** : Données brutes (MinIO Object Storage)
- **Silver** : Bases spécialisées (TimescaleDB + PostGIS + Neo4j)
- **Gold** : Knowledge Graph unifié (SOSA/Future)

### **🎯 Configuration Développement**
```yaml
APIs_Intégrées: 8 (Hub'Eau + externes)
Sources_Données: 11 total
Bases_Spécialisées: 3 (TimescaleDB + PostGIS + Neo4j)
Orchestration: Dagster (assets + jobs + schedules)
```

---

## 🔬 **Vision Future : Knowledge Graph & IA**

### **🧠 Couche SOSA/KG (Non Implémentée)**

Cette couche représente l'**évolution future** du pipeline vers un **Knowledge Graph unifié** :

#### **🎯 Objectifs Visionnaires**
- **Business Intelligence** : Dashboards cross-sources unifiés
- **Machine Learning** : Entraînement modèles sur graphe enrichi
- **Explicabilité** : Traçabilité complète observations
- **Recherche Naturelle** : Interface conversationnelle LLM

#### **🚀 Technologies Envisagées**
- **GraphRAG** : Retrieval-Augmented Generation sur graphe
- **Neo4j Vector Search** : Embeddings sémantiques
- **LangChain/LlamaIndex** : Orchestration LLM + graphe

---

## 📚 **Documentation Complète**

### **📖 Guides Techniques**
- [📊 Sources de Données Complètes](docs/DATA_SOURCES_COMPLETE.md)
- [🏗️ Architecture Technique](docs/TECHNICAL_ARCHITECTURE.md)  
- [🔗 Vision SOSA/Knowledge Graph](docs/SOSA_FUTURE_VISION.md)
- [🎯 Stratégie Stockage Données](docs/DATA_STORAGE_STRATEGY.md)

---

## 🔧 **Développement**

### **📁 Structure**
```
src/hubeau_pipeline/
├── assets/              # Assets Dagster (Bronze/Silver/Gold)
├── jobs/                # Jobs orchestration
├── resources/           # Connexions bases données
├── schedules/           # Planification temporelle
└── sensors/             # Surveillance & alerting
```

### **🧪 Tests**
```bash
pytest tests/ -v
pytest tests/integration/ --docker
```

---

## 📄 **Licence & Crédits**

**Licence MIT** - Projet Open Source

**Remerciements :**
- **Hub'Eau** (OFB) : APIs données hydrologiques
- **BRGM** : Référentiel BDLISA hydrogéologique  
- **Sandre** (OFB + OiEau) : Nomenclatures officielles
- **W3C** : Standards SOSA/SSN sémantiques

---

**🌊 Construisons ensemble l'avenir de l'analyse des données hydrologiques françaises !**

[![Dagster](https://img.shields.io/badge/powered%20by-Dagster-orange)](https://dagster.io/)
[![France](https://img.shields.io/badge/made%20in-France-blue)](https://www.eaufrance.fr/)
[![Open Source](https://img.shields.io/badge/open-source-green)](https://github.com/)
