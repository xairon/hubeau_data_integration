# 🌊 Hub'Eau Data Integration - Axe EAU du Programme JUNON

Pipeline d'intégration des données Hub'Eau pour le programme **JUNON** (Jumeaux Numériques au service des ressources naturelles, BRGM).

> **Programme officiel** : [JUNON](https://www.junon-cvl.fr/fr) | Budget : 12,3M€ | BRGM Centre-Val de Loire  
> **Dépôt GitLab** : https://scm.univ-tours.fr/ringuet/hubeau_data_integration

---

## 📋 Table des Matières

- [Contexte : Programme JUNON](#-contexte--programme-junon)
- [Notre Contribution : Axe EAU](#-notre-contribution--axe-eau)
- [Architecture & Technologies](#-architecture--technologies)
- [Installation Rapide](#-installation-rapide)
- [Données Intégrées](#-données-intégrées)
- [Jobs Dagster](#-jobs-dagster)
- [Documentation](#-documentation)
- [Structure du Projet](#-structure-du-projet)

---

## 🎯 Contexte : Programme JUNON

### Qu'est-ce que JUNON ?

**JUNON** est un programme officiel du BRGM (Bureau de Recherches Géologiques et Minières) doté de **12,3 millions d'euros** sur **5 ans**, visant à développer des **jumeaux numériques** pour la gestion des ressources naturelles en région Centre-Val de Loire.

**Définition officielle** (Sébastien Dupraz, Coordinateur JUNON) :
> "Un jumeau numérique est une reproduction virtuelle d'un objet ou d'un environnement qui, grâce à des méthodes d'intelligence artificielle, simule le comportement de son double réel afin de mieux le comprendre et le gérer."

### Les 5 Axes du Programme

1. **🌊 EAU** - Gestion ressources en eau (notre projet)
2. **🌱 SOL/AIR** - Qualité sols et atmosphère
3. **💾 DATA** - Infrastructure de données et interopérabilité
4. **🔮 PRÉDICTION** - Modélisation prédictive et IA
5. **🎯 JUMEAUX NUMÉRIQUES** - Développement des jumeaux numériques

**Plus d'infos** : [junon-cvl.fr](https://www.junon-cvl.fr/fr)

---

## 🌊 Notre Contribution : Axe EAU

Ce projet constitue **la fondation data** de l'axe EAU du programme JUNON :

### Objectif
Créer un **entrepôt de données unifié** des ressources en eau (nappes, cours d'eau) en intégrant **8 APIs Hub'Eau** (portail national des données sur l'eau) pour servir de base au **jumeau numérique hydrologique**.

### Approche en 3 Phases

```
Phase 1 ✅ IMPLÉMENTÉ
┌─────────────────────────────────────────┐
│   BRONZE - Intégration Données Brutes  │
│                                         │
│  Hub'Eau (8 APIs, 24 endpoints)        │
│         ↓ DLT Pipeline                  │
│  MinIO (Parquet, partitions annuelles) │
└─────────────────────────────────────────┘

Phase 2 🚧 EN COURS
┌─────────────────────────────────────────┐
│   SILVER - Harmonisation & Nettoyage   │
│                                         │
│  Enrichissement avec référentiels       │
│  (SANDRE, BDLISA, COG, NQE, etc.)      │
└─────────────────────────────────────────┘

Phase 3 📋 ROADMAP
┌─────────────────────────────────────────┐
│   ONTOLOGIE - Modèle Sémantique SOSA   │
│                                         │
│  Graphe de connaissances unifié         │
│  → Base du jumeau numérique             │
└─────────────────────────────────────────┘
```

### Vision : Architecture SOSA

Le jumeau numérique s'appuiera sur l'ontologie **SOSA** (Sensor, Observation, Sample, Actuator - Standard W3C) pour unifier toutes les sources de données hétérogènes dans un modèle sémantique cohérent.

**Voir** : [Documentation complète JUNON](docs/PROJET_JUNON_VISION.md)

---

## 🏗️ Architecture & Technologies

### État Actuel ✅ (Phase Bronze)

| Composant | Technologie | Statut | Rôle |
|-----------|-------------|--------|------|
| **Orchestration** | Dagster 1.5+ | ✅ Prod | Workflow, scheduling, monitoring |
| **Data Loading** | DLT Custom | ✅ Prod | Extraction Hub'Eau → Parquet |
| **Stockage Bronze** | MinIO (S3) | ✅ Prod | Data lake Parquet |

### Roadmap 🚧 (Phases Silver/Gold)

| Composant | Technologie | Statut | Rôle |
|-----------|-------------|--------|------|
| **Time Series** | TimescaleDB | 🚧 Roadmap | Chroniques optimisées |
| **Geospatial** | PostGIS | 🚧 Roadmap | Analyses spatiales |
| **Graph** | Neo4j | 🚧 Roadmap | Ontologie SOSA/SANDRE |

**Architecture complète** : [Architecture Technique](docs/ARCHITECTURE_MODERNE.md)

---

## 🚀 Installation & Déploiement

### Développement Local

**Prérequis** : Docker, Docker Compose, Python 3.11+

```bash
# 1. Cloner le projet
git clone https://scm.univ-tours.fr/ringuet/hubeau_data_integration.git
cd hubeau_data_integration

# 2. Configurer les variables d'environnement
cp env.example .env
nano .env  # Éditer avec vos mots de passe

# 3. Démarrer les services
docker-compose up -d

# 4. Accès aux services
# - Dagster UI : http://localhost:8080
# - MinIO Console : http://localhost:9001
```

### Déploiement Production (GitLab CI/CD)

**Automatique sur push vers `main`** :

1. **Configurer les secrets GitLab** (Settings > CI/CD > Variables) :
   - `DAGSTER_PG_PASSWORD`
   - `MINIO_USER`
   - `MINIO_PASS`

2. **Push vers main** :
   ```bash
   git push origin main
   ```

3. **GitLab CI/CD** :
   - Build image Docker
   - Génère `.env.production` depuis secrets
   - Déploie automatiquement
   - Health checks

**Le runner doit avoir** :
- Docker installé
- Tag `hubeau` configuré
- Accès aux volumes de données

**Détails** : [scripts/README.md](scripts/README.md)

### Premier Job

```bash
# Stations de référence
dagster job execute -j sync_all_stations

# Données annuelles (partition 2024)
dagster asset materialize -a temperature_chroniques --partition 2024
```

**Guide complet** : [Tutoriel DLT](docs/TUTORIEL_DLT.md)

---

## 📊 Données Intégrées

### 8 APIs Hub'Eau - 24 Endpoints

| API | Endpoints | Description |
|-----|-----------|-------------|
| **Hydrométrie** | 3 | Stations, sites, observations élaborées (débits/hauteurs) |
| **Piézométrie** | 3 | Stations, chroniques temps réel, chroniques historiques |
| **Qualité Cours d'Eau** | 4 | Stations, analyses, opérations, conditions environnementales |
| **Qualité Nappes** | 2 | Stations, analyses physico-chimiques |
| **Température** | 2 | Stations, chroniques température en continu |
| **Écoulement (ONDE)** | 3 | Stations, observations, campagnes |
| **Hydrobiologie** | 3 | Stations, indices biologiques, taxons |
| **Prélèvements** | 3 | Ouvrages, points, chroniques volumes |

**Total attributs documentés** : **778 champs**

**Documentation complète** : [APIs Hub'Eau - Référence Complète](docs/APIS_HUBEAU_REFERENCE_COMPLETE.md)

---

## 🎮 Jobs Dagster

### Architecture des Jobs

Les données sont organisées en **chaînes logiques** :

```
Station Référence (pas de partition)
    ↓
Observations/Chroniques (partitions annuelles 2020-2025)
```

### Jobs Disponibles

```bash
# 📍 Toutes les stations de référence (exécution unique)
dagster job execute -j sync_all_stations

# 📈 Toutes les données annuelles (historique 2020-2025)
dagster job execute -j sync_all_yearly_data

# 🌊 Job Hydrométrie (stations → sites → obs_elab)
dagster job execute -j hubeau_hydrometry_job

# 🏔️ Job Piézométrie (stations → chroniques)
dagster job execute -j hubeau_piezometry_job

# 🧪 Job Qualité Cours d'Eau (stations → analyses + opérations + conditions)
dagster job execute -j hubeau_quality_rivers_job

# 🌡️ Job Température (stations → chroniques)
dagster job execute -j hubeau_temperature_job
```

### Exécution par Partition

```bash
# Matérialiser une partition spécifique
dagster asset materialize -a temperature_chroniques --partition 2024
dagster asset materialize -a hydrometry_obs_elab --partition 2023

# Backfill multi-partitions
dagster asset materialize -a piezometry_chroniques --partition 2020,2021,2022,2023,2024
```

### Schedule Automatique

**Schedule annuel** : 1er janvier à 3h du matin
- Exécute `sync_all_yearly_data`
- Collecte données de l'année précédente

---

## 📁 Structure du Projet

```
hubeau_data_integration/
├── configs/hubeau/              # ✅ Configurations DLT (24 fichiers YAML)
│   ├── hydrometry_*.yml
│   ├── piezometry_*.yml
│   ├── quality_*.yml
│   ├── temperature_*.yml
│   └── ...
│
├── pipelines/dlt/               # ✅ Pipeline DLT générique
│   ├── hubeau_generic.py        # Source DLT Hub'Eau
│   ├── slicing.py               # Découpage intelligent
│   └── http_client.py           # Client HTTP avec retry
│
├── src/hubeau_pipeline/         # ✅ Code Dagster
│   ├── assets/bronze/           # Assets DLT par API
│   │   └── dlt_assets.py
│   ├── jobs/dlt_jobs.py         # Jobs Dagster
│   ├── schedules/               # Planification
│   └── definitions.py           # Définitions Dagster
│
├── docker/                      # ✅ Configuration Docker
│   └── dagster/Dockerfile
│
├── scripts/                     # ✅ Déploiement
│   └── README.md                # Configuration GitLab CI/CD
│
└── docs/                        # ✅ Documentation complète
    ├── APIS_HUBEAU_REFERENCE_COMPLETE.md    # Schémas 8 APIs (778 attributs)
    ├── AUTRES_REFERENTIELS.md               # SANDRE, BDLISA, COG, NQE...
    ├── ARCHITECTURE_MODERNE.md              # Architecture technique
    ├── TUTORIEL_DLT.md                      # Guide configuration DLT
    └── PROJET_JUNON_VISION.md               # Vision jumeau numérique
```

---

## 📚 Documentation

### Documentation Technique

| Document | Description | Audience |
|----------|-------------|----------|
| **[APIs Hub'Eau](docs/APIS_HUBEAU_REFERENCE_COMPLETE.md)** | Référence exhaustive des 8 APIs intégrées (778 attributs documentés) | Utilisateurs, développeurs |
| **[Tutoriel DLT](docs/TUTORIEL_DLT.md)** | Guide des configurations YAML, modes de slicing, optimisations | Développeurs |
| **[Architecture Technique](docs/ARCHITECTURE_MODERNE.md)** | Stack technique, choix architecturaux, état d'implémentation | Architectes, DevOps |
| **[Autres Référentiels](docs/AUTRES_REFERENTIELS.md)** | Guide d'intégration SANDRE, BDLISA, COG, NQE, TAXREF | Data engineers |

### Documentation Projet

| Document | Description | Audience |
|----------|-------------|----------|
| **[Vision JUNON](docs/PROJET_JUNON_VISION.md)** | Contexte programme JUNON, vision jumeau numérique, ontologie SOSA | Management, chercheurs |
| **[Déploiement](scripts/README.md)** | Configuration GitLab CI/CD pour déploiement automatique | DevOps |

---

## 🔧 Caractéristiques Techniques

### Pipeline DLT Intelligent

- ✅ **5 modes de slicing** : Global, datetime, dept, station_month_chunked, dept_datetime
- ✅ **Pagination automatique** : Page-based et cursor-based (20K records/page)
- ✅ **Fallbacks sur troncature** : Découpage automatique si limite API atteinte
- ✅ **Rate limiting adaptatif** : 0.5-2.0 req/s selon API
- ✅ **Filtrage stations actives** : Évite requêtes inutiles (gain 90%+ sur certaines APIs)
- ✅ **Garbage collection** : Optimisation mémoire (évite OOM)
- ✅ **Retry intelligent** : Backoff exponentiel 2s → 120s

### Optimisations Mémoire

- ✅ **In-process executor** : Tous les jobs (évite overhead multiprocess)
- ✅ **Garbage collection explicite** : Après chaque slice
- ✅ **Buffered batches cleanup** : Libération mémoire après traitement
- ✅ **Partitionnement annuel** : Charge data year-by-year

### Stockage Bronze (MinIO)

```
hubeau-bronze/
├── hydrometry_api/
│   ├── hydrometry_stations/*.parquet
│   ├── hydrometry_sites/*.parquet
│   └── hydrometry_obs_elab/year={2020,2021,...,2025}/*.parquet
│
├── quality_rivers_api/
│   ├── quality_rivers_stations/*.parquet
│   └── quality_rivers_analyses/year={2020,...,2025}/*.parquet
│
└── {api_name}/{endpoint}/year={YYYY}/*.parquet
```

**Format** : Parquet compressé (Snappy)  
**Métadonnées** : `_load_id`, `_slice_id`, `_dlt_load_timestamp`

---

## 🔍 Monitoring

### Dagster UI (http://localhost:8080)

- ✅ **Assets Graph** : Visualisation des dépendances
- ✅ **Runs** : Historique d'exécution avec logs détaillés
- ✅ **Partitions** : Vue par année (2020-2025)
- ✅ **Lineage** : Traçabilité des données
- ✅ **Métriques** : Records, durée, requêtes API

### Logs Détaillés

```bash
# Logs temps réel
docker-compose logs -f dagster_daemon

# Logs spécifiques API
docker-compose logs dagster_daemon | grep hydrometry

# Logs d'une exécution
dagster run logs <run_id>
```

---

## 🚀 Développement & Contribution

### Ajouter une Nouvelle API

1. Créer config YAML : `configs/hubeau/nouvelle_api.yml`
2. Définir asset Dagster : `src/hubeau_pipeline/assets/bronze/dlt_assets.py`
3. Ajouter au job : `src/hubeau_pipeline/jobs/dlt_jobs.py`
4. Tester : Dagster UI → Jobs → Execute

**Guide complet** : [Tutoriel DLT](docs/TUTORIEL_DLT.md)

### Modifier une Configuration

1. Éditer le YAML : `configs/hubeau/api.yml`
2. Commit : `git add . && git commit -m "fix: ..."`
3. Push : `git push origin main`
4. Déploiement automatique via GitLab CI/CD

### Tests

```bash
# Vérifier une configuration DLT
python -c "from pipelines.dlt.hubeau_generic import validate_config; validate_config('configs/hubeau/api.yml')"

# Tester un asset
dagster asset materialize -a temperature_stations_reference
```

---

## 📖 Documentation Complète

### Par Thème

**🚀 Démarrage Rapide**
- Ce README
- [Tutoriel DLT](docs/TUTORIEL_DLT.md) - Guide pratique

**📊 Données**
- [APIs Hub'Eau - Référence Complète](docs/APIS_HUBEAU_REFERENCE_COMPLETE.md) - 778 attributs documentés
- [Autres Référentiels](docs/AUTRES_REFERENTIELS.md) - SANDRE, BDLISA, COG, etc.

**🏗️ Architecture**
- [Architecture Technique](docs/ARCHITECTURE_MODERNE.md) - Stack & choix techniques
- [Vision JUNON](docs/PROJET_JUNON_VISION.md) - Contexte jumeau numérique

**⚙️ Production**
- [Déploiement](scripts/README.md) - GitLab CI/CD

### Par Audience

| Rôle | Documents Recommandés |
|------|----------------------|
| **Data User** | APIs Hub'Eau, Autres Référentiels |
| **Developer** | Tutoriel DLT, Architecture Technique, ce README |
| **DevOps** | Architecture Technique, Scripts Production |
| **Researcher** | Vision JUNON, APIs Hub'Eau, Autres Référentiels |
| **Manager** | Vision JUNON, Architecture Technique |

---

## 🤝 Contribution & Contact

### Workflow de Contribution

1. Fork du projet
2. Créer branche feature : `git checkout -b feature/ma-feature`
3. Développer et tester
4. Commit : `git commit -m "feat: description"`
5. Push : `git push origin feature/ma-feature`
6. Créer Merge Request vers `main`

### GitLab CI/CD

Le déploiement est **automatique** sur push vers `main` :
1. Build image Docker
2. Génération `.env.production` depuis secrets GitLab
3. Déploiement automatique sur serveur cible
4. Health checks automatiques

**Variables secrets** : Settings > CI/CD > Variables

---

## 📄 Licence

Projet développé dans le cadre du **programme JUNON** (BRGM) et de l'**Université de Tours**.

---

## 🔗 Liens Utiles

### Programme JUNON
- [Site officiel JUNON](https://www.junon-cvl.fr/fr)
- [BRGM](https://www.brgm.fr)

### Sources de Données
- [Hub'Eau](https://hubeau.eaufrance.fr) - Portail national données eau
- [SANDRE](https://www.sandre.eaufrance.fr) - Référentiels eau
- [BDLISA](https://bdlisa.eaufrance.fr) - Référentiel hydrogéologique

### Standards & Ontologies
- [SOSA/SSN (W3C)](https://www.w3.org/TR/vocab-ssn/) - Ontologie observations
- [GeoSPARQL (OGC)](https://www.ogc.org/standards/geosparql) - Données géospatiales
- [INSPIRE (EU)](https://inspire.ec.europa.eu) - Directive européenne
