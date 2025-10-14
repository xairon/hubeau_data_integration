# Architecture Technique - État Actuel

## Vue d'Ensemble

### Architecture Implémentée (Bronze Layer)

```
┌─────────────────────────────────────────────────────────────┐
│                      SOURCES (8 APIs)                       │
│                                                             │
│  Hydrométrie  │  Piézométrie  │  Qualité Cours d'Eau       │
│  Qualité Nappes  │  Température  │  Écoulement             │
│  Hydrobiologie  │  Prélèvements                            │
│                                                             │
│  📊 24 endpoints intégrés                                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼ DLT Pipeline (Generic)
                            │
┌─────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION                            │
│                                                             │
│                      🎯 DAGSTER                             │
│                                                             │
│  • Assets DLT (définition déclarative)                      │
│  • Jobs par API avec dépendances                            │
│  • Partitions annuelles (2020-2025)                         │
│  • In-process executor (optimisation mémoire)               │
│  • Schedules annuels (1er janvier)                          │
│  • Monitoring & lineage intégré                             │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼ run_pipeline()
                            │
┌─────────────────────────────────────────────────────────────┐
│                   STOCKAGE BRONZE                           │
│                                                             │
│                      📦 MINIO                               │
│                  (S3-Compatible)                            │
│                                                             │
│  Structure :                                                │
│  └── hubeau_api/                                            │
│      └── {endpoint_name}/                                   │
│          └── year={YYYY}/                                   │
│              └── *.parquet                                  │
│                                                             │
│  • Format : Parquet (compression)                           │
│  • Partitionnement : API + Année                            │
│  • Métadonnées : Timestamps, counts, slice_id              │
│  • Buckets : hubeau-bronze                                  │
└─────────────────────────────────────────────────────────────┘
```

### Architecture Planifiée (Silver/Gold Layers - Roadmap)

```
                    ┌─────────────────────────────┐
                    │      COUCHE SILVER          │
                    │      (En développement)     │
                    │                             │
                    │ ┌───────────┬─────────────┐ │
                    │ │TimescaleDB│   PostGIS   │ │
                    │ │Time Series│ Geospatial  │ │
                    │ └───────────┴─────────────┘ │
                    │ ┌───────────┐               │
                    │ │   Neo4j   │               │
                    │ │   Graph   │               │
                    │ └───────────┘               │
                    └─────────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────────────┐
                    │       COUCHE GOLD           │
                    │       (Roadmap)             │
                    │                             │
                    │  • Dashboards               │
                    │  • APIs Analytics           │
                    │  • Rapports                 │
                    └─────────────────────────────┘
```

## Choix Architecturaux (État Actuel)

### 🎯 Orchestration : Dagster
**Statut** : ✅ **Implémenté**  
**Justification** : Pipeline complexe avec dépendances, retry automatique, monitoring intégré

**Fonctionnalités utilisées** :
- ✅ Assets DLT déclaratifs avec lineage automatique
- ✅ Jobs par API avec chaînage logique (stations → observations)
- ✅ Partitions annuelles (`YEARLY_PARTITIONS`: 2020-2025)
- ✅ In-process executor (optimisation mémoire, évite OOM)
- ✅ Schedules annuels (1er janvier à 3h)
- ✅ UI intégrée pour monitoring et debugging
- ✅ Gestion automatique des dépendances entre assets

**Configuration actuelle** :
```python
# Tous les jobs utilisent in_process_executor
hydrometry_job = define_asset_job(
    name="hubeau_hydrometry_job",
    selection=AssetSelection.assets(...),
    executor_def=in_process_executor,  # Optimisation mémoire
)
```

### 📦 Data Lake : MinIO
**Statut** : ✅ **Implémenté** (Couche Bronze uniquement)  
**Justification** : Stockage S3-compatible, coût réduit, intégration Docker native

**Implémentation actuelle** :
- ✅ Compatible AWS S3 (migration future possible)
- ✅ Bucket bronze : `hubeau-bronze`
- ✅ Format : Parquet avec compression
- ✅ Partitionnement : `{api_name}/{endpoint}/year={YYYY}/`
- ✅ Métadonnées d'ingestion : `_slice_id`, `_load_id`, timestamps
- ✅ Accès programmatique via `boto3` et `pyarrow`

**Structure de stockage** :
```
hubeau-bronze/
├── hydrometry_api/
│   ├── hydrometry_stations/
│   │   └── *.parquet
│   └── hydrometry_obs_elab/
│       ├── year=2020/
│       ├── year=2021/
│       └── year=2022/
└── quality_rivers_api/
    └── quality_rivers_analyses/
        └── year=2024/
```

### 🔮 Bases de Données Spécialisées (Roadmap)

#### TimescaleDB (Time Series)
**Statut** : 🚧 **Planifié** (Couche Silver)  
**Justification** : Optimisé pour données temporelles, compression automatique, requêtes analytiques

**Cas d'usage prévus** :
- Hypertables pour séries temporelles (chroniques, observations)
- Compression automatique sur données historiques
- Fonctions analytiques (LAG, LEAD, window functions)
- Agrégations continues pour dashboards

#### PostGIS (Geospatial)
**Statut** : 🚧 **Planifié** (Couche Silver)  
**Justification** : Standard industriel pour analyses spatiales

**Cas d'usage prévus** :
- Types géométriques natifs (POINT pour stations)
- Analyses spatiales (distances, intersections, buffers)
- Jointures géographiques (stations vs référentiels territoriaux)
- Support projections multiples (WGS84, Lambert 93)

#### Neo4j (Graph)
**Statut** : 🚧 **Planifié** (Couche Silver)  
**Justification** : Modélisation relations complexes entre entités

**Cas d'usage prévus** :
- Modèle SANDRE/SOSA (stations, observations, propriétés)
- Relations réseau hydrographique (amont/aval)
- Traversées de graphe (propagation pollutions)
- Contraintes d'intégrité référentielle

## Architecture Medallion

### État d'Implémentation

| Couche | Statut | Technologies | Description |
|--------|--------|--------------|-------------|
| **Bronze** | ✅ **Implémenté** | MinIO + Parquet | Données brutes Hub'Eau |
| **Silver** | 🚧 **En développement** | TimescaleDB, PostGIS, Neo4j | Données transformées optimisées |
| **Gold** | 📋 **Roadmap** | Vues matérialisées, APIs | Agrégations métier |

### Bronze Layer ✅ (Implémenté)

```
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                             │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    MinIO S3                              │  │
│  │                                                          │  │
│  │  📦 Bucket : hubeau-bronze                               │  │
│  │                                                          │  │
│  │  Structure :                                             │  │
│  │  └── {api_name}/{endpoint}/year={YYYY}/*.parquet        │  │
│  │                                                          │  │
│  │  • Format : Parquet (compression Snappy)                 │  │
│  │  • Métadonnées : _load_id, _slice_id, timestamps        │  │
│  │  • Partitionnement : Annuel (2020-2025)                  │  │
│  │  • Pas de transformation : données "as-is"               │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Rôle Bronze** :
- ✅ Stockage données brutes Hub'Eau (24 endpoints)
- ✅ Préservation structure originale API
- ✅ Métadonnées d'ingestion complètes
- ✅ Historique immuable (append-only)
- ✅ Source de vérité pour retraitement

### Silver Layer 🚧 (En développement)

```
┌─────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                             │
│                      (En développement)                         │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │TimescaleDB  │  │   PostGIS   │  │    Neo4j    │             │
│  │Time Series  │  │ Geospatial  │  │    Graph    │             │
│  │             │  │             │  │             │             │
│  │• Hypertables│  │• Geometries │  │• SANDRE     │             │
│  │• Compression│  │• Indexes    │  │• Relations  │             │
│  │• Analytics  │  │• Spatial    │  │• SOSA       │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

**Rôle Silver (prévu)** :
- 🚧 Transformation Bronze → Schémas optimisés
- 🚧 Indexation pour requêtes performantes
- 🚧 Typage fort et contraintes d'intégrité
- 🚧 Dédoublonnage et nettoyage
- 🚧 Enrichissement avec référentiels (SANDRE, BDLISA, COG)

### Gold Layer 📋 (Roadmap)

```
┌─────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                              │
│                         (Roadmap)                               │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ Dashboards  │  │    APIs     │  │   Rapports  │             │
│  │  Grafana    │  │  FastAPI    │  │  Jupyter    │             │
│  │  Metabase   │  │  GraphQL    │  │  PDF/Excel  │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

**Rôle Gold (prévu)** :
- 📋 Vues matérialisées pré-calculées
- 📋 KPIs et métriques métier
- 📋 Conformité DCE, dépassements NQE
- 📋 Dashboards temps réel
- 📋 APIs REST/GraphQL pour applications

## Flux de Données Actuel (DLT → Bronze)

### Pipeline d'Ingestion ✅ (Implémenté)

```
1️⃣ EXTRACTION - DLT Generic Pipeline
   ┌─────────────────────────────────────────────────────┐
   │           APIs Hub'Eau (8 sources)                  │
   │                                                     │
   │  • Slicing intelligent (dept, datetime, station)    │
   │  • Rate limiting adaptatif (0.5-2.0 rps)            │
   │  • Pagination automatique (20K records/page)        │
   │  • Fallbacks sur troncature                         │
   │  • Retry avec backoff exponentiel                   │
   │  • Filtrage stations actives                        │
   └─────────────────────────────────────────────────────┘
                          │
                          ▼ HTTP GET
   ┌─────────────────────────────────────────────────────┐
   │              DLT Pipeline Engine                    │
   │                                                     │
   │  • Pagination handler (page/cursor)                 │
   │  • JSONPath extraction ($.data)                     │
   │  • Primary keys validation                          │
   │  • Truncation detection                             │
   │  • Fallback chain execution                         │
   │  • Garbage collection (évite OOM)                   │
   └─────────────────────────────────────────────────────┘
                          │
                          ▼ run_pipeline()
2️⃣ LOAD - Stockage Bronze
   ┌─────────────────────────────────────────────────────┐
   │              MinIO (S3-Compatible)                  │
   │                                                     │
   │  Bucket: hubeau-bronze                              │
   │  Format: Parquet (compression Snappy)               │
   │  Partition: {api}/{endpoint}/year={YYYY}/           │
   │                                                     │
   │  Métadonnées automatiques :                         │
   │  • _load_id (DLT)                                   │
   │  • _slice_id (découpage)                            │
   │  • _dlt_load_timestamp                              │
   │  • _dlt_id (PK unique)                              │
   └─────────────────────────────────────────────────────┘
                          │
                          ▼
3️⃣ MONITORING - Dagster UI
   ┌─────────────────────────────────────────────────────┐
   │               Dagster Orchestration                 │
   │                                                     │
   │  ✅ Lineage automatique (assets graph)              │
   │  ✅ Logs détaillés par slice                        │
   │  ✅ Métriques : records, requêtes, durée            │
   │  ✅ Retry automatique sur échec                     │
   │  ✅ Alertes si erreur persistante                   │
   │  ✅ Schedules annuels (1er janvier)                 │
   └─────────────────────────────────────────────────────┘
```

### Flux Transformation (🚧 En développement)

```
4️⃣ TRANSFORMATION (Roadmap)
   ┌─────────────┬─────────────┬─────────────┐
   │TimescaleDB  │   PostGIS   │    Neo4j    │
   │Time Series  │ Geospatial  │    Graph    │
   │Compression  │   Indexes   │ Relations   │
   └─────────────┴─────────────┴─────────────┘

5️⃣ ANALYTICS (Roadmap)
   ┌─────────────┬─────────────┬─────────────┐
   │ Dashboards  │    APIs     │   Rapports  │
   │  Metriques  │  Services   │  Analytics  │
   └─────────────┴─────────────┴─────────────┘
```

## Stack Technologique

### Implémenté ✅

```
┌─────────────────────────────────────────────────────────────────┐
│                    MONITORING & OBSERVABILITÉ ✅                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                  Portainer CE 2.19.4                    │   │
│  │                                                         │   │
│  │  ✅ Container Management (start/stop/restart)           │   │
│  │  ✅ Resource Monitoring (CPU/RAM/Network)               │   │
│  │  ✅ Log Viewer centralisé                               │   │
│  │  ✅ Volume/Network Management                           │   │
│  │  ✅ Stack deployment                                    │   │
│  │  ✅ Health checks visualization                         │   │
│  │                                                         │   │
│  │  Sécurité :                                            │   │
│  │  • HTTPS uniquement (port 9443)                        │   │
│  │  • Docker socket read-only                             │   │
│  │  • Resource limits (256MB RAM)                          │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION ✅                             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Dagster 1.5+                         │   │
│  │                                                         │   │
│  │  ✅ Assets DLT déclaratifs                              │   │
│  │  ✅ Jobs avec dépendances                               │   │
│  │  ✅ Partitions annuelles (2020-2025)                    │   │
│  │  ✅ In-process executor (optimisation mémoire)          │   │
│  │  ✅ Schedules annuels                                   │   │
│  │  ✅ UI monitoring intégrée                              │   │
│  │  ✅ Lineage automatique                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATA LOADING ✅                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              DLT Custom (hubeau_generic)                │   │
│  │                                                         │   │
│  │  ✅ Pipeline générique configurable (YAML)              │   │
│  │  ✅ 5 modes slicing (global, datetime, dept, etc.)      │   │
│  │  ✅ Pagination (page/cursor) automatique                │   │
│  │  ✅ Fallbacks sur troncature                            │   │
│  │  ✅ Rate limiting adaptatif                             │   │
│  │  ✅ Retry avec backoff exponentiel                      │   │
│  │  ✅ Garbage collection (évite OOM)                      │   │
│  │  ✅ Filtrage stations actives                           │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    STORAGE BRONZE ✅                            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    MinIO S3                             │   │
│  │                                                         │   │
│  │  ✅ Bucket : hubeau-bronze                              │   │
│  │  ✅ Format : Parquet (Snappy)                           │   │
│  │  ✅ Partitionnement : API + Année                       │   │
│  │  ✅ Métadonnées : load_id, slice_id, timestamps         │   │
│  │  ✅ Accès : boto3 + pyarrow                             │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Planifié 🚧

```
┌─────────────────────────────────────────────────────────────────┐
│                   STORAGE SILVER 🚧                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │TimescaleDB  │  │   PostGIS   │  │    Neo4j    │             │
│  │Time Series  │  │ Geospatial  │  │    Graph    │             │
│  │(Roadmap)    │  │(Roadmap)    │  │(Roadmap)    │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     ANALYTICS GOLD 📋                           │
│  ┌─────────────┐  ┌─────────────┐                              │
│  │  Dashboards │  │Analytics APIs│                              │
│  │  (Roadmap)  │  │  (Roadmap)   │                              │
│  └─────────────┘  └─────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
```

### Technologies par Couche

| Couche | Technologie | Version | Statut | Rôle |
|--------|-------------|---------|--------|------|
| **Monitoring** | Portainer CE | 2.19.4 | ✅ Prod | Docker management & monitoring |
| **Orchestration** | Dagster | 1.5+ | ✅ Prod | Workflow, scheduling, monitoring |
| **Ingestion** | DLT Custom | - | ✅ Prod | Extraction Hub'Eau → Parquet |
| **Bronze** | MinIO | Latest | ✅ Prod | Stockage S3 données brutes |
| **Bronze** | Parquet | - | ✅ Prod | Format columnar compressé |
| **Silver** | TimescaleDB | 2.x | 🚧 Dev | Time series optimisées |
| **Silver** | PostGIS | 3.x | 🚧 Dev | Analyses géospatiales |
| **Silver** | Neo4j | 5.x | 🚧 Dev | Relations graphe SANDRE/SOSA |
| **Gold** | Grafana | - | 📋 Roadmap | Dashboards métriques |
| **Gold** | FastAPI | - | 📋 Roadmap | APIs REST analytics |

