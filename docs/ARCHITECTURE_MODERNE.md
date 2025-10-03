# Architecture Technique

## Vue d'Ensemble

```mermaid
graph TB
    subgraph "Sources de Données"
        H1[API Hydrométrie]
        H2[API Piézométrie]
        H3[API Qualité]
        H4[API Température]
        H5[API Écoulement]
        H6[API Hydrobiologie]
        H7[API Prélèvements]
    end
    
    subgraph "Orchestration"
        DAG[Dagster]
    end
    
    subgraph "Data Lake"
        MINIO[MinIO<br/>Bronze Layer]
    end
    
    subgraph "Bases Spécialisées"
        TS[TimescaleDB<br/>Time Series]
        PG[PostGIS<br/>Géospatial]
        NEO[Neo4j<br/>Graph]
    end
    
    subgraph "Analytics"
        GOLD[Gold Layer<br/>Métriques & Dashboards]
    end
    
    H1 --> DAG
    H2 --> DAG
    H3 --> DAG
    H4 --> DAG
    H5 --> DAG
    H6 --> DAG
    H7 --> DAG
    
    DAG --> MINIO
    MINIO --> TS
    MINIO --> PG
    MINIO --> NEO
    
    TS --> GOLD
    PG --> GOLD
    NEO --> GOLD
```

## Choix Architecturaux

### Orchestration : Dagster
**Justification :** Pipeline complexe avec dépendances, retry automatique, monitoring intégré
- Assets déclaratifs avec lineage automatique
- Schedules et sensors pour automatisation
- UI intégrée pour monitoring et debugging
- Support natif des partitions temporelles

### Data Lake : MinIO
**Justification :** Stockage S3-compatible, coût réduit, intégration Docker native
- Compatible AWS S3 (migration future possible)
- Buckets organisés par couche (bronze/silver/gold)
- Versioning et lifecycle policies
- Accès programmatique via boto3

### Bases de Données Spécialisées

#### TimescaleDB (Time Series)
**Justification :** Optimisé pour données temporelles, compression automatique, requêtes analytiques
- Hypertables pour partitionnement temporel automatique
- Compression automatique sur données historiques
- Fonctions analytiques (LAG, LEAD, window functions)
- Indexes B-tree et BRIN optimisés

#### PostGIS (Geospatial)
**Justification :** Standard industriel, intégration PostgreSQL, performances spatiales
- Types géométriques natifs (POINT, LINESTRING, POLYGON)
- Indexes spatiaux (GIST, SP-GIST)
- Fonctions spatiales (ST_Distance, ST_Contains, ST_Intersects)
- Support projections cartographiques multiples

#### Neo4j (Graph)
**Justification :** Modélisation relationnelle complexe, requêtes graph efficaces
- Modèle Sandre/SOSA avec relations sémantiques
- Requêtes Cypher pour traversées de graphe
- Indexes sur propriétés et relations
- Contraintes d'unicité et intégrité

## Architecture Medallion

```mermaid
graph LR
    subgraph "Bronze Layer"
        BRONZE[MinIO<br/>Données Brutes<br/>JSON + Métadonnées]
    end
    
    subgraph "Silver Layer"
        SILVER1[TimescaleDB<br/>Time Series<br/>Compression]
        SILVER2[PostGIS<br/>Géométries<br/>Indexes Spatiaux]
        SILVER3[Neo4j<br/>Relations<br/>Sandre/SOSA]
    end
    
    subgraph "Gold Layer"
        GOLD1[Dashboards<br/>Métriques]
        GOLD2[APIs<br/>Services]
        GOLD3[Rapports<br/>Analytics]
    end
    
    BRONZE --> SILVER1
    BRONZE --> SILVER2
    BRONZE --> SILVER3
    
    SILVER1 --> GOLD1
    SILVER2 --> GOLD2
    SILVER3 --> GOLD3
    
    style BRONZE fill:#cd7f32
    style SILVER1 fill:#c0c0c0
    style SILVER2 fill:#c0c0c0
    style SILVER3 fill:#c0c0c0
    style GOLD1 fill:#ffd700
    style GOLD2 fill:#ffd700
    style GOLD3 fill:#ffd700
```

### Bronze Layer (MinIO)
**Rôle :** Stockage des données brutes Hub'Eau
- Format JSON préservant structure originale
- Partitioning par API et date
- Métadonnées d'ingestion (timestamps, counts, errors)
- Pas de transformation, données "as-is"

### Silver Layer (Specialized DBs)
**Rôle :** Données transformées et optimisées par cas d'usage
- **TimescaleDB :** Time series avec compression
- **PostGIS :** Géométries et requêtes spatiales
- **Neo4j :** Relations sémantiques Sandre/SOSA
- Schémas optimisés pour requêtes analytiques

### Gold Layer (Analytics)
**Rôle :** Agrégations et métriques métier
- Vues matérialisées pour performances
- Indicateurs de qualité des données
- Dashboards et rapports pré-calculés
- APIs pour applications consommatrices

## Flux de Données avec DLT

```mermaid
sequenceDiagram
    participant API as APIs Hub'Eau
    participant DLT as DLT Pipeline
    participant MINIO as MinIO Bronze
    participant DB as Bases Spécialisées
    participant GOLD as Gold Analytics
    
    Note over API,GOLD: Ingestion Automatisée
    
    API->>DLT: Requête avec slicing intelligent
    DLT->>DLT: Pagination + Fallbacks
    DLT->>MINIO: Stockage JSON brut
    MINIO->>DB: Transformation par cas d'usage
    
    Note over DB: TimescaleDB: Time Series<br/>PostGIS: Géométries<br/>Neo4j: Relations
    
    DB->>GOLD: Agrégations et métriques
    GOLD->>GOLD: Dashboards et APIs
    
    Note over API,GOLD: Monitoring Dagster
    DLT->>DLT: Logs et métriques
    DLT->>DLT: Retry automatique
```

## Stack Technologique

```mermaid
graph TB
    subgraph "Orchestration"
        DAGSTER[Dagster<br/>Pipelines & Monitoring]
    end
    
    subgraph "Data Loading"
        DLT[DLT<br/>Data Load Tool<br/>Slicing & Fallbacks]
    end
    
    subgraph "Storage"
        MINIO[MinIO<br/>S3-Compatible<br/>Bronze Layer]
        TS[TimescaleDB<br/>Time Series<br/>Compression]
        PG[PostGIS<br/>Geospatial<br/>Indexes]
        NEO[Neo4j<br/>Graph<br/>Relations]
    end
    
    subgraph "Services"
        UI[Dagster UI<br/>Monitoring]
        API[Analytics APIs<br/>Services]
    end
    
    DAGSTER --> DLT
    DLT --> MINIO
    MINIO --> TS
    MINIO --> PG
    MINIO --> NEO
    
    DAGSTER --> UI
    TS --> API
    PG --> API
    NEO --> API
```

