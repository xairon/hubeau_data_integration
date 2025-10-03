# Architecture Technique

## Vue d'Ensemble

```mermaid
graph TB
    A[APIs Hubeau] --> B[Dagster]
    B --> C[MinIO]
    C --> D[TimescaleDB]
    C --> E[PostGIS]
    C --> F[Neo4j]
    D --> G[Analytics]
    E --> G
    F --> G
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
    A[Bronze MinIO] --> B[Silver TimescaleDB]
    A --> C[Silver PostGIS]
    A --> D[Silver Neo4j]
    B --> E[Gold Analytics]
    C --> E
    D --> E
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
    participant API as APIs
    participant DLT as DLT Pipeline
    participant MINIO as MinIO
    participant DB as Databases
    participant GOLD as Analytics
    
    Note over API,GOLD: Ingestion Automatisee
    
    API->>DLT: Requete avec slicing
    DLT->>DLT: Pagination + Fallbacks
    DLT->>MINIO: Stockage JSON brut
    MINIO->>DB: Transformation
    
    Note over DB: TimescaleDB PostGIS Neo4j
    
    DB->>GOLD: Agregations
    GOLD->>GOLD: Dashboards APIs
    
    Note over API,GOLD: Monitoring Dagster
    DLT->>DLT: Logs metriques
    DLT->>DLT: Retry automatique
```

## Stack Technologique

```mermaid
graph TB
    subgraph Orchestration
        DAGSTER[Dagster Pipelines]
    end
    
    subgraph DataLoading
        DLT[DLT Data Load Tool]
    end
    
    subgraph Storage
        MINIO[MinIO S3-Compatible]
        TS[TimescaleDB Time Series]
        PG[PostGIS Geospatial]
        NEO[Neo4j Graph]
    end
    
    subgraph Services
        UI[Dagster UI]
        API[Analytics APIs]
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

