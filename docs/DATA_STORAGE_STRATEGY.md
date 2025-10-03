# Stratégies de Stockage

## Architecture Medallion

### Bronze Layer (MinIO)
- **Rôle** : Stockage des données brutes Hub'Eau
- **Format** : JSONL (JSON Lines) compressé
- **Organisation** : Par dataset et partition temporelle

### Silver Layer (Bases Spécialisées)
- **TimescaleDB** : Données temporelles (hydrométrie, piézométrie, température)
- **PostGIS** : Données géospatiales (stations, localisations)
- **Neo4j** : Relations entre entités (stations, paramètres, réseaux)

### Gold Layer (Analytics)
- **Dashboards** : Visualisations et métriques
- **Rapports** : Analyses agrégées
- **APIs** : Services de données pour applications

## Configuration MinIO

### Buckets
- `bronze` : Données brutes Hub'Eau
- `silver` : Données transformées
- `gold` : Données analytiques

### Accès
- **Endpoint** : `http://minio:9000` (interne Docker)
- **Interface** : `http://localhost:9001` (externe)
- **Authentification** : Access Key / Secret Key

## Configuration Bases de Données

### TimescaleDB
- **Host** : `timescaledb:5432`
- **Database** : `water_timeseries`
- **Extensions** : `timescaledb`, `postgis`

### PostGIS
- **Host** : `postgis:5432`
- **Database** : `water_geo`
- **Extensions** : `postgis`, `postgis_topology`

### Neo4j
- **Host** : `neo4j:7474`
- **Database** : `water_graph`
- **Protocol** : `bolt://neo4j:7687`