# Stratégies de Stockage

## Architecture Medallion

### Bronze Layer (MinIO)
**Rôle :** Stockage des données brutes Hub'Eau
**Format :** JSON préservant structure originale
**Organisation :** `{api_name}/{date_partition}/{endpoint_name}_data.json`

#### Structure MinIO
```
bronze/
├── hydrometry/
│   ├── 2024-09-30/
│   │   ├── referentiel_stations_data.json
│   │   ├── observations_tr_data.json
│   │   └── obs_elab_data.json
│   └── ingestion_metadata.json
├── piezometry/
│   └── 2024-09-30/
└── ...
```

#### Métadonnées d'Ingestion
```json
{
  "api_name": "hydrometry",
  "partition_date": "2024-09-30",
  "execution_date": "2024-09-30T10:30:00Z",
  "total_records": 150000,
  "results_by_endpoint": {
    "referentiel_stations": {"records_count": 15000},
    "observations_tr": {"records_count": 100000},
    "obs_elab": {"records_count": 35000}
  },
  "metrics": {
    "departements_traites": 101,
    "chunks_total": 245,
    "erreurs_http_500": 0
  }
}
```

### Silver Layer (Specialized DBs)

#### TimescaleDB (Time Series)
**Rôle :** Données temporelles optimisées pour requêtes analytiques
**Compression :** 90% réduction sur données historiques
**Partitioning :** Hypertables par API et année

##### Schémas Principaux
```sql
-- Hydrométrie
CREATE TABLE hydrometry_observations (
    code_entite TEXT,
    date_obs TIMESTAMPTZ,
    resultat DOUBLE PRECISION,
    code_qualification TEXT,
    PRIMARY KEY (code_entite, date_obs)
);

-- Piézométrie
CREATE TABLE piezometry_chroniques (
    code_bss TEXT,
    date_mesure TIMESTAMPTZ,
    niveau_nappe_eau DOUBLE PRECISION,
    profondeur_nappe DOUBLE PRECISION,
    PRIMARY KEY (code_bss, date_mesure)
);
```

##### Hypertables
```sql
-- Conversion en hypertables pour compression
SELECT create_hypertable('hydrometry_observations', 'date_obs');
SELECT create_hypertable('piezometry_chroniques', 'date_mesure');

-- Compression automatique après 7 jours
SELECT add_compression_policy('hydrometry_observations', INTERVAL '7 days');
```

#### PostGIS (Geospatial)
**Rôle :** Géométries et requêtes spatiales
**Indexes :** GIST pour performances spatiales
**Projections :** WGS84 (EPSG:4326) et Lambert93 (EPSG:2154)

##### Schémas Principaux
```sql
-- Stations avec géométries
CREATE TABLE stations_geo (
    code_station TEXT PRIMARY KEY,
    libelle_station TEXT,
    code_departement TEXT,
    code_commune TEXT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    geom GEOMETRY(POINT, 4326)
);

-- Index spatial
CREATE INDEX idx_stations_geom ON stations_geo USING GIST (geom);
```

##### Fonctions Spatiales
```sql
-- Requêtes spatiales optimisées
SELECT * FROM stations_geo 
WHERE ST_DWithin(geom, ST_Point(2.3522, 48.8566), 0.01);

-- Agrégations par département
SELECT code_departement, COUNT(*) 
FROM stations_geo 
GROUP BY code_departement;
```

#### Neo4j (Graph)
**Rôle :** Relations sémantiques Sandre/SOSA
**Modèle :** Entités et relations complexes
**Indexes :** Sur propriétés et relations

##### Modèle de Données
```cypher
// Stations
CREATE CONSTRAINT station_code IF NOT EXISTS 
FOR (s:Station) REQUIRE s.code_station IS UNIQUE;

// Relations Sandre/SOSA
CREATE (s:Station {code_station: "12345678"})
CREATE (d:Departement {code_departement: "75"})
CREATE (s)-[:LOCATED_IN]->(d);
```

##### Requêtes Graph
```cypher
// Traversée de graphe
MATCH (s:Station)-[:LOCATED_IN]->(d:Departement)
WHERE d.code_departement = "75"
RETURN s.code_station, s.libelle_station;
```

### Gold Layer (Analytics)
**Rôle :** Agrégations et métriques métier
**Format :** Vues matérialisées et APIs
**Refresh :** Automatique via Dagster

#### Vues Matérialisées
```sql
-- Indicateurs qualité données
CREATE MATERIALIZED VIEW data_quality_metrics AS
SELECT 
    api_name,
    partition_date,
    total_records,
    records_with_null_values,
    records_with_errors,
    (records_with_errors::float / total_records) * 100 as error_rate
FROM ingestion_metadata
WHERE partition_date >= CURRENT_DATE - INTERVAL '30 days';
```

#### APIs Consommatrices
```python
# API REST pour applications
@app.get("/api/v1/stations/{departement}")
async def get_stations_by_departement(departement: str):
    query = """
    SELECT code_station, libelle_station, longitude, latitude
    FROM stations_geo 
    WHERE code_departement = %s
    """
    return await db.fetch_all(query, (departement,))
```

## Stratégies de Partitioning

### Temporel
- **Daily :** Hydrométrie, piézométrie, température
- **Annual :** Qualité eaux, hydrobiologie, ONDE, prélèvements
- **Hybrid :** Hydrobiologie (campagnes saisonnières)

### Spatial
- **Départements :** Chunking par groupes de départements
- **Bbox :** Requêtes par bounding box pour APIs géospatiales
- **Communes :** Filtrage par code commune si supporté

### Par API
- **Volume élevé :** Chunking systématique (hydrométrie, piézométrie)
- **Volume modéré :** Chunking adaptatif (qualité, température)
- **Volume faible :** Requêtes directes (ONDE, hydrobiologie)

## Optimisations de Performance

### Compression
- **TimescaleDB :** Compression automatique après 7 jours
- **MinIO :** Compression gzip sur fichiers JSON
- **Neo4j :** Compression native des données

### Indexes
- **TimescaleDB :** B-tree sur clés temporelles, BRIN sur valeurs
- **PostGIS :** GIST sur géométries, B-tree sur codes
- **Neo4j :** Indexes sur propriétés fréquemment requêtées

### Cache
- **Redis :** Cache des requêtes fréquentes
- **PostgreSQL :** Buffer pool optimisé
- **Neo4j :** Cache des traversées de graphe

## Backup et Récupération

### Stratégies
- **MinIO :** Backup quotidien vers S3 externe
- **TimescaleDB :** pg_dump avec compression
- **PostGIS :** pg_dump avec schémas spatiaux
- **Neo4j :** Neo4j backup avec compression

### Rétention
- **Bronze :** 2 ans (données brutes)
- **Silver :** 5 ans (données transformées)
- **Gold :** 10 ans (agrégations métier)

### Monitoring
- **Espace disque :** Alertes sur seuils (80%, 90%, 95%)
- **Performance :** Monitoring des requêtes lentes
- **Intégrité :** Vérification automatique des checksums