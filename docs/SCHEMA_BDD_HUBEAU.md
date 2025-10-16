# Schéma de Données Hub'Eau

> **Architecture Actuelle : Bronze Layer (MinIO) → Silver/Gold Layers (PostgreSQL + PostGIS)**
> **Date** : 2025-10-16
> **Version** : 3.0 - Architecture Simplifiée

## Table des Matières

1. [Architecture Données](#architecture-données)
2. [Bronze Layer - MinIO](#bronze-layer---minio)
3. [Silver/Gold Layers - PostgreSQL](#silvergold-layers---postgresql)
4. [Schéma Relationnel](#schéma-relationnel)
5. [Conventions et Standards](#conventions-et-standards)

---

## Architecture Données

### Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  BRONZE LAYER - Raw Data (Immuable)                        │
│                                                              │
│  MinIO (S3-compatible)                                      │
│  Format: Parquet (colonnaire, compressé)                   │
│                                                              │
│  Structure:                                                  │
│  bronze/                                                     │
│    └─ hubeau/                                               │
│        ├─ hydrometrie/                                      │
│        │   ├─ stations/                                     │
│        │   │   └─ YYYY-MM-DD/*.parquet                     │
│        │   └─ observations/                                 │
│        │       └─ YYYY-MM-DD/*.parquet                     │
│        ├─ piezometrie/                                      │
│        ├─ qualite_rivieres/                                │
│        ├─ qualite_nappes/                                  │
│        ├─ temperature/                                      │
│        ├─ ecoulement/                                       │
│        ├─ hydrobiologie/                                    │
│        └─ prelevements/                                     │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ dbt transform
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER LAYER - Cleaned & Validated                        │
│                                                              │
│  PostgreSQL 16 + PostGIS 3.4                               │
│                                                              │
│  Tables:                                                     │
│  - Référentiels SANDRE (normalisés)                        │
│  - Référentiels BDLISA (géologie)                          │
│  - Entités géographiques (cours d'eau, bassins, etc.)     │
│  - Stations (toutes APIs)                                   │
│  - Chroniques/Observations (données nettoyées)             │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ dbt aggregate
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD LAYER - Analytics Ready                               │
│                                                              │
│  PostgreSQL 16 + PostGIS 3.4                               │
│                                                              │
│  Tables/Views:                                               │
│  - Agrégations temporelles (moyennes mensuelles, etc.)     │
│  - Indicateurs par bassin/département                       │
│  - Métriques qualité eau                                    │
│  - Données géospatiales optimisées                         │
└─────────────────────────────────────────────────────────────┘
```

### Pourquoi cette architecture ?

**Bronze (MinIO + Parquet)**
- ✅ Immuabilité : Données brutes jamais modifiées
- ✅ Compression : 10x moins d'espace que JSON
- ✅ Performance : Format colonnaire pour analytics
- ✅ Standard : Compatible avec tous les outils (Spark, Pandas, DuckDB)
- ✅ Coût : Self-hosted, pas de coûts cloud

**Silver/Gold (PostgreSQL + PostGIS)**
- ✅ SQL standard : Requêtes familières
- ✅ ACID : Transactions, contraintes, intégrité
- ✅ PostGIS : Fonctions géospatiales (ST_Distance, ST_Contains, etc.)
- ✅ Performance : Index B-tree, GIST pour géométries
- ✅ Maturité : Base de données éprouvée depuis 30 ans

---

## Bronze Layer - MinIO

### Structure Parquet

Chaque endpoint d'API Hub'Eau génère des fichiers Parquet dans MinIO.

**Exemple Hydrométrie Stations** :
```
bronze/hubeau/hydrometrie/stations/2025-10-16/run_xyz.parquet
```

**Schéma Parquet** (exemple `hydrometrie_stations`) :
```python
{
    "code_station": "string",
    "code_site": "string",
    "libelle_station": "string",
    "longitude_station": "double",
    "latitude_station": "double",
    "type_station": "string",
    "code_cours_eau": "string",
    "code_commune_station": "string",
    "code_departement": "string",
    "en_service": "boolean",
    "date_ouverture_station": "date",
    "date_maj_station": "timestamp",
    # ... 85 colonnes au total
}
```

### Organisation par API

```
bronze/hubeau/
├─ hydrometrie/
│   ├─ sites/                    # Endpoint /hydrometrie/sites
│   ├─ stations/                 # Endpoint /hydrometrie/stations
│   └─ observations_tr/          # Endpoint /hydrometrie/observations_tr
│
├─ piezometrie/
│   ├─ stations/                 # Endpoint /niveaux_nappes/stations
│   ├─ chroniques/               # Endpoint /niveaux_nappes/chroniques
│   └─ chroniques_tr/            # Endpoint /niveaux_nappes/chroniques_tr
│
├─ qualite_rivieres/
│   ├─ stations/                 # Endpoint /qualite_rivieres/station_pc
│   ├─ operations/               # Endpoint /qualite_rivieres/operation_pc
│   ├─ analyses/                 # Endpoint /qualite_rivieres/analyse_pc
│   └─ conditions/               # Endpoint /qualite_rivieres/condition_environnementale_pc
│
├─ qualite_nappes/
│   ├─ stations/                 # Endpoint /qualite_nappes_superficielles/stations
│   └─ analyses/                 # Endpoint /qualite_nappes_superficielles/analyses
│
├─ temperature/
│   ├─ stations/                 # Endpoint /temperature/station
│   └─ chroniques/               # Endpoint /temperature/chronique
│
├─ ecoulement/
│   ├─ stations/                 # Endpoint /ecoulement/stations
│   ├─ campagnes/                # Endpoint /ecoulement/campagnes
│   └─ observations/             # Endpoint /ecoulement/observations
│
├─ hydrobiologie/
│   ├─ stations/                 # Endpoint /hydrobiologie/stations_hydrobio
│   ├─ indices/                  # Endpoint /hydrobiologie/indices_hydrobio
│   └─ taxons/                   # Endpoint /hydrobiologie/taxons_hydrobio
│
└─ prelevements/
    ├─ ouvrages/                 # Endpoint /prelevements/ouvrages
    ├─ points/                   # Endpoint /prelevements/points_prelevement
    └─ chroniques/               # Endpoint /prelevements/chroniques
```

**Total** : 8 APIs × 3 endpoints = 24 dossiers Bronze

### Lecture Parquet

**Depuis Python (DuckDB)** :
```python
import duckdb

# Query direct sur MinIO
result = duckdb.sql("""
    SELECT code_station, AVG(resultat) as debit_moyen
    FROM read_parquet('s3://bronze/hubeau/hydrometrie/observations/**/*.parquet')
    WHERE date_obs >= '2024-01-01'
    GROUP BY code_station
""").df()
```

**Depuis PostgreSQL (parquet_fdw)** :
```sql
-- Extension (future)
CREATE EXTENSION parquet_fdw;

CREATE FOREIGN TABLE hydrometrie_stations_bronze (
    code_station TEXT,
    libelle_station TEXT,
    -- ... autres colonnes
)
SERVER parquet_server
OPTIONS (filename 's3://bronze/hubeau/hydrometrie/stations/**/*.parquet');
```

---

## Silver/Gold Layers - PostgreSQL

### Configuration PostgreSQL

**Version** : PostgreSQL 16
**Extensions** :
- `postgis` 3.4 - Fonctions géospatiales
- `pg_stat_statements` - Monitoring performance
- `uuid-ossp` - Génération UUID

**Schemas** :
```sql
-- Bronze (optionnel - foreign tables vers Parquet)
CREATE SCHEMA bronze;

-- Silver (données nettoyées)
CREATE SCHEMA silver;

-- Gold (agrégations)
CREATE SCHEMA gold;

-- Référentiels
CREATE SCHEMA ref;
```

### PostGIS - Fonctions Géospatiales

**Index spatiaux** :
```sql
-- Index GIST pour requêtes spatiales
CREATE INDEX idx_stations_geom
ON silver.hydrometrie_stations
USING GIST (geometry);

-- Index sur département (souvent utilisé)
CREATE INDEX idx_stations_dept
ON silver.hydrometrie_stations (code_departement);
```

**Requêtes spatiales courantes** :
```sql
-- Stations dans un rayon de 5km
SELECT code_station, libelle_station,
       ST_Distance(geometry::geography,
                   ST_SetSRID(ST_MakePoint(2.3488, 48.8534), 4326)::geography) AS distance_m
FROM silver.hydrometrie_stations
WHERE ST_DWithin(geometry::geography,
                 ST_SetSRID(ST_MakePoint(2.3488, 48.8534), 4326)::geography,
                 5000)
ORDER BY distance_m;

-- Stations dans un polygone (département)
SELECT s.code_station
FROM silver.hydrometrie_stations s
JOIN ref.departements d ON s.code_departement = d.code_departement
WHERE ST_Contains(d.geometry, s.geometry);
```

---

## Schéma Relationnel

### Notation

- **PK** : Clé Primaire
- **FK** : Clé Étrangère
- **→** : Relation FK vers PK
- **1:N** : Un à plusieurs
- **N:N** : Plusieurs à plusieurs (table de liaison)

### Référentiels SANDRE

Les référentiels SANDRE normalisent TOUTES les APIs Hub'Eau.

```sql
-- Paramètres mesurés (température, pH, débit, etc.)
CREATE TABLE ref.sandre_parametres (
    code_parametre TEXT PRIMARY KEY,
    libelle_parametre TEXT NOT NULL,
    nature TEXT,  -- 'physico-chimie', 'biologie', 'hydrologie'
    uri_parametre TEXT
);

-- Unités de mesure
CREATE TABLE ref.sandre_unites (
    code_unite TEXT PRIMARY KEY,
    symbole_unite TEXT NOT NULL,  -- 'mg/L', '°C', 'm³/s'
    libelle_unite TEXT,
    uri_unite TEXT
);

-- Qualifications (qualité de la donnée)
CREATE TABLE ref.sandre_qualifications (
    code_qualification TEXT PRIMARY KEY,
    libelle_qualification TEXT NOT NULL,
    niveau_confiance INTEGER  -- 1=correcte, 2=incertaine, 3=douteuse, 4=mauvaise
);

-- Supports de prélèvement
CREATE TABLE ref.sandre_supports (
    code_support TEXT PRIMARY KEY,
    libelle_support TEXT NOT NULL,  -- 'Eau', 'Sédiment', 'Biote'
    uri_support TEXT
);

-- Méthodes d'analyse
CREATE TABLE ref.sandre_methodes (
    code_methode TEXT PRIMARY KEY,
    nom_methode TEXT NOT NULL,
    type_methode TEXT,  -- 'analyse', 'prelevement', 'extraction'
    uri_methode TEXT
);

-- Taxons (espèces biologiques)
CREATE TABLE ref.sandre_taxons (
    code_appel_taxon TEXT PRIMARY KEY,
    libelle_appel_taxon TEXT NOT NULL,  -- Nom scientifique
    codes_taxons_parents TEXT[],  -- Hiérarchie taxonomique
    rang_taxonomique TEXT  -- 'Espèce', 'Genre', 'Famille', 'Ordre'
);

-- Statuts
CREATE TABLE ref.sandre_statuts (
    code_statut TEXT PRIMARY KEY,
    mnemo_statut TEXT,
    libelle_statut TEXT
);
```

### Référentiels BDLISA (Géologie)

```sql
-- Formations géologiques / aquifères
CREATE TABLE ref.bdlisa_formations (
    code_bdlisa TEXT PRIMARY KEY,
    nom_formation TEXT NOT NULL,
    urn_bdlisa TEXT,
    nature_lithologique TEXT,  -- 'Calcaire', 'Sable', 'Grès'
    productivite TEXT,  -- 'Très productive', 'Productive', 'Peu productive'
    type_porosite TEXT,  -- 'Primaire', 'Secondaire (fissures, karst)'
    type_entite TEXT  -- 'Aquifère', 'Aquitard'
);
```

### Entités Géographiques

```sql
-- Cours d'eau
CREATE TABLE ref.cours_eau (
    code_cours_eau TEXT PRIMARY KEY,
    libelle_cours_eau TEXT NOT NULL,
    uri_cours_eau TEXT
);

-- Masses d'eau (DCE)
CREATE TABLE ref.masses_eau (
    code_masse_deau TEXT PRIMARY KEY,
    nom_masse_deau TEXT NOT NULL,
    type_masse_deau TEXT,  -- 'Cours d'eau', 'Plan d'eau', 'Souterraine'
    code_bassin TEXT,
    FOREIGN KEY (code_bassin) REFERENCES ref.bassins(code_bassin)
);

-- Bassins hydrographiques
CREATE TABLE ref.bassins (
    code_bassin TEXT PRIMARY KEY,
    libelle_bassin TEXT NOT NULL,
    code_eu_bassin TEXT,
    geometry GEOMETRY(MultiPolygon, 4326)
);

-- Départements
CREATE TABLE ref.departements (
    code_departement TEXT PRIMARY KEY,
    libelle_departement TEXT NOT NULL,
    code_region TEXT,
    geometry GEOMETRY(MultiPolygon, 4326),
    FOREIGN KEY (code_region) REFERENCES ref.regions(code_region)
);

-- Communes
CREATE TABLE ref.communes (
    code_commune_insee TEXT PRIMARY KEY,
    libelle_commune TEXT NOT NULL,
    code_departement TEXT,
    geometry GEOMETRY(MultiPolygon, 4326),
    FOREIGN KEY (code_departement) REFERENCES ref.departements(code_departement)
);

-- Régions
CREATE TABLE ref.regions (
    code_region TEXT PRIMARY KEY,
    libelle_region TEXT NOT NULL,
    geometry GEOMETRY(MultiPolygon, 4326)
);
```

### Hydrométrie (Débits)

```sql
-- Sites hydrométrie
CREATE TABLE silver.hydrometrie_sites (
    code_site TEXT PRIMARY KEY,
    libelle_site TEXT NOT NULL,
    longitude_site DOUBLE PRECISION,
    latitude_site DOUBLE PRECISION,
    geometry GEOMETRY(Point, 4326),
    code_cours_eau TEXT,
    code_departement TEXT,
    surface_bv DOUBLE PRECISION,  -- Surface bassin versant (km²)
    date_maj_site TIMESTAMP,
    FOREIGN KEY (code_cours_eau) REFERENCES ref.cours_eau(code_cours_eau),
    FOREIGN KEY (code_departement) REFERENCES ref.departements(code_departement)
);

-- Stations hydrométrie
CREATE TABLE silver.hydrometrie_stations (
    code_station TEXT PRIMARY KEY,
    code_site TEXT,
    libelle_station TEXT NOT NULL,
    longitude_station DOUBLE PRECISION,
    latitude_station DOUBLE PRECISION,
    geometry GEOMETRY(Point, 4326),
    type_station TEXT,
    code_cours_eau TEXT,
    code_departement TEXT,
    en_service BOOLEAN,
    date_ouverture_station DATE,
    date_fermeture_station DATE,
    date_maj_station TIMESTAMP,
    FOREIGN KEY (code_site) REFERENCES silver.hydrometrie_sites(code_site),
    FOREIGN KEY (code_cours_eau) REFERENCES ref.cours_eau(code_cours_eau),
    FOREIGN KEY (code_departement) REFERENCES ref.departements(code_departement)
);

-- Observations élaborées (débits temps réel)
CREATE TABLE silver.hydrometrie_obs_elab (
    code_station TEXT NOT NULL,
    date_obs_elab TIMESTAMP NOT NULL,
    grandeur_hydro_elab TEXT NOT NULL,  -- 'QmJ', 'H', etc.
    resultat_obs_elab DOUBLE PRECISION,
    code_qualification TEXT,
    code_methode TEXT,
    PRIMARY KEY (code_station, date_obs_elab, grandeur_hydro_elab),
    FOREIGN KEY (code_station) REFERENCES silver.hydrometrie_stations(code_station),
    FOREIGN KEY (code_qualification) REFERENCES ref.sandre_qualifications(code_qualification),
    FOREIGN KEY (code_methode) REFERENCES ref.sandre_methodes(code_methode)
);

-- Index pour requêtes temporelles
CREATE INDEX idx_hydro_obs_date ON silver.hydrometrie_obs_elab (date_obs_elab);
CREATE INDEX idx_hydro_obs_station_date ON silver.hydrometrie_obs_elab (code_station, date_obs_elab);
```

### Piézométrie (Nappes)

```sql
-- Stations piézométriques (BSS)
CREATE TABLE silver.piezometrie_stations (
    code_bss TEXT PRIMARY KEY,
    bss_id TEXT UNIQUE,
    urn_bss TEXT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    geometry GEOMETRY(Point, 4326),
    code_commune_insee TEXT,
    code_departement TEXT,
    altitude_station DOUBLE PRECISION,
    profondeur_investigation DOUBLE PRECISION,  -- Profondeur max (m)
    libelle_pe TEXT,  -- Point d'eau
    date_debut_mesure DATE,
    date_fin_mesure DATE,
    date_maj TIMESTAMP,
    FOREIGN KEY (code_commune_insee) REFERENCES ref.communes(code_commune_insee),
    FOREIGN KEY (code_departement) REFERENCES ref.departements(code_departement)
);

-- Chroniques piézométriques
CREATE TABLE silver.piezometrie_chroniques (
    code_bss TEXT NOT NULL,
    timestamp_mesure TIMESTAMP NOT NULL,
    niveau_eau_ngf DOUBLE PRECISION,  -- Niveau NGF (m)
    profondeur_nappe DOUBLE PRECISION,  -- Profondeur depuis surface (m)
    code_qualification TEXT,
    mode_obtention TEXT,  -- 'Temps réel', 'Manuel'
    PRIMARY KEY (code_bss, timestamp_mesure),
    FOREIGN KEY (code_bss) REFERENCES silver.piezometrie_stations(code_bss),
    FOREIGN KEY (code_qualification) REFERENCES ref.sandre_qualifications(code_qualification)
);

-- Liaison stations ↔ formations géologiques (N:N)
CREATE TABLE silver.piezometrie_stations_bdlisa (
    code_bss TEXT,
    code_bdlisa TEXT,
    PRIMARY KEY (code_bss, code_bdlisa),
    FOREIGN KEY (code_bss) REFERENCES silver.piezometrie_stations(code_bss),
    FOREIGN KEY (code_bdlisa) REFERENCES ref.bdlisa_formations(code_bdlisa)
);

-- Index temporels
CREATE INDEX idx_piezo_chron_date ON silver.piezometrie_chroniques (timestamp_mesure);
```

### Qualité Cours d'Eau

```sql
-- Stations qualité cours d'eau
CREATE TABLE silver.qualite_rivieres_stations (
    code_station TEXT PRIMARY KEY,
    libelle_station TEXT NOT NULL,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    geometry GEOMETRY(Point, 4326),
    code_cours_eau TEXT,
    code_masse_deau TEXT,
    code_departement TEXT,
    date_maj_information TIMESTAMP,
    FOREIGN KEY (code_cours_eau) REFERENCES ref.cours_eau(code_cours_eau),
    FOREIGN KEY (code_masse_deau) REFERENCES ref.masses_eau(code_masse_deau),
    FOREIGN KEY (code_departement) REFERENCES ref.departements(code_departement)
);

-- Opérations (campagnes prélèvement)
CREATE TABLE silver.qualite_rivieres_operations (
    code_station TEXT NOT NULL,
    date_prelevement DATE NOT NULL,
    code_operation TEXT NOT NULL,
    heure_prelevement TIME,
    code_support TEXT,
    code_methode TEXT,
    PRIMARY KEY (code_station, date_prelevement, code_operation),
    FOREIGN KEY (code_station) REFERENCES silver.qualite_rivieres_stations(code_station),
    FOREIGN KEY (code_support) REFERENCES ref.sandre_supports(code_support),
    FOREIGN KEY (code_methode) REFERENCES ref.sandre_methodes(code_methode)
);

-- Analyses physico-chimiques
CREATE TABLE silver.qualite_rivieres_analyses (
    code_analyse TEXT PRIMARY KEY,
    code_station TEXT NOT NULL,
    date_prelevement DATE NOT NULL,
    code_parametre TEXT NOT NULL,
    resultat DOUBLE PRECISION,
    code_unite TEXT,
    limite_detection DOUBLE PRECISION,
    limite_quantification DOUBLE PRECISION,
    code_qualification TEXT,
    code_support TEXT,
    code_methode_analyse TEXT,
    FOREIGN KEY (code_station, date_prelevement) REFERENCES silver.qualite_rivieres_operations(code_station, date_prelevement),
    FOREIGN KEY (code_parametre) REFERENCES ref.sandre_parametres(code_parametre),
    FOREIGN KEY (code_unite) REFERENCES ref.sandre_unites(code_unite),
    FOREIGN KEY (code_qualification) REFERENCES ref.sandre_qualifications(code_qualification),
    FOREIGN KEY (code_support) REFERENCES ref.sandre_supports(code_support)
);

-- Index pour requêtes fréquentes
CREATE INDEX idx_qual_riv_analyses_param ON silver.qualite_rivieres_analyses (code_parametre);
CREATE INDEX idx_qual_riv_analyses_date ON silver.qualite_rivieres_analyses (date_prelevement);
```

### Gold Layer - Agrégations

```sql
-- Débits moyens mensuels par station
CREATE MATERIALIZED VIEW gold.hydrometrie_debits_mensuels AS
SELECT
    code_station,
    DATE_TRUNC('month', date_obs_elab) AS mois,
    grandeur_hydro_elab,
    AVG(resultat_obs_elab) AS debit_moyen,
    MIN(resultat_obs_elab) AS debit_min,
    MAX(resultat_obs_elab) AS debit_max,
    COUNT(*) AS nb_mesures
FROM silver.hydrometrie_obs_elab
WHERE code_qualification = '1'  -- Données correctes uniquement
GROUP BY code_station, DATE_TRUNC('month', date_obs_elab), grandeur_hydro_elab;

-- Index pour requêtes
CREATE INDEX idx_debits_mensuels_station_mois ON gold.hydrometrie_debits_mensuels (code_station, mois);

-- Refresh automatique (cron ou trigger)
-- Ou manuel: REFRESH MATERIALIZED VIEW gold.hydrometrie_debits_mensuels;
```

---

## Conventions et Standards

### Nommage

**Tables** :
- `{layer}.{api}_{entity}` (ex: `silver.hydrometrie_stations`)
- Pluriel pour collections
- Snake_case

**Colonnes** :
- Snake_case
- Préfixe `code_` pour clés étrangères
- Préfixe `libelle_` pour labels
- Suffixe `_date` pour dates
- Suffixe `_timestamp` pour timestamps

**Index** :
- `idx_{table}_{colonne(s)}` (ex: `idx_stations_geom`)

### Types de Données

| Donnée | Type PostgreSQL | Notes |
|--------|----------------|-------|
| Codes (PK/FK) | `TEXT` | Plus flexible que VARCHAR |
| Libellés | `TEXT` | |
| Nombres décimaux | `DOUBLE PRECISION` | Mesures scientifiques |
| Dates | `DATE` | Format YYYY-MM-DD |
| Timestamps | `TIMESTAMP` | Avec timezone si nécessaire |
| Booléens | `BOOLEAN` | `TRUE`/`FALSE` |
| Géométries | `GEOMETRY(type, 4326)` | PostGIS, SRID 4326 (WGS84) |
| Arrays | `TEXT[]` | Pour listes (ex: codes_taxons_parents) |

### Contraintes

```sql
-- Clés primaires
ALTER TABLE silver.hydrometrie_stations
ADD CONSTRAINT pk_hydro_stations PRIMARY KEY (code_station);

-- Clés étrangères
ALTER TABLE silver.hydrometrie_obs_elab
ADD CONSTRAINT fk_hydro_obs_station
FOREIGN KEY (code_station) REFERENCES silver.hydrometrie_stations(code_station);

-- Check constraints
ALTER TABLE silver.hydrometrie_stations
ADD CONSTRAINT check_longitude CHECK (longitude_station BETWEEN -180 AND 180);

ALTER TABLE silver.hydrometrie_stations
ADD CONSTRAINT check_latitude CHECK (latitude_station BETWEEN -90 AND 90);

-- NOT NULL
ALTER TABLE silver.hydrometrie_stations
ALTER COLUMN libelle_station SET NOT NULL;
```

### Performance

**Index standards** :
```sql
-- Géométries (GIST)
CREATE INDEX idx_{table}_geom ON {schema}.{table} USING GIST (geometry);

-- Dates/Timestamps (B-tree)
CREATE INDEX idx_{table}_date ON {schema}.{table} (date_column);

-- Clés étrangères (B-tree)
CREATE INDEX idx_{table}_fk ON {schema}.{table} (fk_column);

-- Recherche texte (GIN)
CREATE INDEX idx_{table}_search ON {schema}.{table} USING GIN (to_tsvector('french', libelle_column));
```

**Partitionnement temporel** (si > 100M lignes) :
```sql
-- Exemple pour chroniques volumineuses
CREATE TABLE silver.hydrometrie_obs_elab (
    -- colonnes...
) PARTITION BY RANGE (date_obs_elab);

-- Créer partitions par année
CREATE TABLE silver.hydrometrie_obs_elab_2024
    PARTITION OF silver.hydrometrie_obs_elab
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE silver.hydrometrie_obs_elab_2025
    PARTITION OF silver.hydrometrie_obs_elab
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
```

### Maintenance

```sql
-- Vacuum régulier
VACUUM ANALYZE silver.hydrometrie_obs_elab;

-- Reindex si nécessaire
REINDEX INDEX CONCURRENTLY idx_hydro_obs_station_date;

-- Stats pour optimizer
ANALYZE silver.hydrometrie_stations;
```

---

## Ressources

- **PostgreSQL 16** : https://www.postgresql.org/docs/16/
- **PostGIS 3.4** : https://postgis.net/docs/
- **Apache Parquet** : https://parquet.apache.org/docs/
- **MinIO** : https://min.io/docs/minio/linux/index.html
- **DuckDB** : https://duckdb.org/docs/

---

**Architecture Actuelle** : Bronze (MinIO Parquet) → Silver/Gold (PostgreSQL + PostGIS)
**Simple, Éprouvée, Performante** 🌊
