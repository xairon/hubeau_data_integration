# Schéma Base de Données Hub'Eau

> **Architecture** : Hub'Eau APIs → DLT → PostgreSQL
> **Version** : 4.0 - Ingestion directe PostgreSQL
> **Date** : 2025-10-24

## Table des Matières

1. [Architecture](#architecture)
2. [Configuration PostgreSQL](#configuration-postgresql)
3. [Organisation des Tables](#organisation-des-tables)
4. [Schéma des Tables](#schéma-des-tables)
5. [Conventions et Standards](#conventions-et-standards)

---

## Architecture

### Pipeline Simple

```
Hub'Eau APIs (CSV) → DLT → PostgreSQL
                              └─ Schema: hubeau
                                 └─ 22 tables de données
                                 └─ 3 tables DLT (métadonnées)
```

**Caractéristiques** :
- ✅ **Ingestion directe** : CSV depuis Hub'Eau → PostgreSQL sans transformation
- ✅ **Déduplication automatique** : MERGE/UPSERT sur clés primaires
- ✅ **3 modes d'ingestion** : FULL (historique complet), YEAR (année spécifique), INCREMENTAL (derniers N jours)
- ✅ **Schema unique** : Toutes les données dans `hubeau`
- ✅ **PostGIS activé** : Support des géométries spatiales

### Pourquoi cette architecture ?

**Simplicité** :
- Un seul schéma PostgreSQL, pas de layers multiples
- Pas de stockage intermédiaire (MinIO, Parquet, etc.)
- Ingestion directe = zéro transformation

**Performance** :
- DLT gère l'optimisation (batch loading, COPY PostgreSQL natif)
- Déduplication automatique via clés primaires
- Index créés automatiquement par DLT

**Maintenance** :
- DLT crée les tables automatiquement au premier run
- Pandas infère les types depuis les CSV
- Zéro définition manuelle de schéma

---

## Configuration PostgreSQL

### Version et Extensions

**PostgreSQL** : 16
**Extensions installées** :
- `postgis` - Fonctions géospatiales (Points, coordonnées)

### Initialisation

Le script `docker/init-scripts/postgres/01_init_minimal.sql` crée :

```sql
-- Schéma unique pour toutes les données Hub'Eau
CREATE SCHEMA IF NOT EXISTS hubeau;

-- Extension PostGIS pour géométries
CREATE EXTENSION IF NOT EXISTS postgis;

-- Permissions complètes
GRANT ALL PRIVILEGES ON SCHEMA hubeau TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA hubeau TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA hubeau TO postgres;

-- Permissions futures (tables créées par DLT)
ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau
GRANT ALL PRIVILEGES ON TABLES TO postgres;
```

**Important** : DLT crée automatiquement les tables au premier run. Pas de définition manuelle de schéma.

---

## Organisation des Tables

### 22 Tables de Données

**11 Stations/Référentiels** (sans filtres date, mode FULL uniquement) :
1. `piezometry_stations` - Stations piézométriques (BSS)
2. `quality_groundwater_stations` - Stations qualité nappes
3. `quality_rivers_stations` - Stations qualité cours d'eau
4. `temperature_stations` - Stations température
5. `hydrometry_sites` - Sites hydrométrie
6. `hydrometry_stations` - Stations hydrométrie
7. `hydrobio_stations` - Stations hydrobiologie
8. `ecoulement_stations` - Stations écoulement
9. `ecoulement_campagnes` - Campagnes écoulement
10. `prelevements_points` - Points de prélèvement
11. `prelevements_ouvrages` - Ouvrages de prélèvement

**11 Chroniques/Observations** (avec filtres date, 3 modes : FULL/YEAR/INCREMENTAL) :
1. `piezometry_chroniques` - Mesures piézométriques
2. `quality_groundwater_analyses` - Analyses qualité nappes
3. `quality_rivers_analyses` - Analyses qualité cours d'eau
4. `quality_rivers_conditions` - Conditions environnementales prélèvement
5. `quality_rivers_operations` - Opérations de prélèvement
6. `temperature_chroniques` - Mesures température
7. `hydrometry_obs_elab` - Observations hydrométrie élaborées
8. `hydrobio_indices` - Indices hydrobiologiques
9. `hydrobio_taxons` - Taxons hydrobiologiques
10. `ecoulement_observations` - Observations écoulement
11. `prelevements_chroniques` - Chroniques prélèvements

### 3 Tables DLT (métadonnées)

DLT crée automatiquement 3 tables techniques dans le schéma `hubeau` :

- `_dlt_loads` - Historique des chargements (timestamps, statuts, erreurs)
- `_dlt_version` - Version du schéma DLT
- `_dlt_pipeline_state` - État du pipeline (cursors, états incrémentaux)

---

## Schéma des Tables

### Exemple 1 : piezometry_chroniques

Table volumineuse : mesures piézométriques horaires depuis 1960.

**Clé primaire** : `[code_bss, timestamp_mesure]` (station + timestamp)
**Mode ingestion** : FULL / YEAR / INCREMENTAL
**Volume typique** : ~50M+ records

```sql
CREATE TABLE hubeau.piezometry_chroniques (
    -- Identifiants (3 colonnes)
    code_bss TEXT NOT NULL,               -- Code BSS (Banque du Sous-Sol)
    bss_id TEXT,                          -- Identifiant interne BSS
    urn_bss TEXT,                         -- URN unique BSS

    -- Temporel (3 colonnes)
    date_mesure DATE,                     -- Date mesure (YYYY-MM-DD)
    timestamp_mesure TIMESTAMP NOT NULL,  -- Timestamp complet (clé primaire)
    date_maj TIMESTAMP,                   -- Date dernière mise à jour

    -- Localisation (2 colonnes)
    longitude DOUBLE PRECISION,           -- Longitude WGS84
    latitude DOUBLE PRECISION,            -- Latitude WGS84

    -- Niveaux piézométriques (4 colonnes)
    altitude_station DOUBLE PRECISION,    -- Altitude station (m NGF)
    altitude_repere DOUBLE PRECISION,     -- Altitude repère mesure (m NGF)
    niveau_eau_ngf DOUBLE PRECISION,      -- Niveau nappe NGF (m)
    profondeur_nappe DOUBLE PRECISION,    -- Profondeur nappe/surface (m)

    PRIMARY KEY (code_bss, timestamp_mesure)
);
```

**Index automatiques DLT** :
- Primary key : `(code_bss, timestamp_mesure)`
- Performance : Index B-tree sur clés primaires

### Exemple 2 : quality_rivers_analyses

Table volumineuse : analyses physico-chimiques des cours d'eau.

**Clé primaire** : `[code_analyse]` (identifiant unique analyse)
**Mode ingestion** : FULL / YEAR / INCREMENTAL
**Volume typique** : ~20M+ records

```sql
CREATE TABLE hubeau.quality_rivers_analyses (
    -- Identifiants
    code_analyse TEXT PRIMARY KEY,        -- Identifiant unique analyse
    code_station TEXT,                    -- Code station prélèvement
    code_operation TEXT,                  -- Code opération prélèvement

    -- Temporel
    date_prelevement DATE,                -- Date prélèvement
    heure_prelevement TIME,               -- Heure prélèvement

    -- Paramètre mesuré
    code_parametre TEXT,                  -- Code SANDRE paramètre (ex: "1340" = Nitrates)
    libelle_parametre TEXT,               -- Libellé paramètre
    code_unite TEXT,                      -- Code unité (ex: "mg/L")
    symbole_unite TEXT,                   -- Symbole unité

    -- Résultat
    resultat DOUBLE PRECISION,            -- Valeur mesurée
    limite_detection DOUBLE PRECISION,    -- Limite détection
    limite_quantification DOUBLE PRECISION, -- Limite quantification
    code_qualification TEXT,              -- Qualité donnée (1=correcte, 2=incertaine, etc.)

    -- Méthodes
    code_support TEXT,                    -- Support prélèvement (Eau, Sédiment, etc.)
    code_methode_analyse TEXT,            -- Méthode analytique utilisée

    -- Métadonnées
    date_maj_information TIMESTAMP        -- Date mise à jour
);
```

**Index automatiques DLT** :
- Primary key : `code_analyse`

### Exemple 3 : piezometry_stations

Table référentiel : liste des stations piézométriques.

**Clé primaire** : `[code_bss]`
**Mode ingestion** : FULL uniquement (pas de filtre date)
**Volume typique** : ~23k records

```sql
CREATE TABLE hubeau.piezometry_stations (
    -- Identifiants
    code_bss TEXT PRIMARY KEY,            -- Code BSS unique
    bss_id TEXT UNIQUE,                   -- Identifiant interne
    urn_bss TEXT,                         -- URN unique

    -- Localisation
    longitude DOUBLE PRECISION,           -- Longitude WGS84
    latitude DOUBLE PRECISION,            -- Latitude WGS84
    code_commune_insee TEXT,              -- Code INSEE commune
    code_departement TEXT,                -- Code département
    altitude_station DOUBLE PRECISION,    -- Altitude (m NGF)

    -- Caractéristiques station
    profondeur_investigation DOUBLE PRECISION, -- Profondeur max (m)
    libelle_pe TEXT,                      -- Libellé point d'eau
    date_debut_mesure DATE,               -- Première mesure
    date_fin_mesure DATE,                 -- Dernière mesure
    date_maj TIMESTAMP                    -- Dernière mise à jour
);
```

### Exemple 4 : hydrometry_obs_elab

Table volumineuse : observations hydrométrie élaborées (débits).

**Clé primaire** : `[code_station, date_obs_elab, grandeur_hydro_elab]`
**Mode ingestion** : FULL / YEAR / INCREMENTAL
**Volume typique** : ~15M+ records

```sql
CREATE TABLE hubeau.hydrometry_obs_elab (
    -- Identifiants
    code_station TEXT NOT NULL,           -- Code station hydrométrie
    code_site TEXT,                       -- Code site (parent station)

    -- Temporel
    date_obs_elab TIMESTAMP NOT NULL,     -- Date/heure observation

    -- Mesure
    grandeur_hydro_elab TEXT NOT NULL,    -- Type mesure (QmJ=débit journalier, H=hauteur)
    resultat_obs_elab DOUBLE PRECISION,   -- Valeur mesurée
    code_qualification TEXT,              -- Qualité donnée
    code_methode TEXT,                    -- Méthode mesure

    PRIMARY KEY (code_station, date_obs_elab, grandeur_hydro_elab)
);
```

### Exemple 5 : prelevements_points

Table référentiel volumineuse : points de prélèvement.

**Clé primaire** : `[code_prelevement]`
**Mode ingestion** : FULL uniquement
**Volume typique** : ~186k records

```sql
CREATE TABLE hubeau.prelevements_points (
    -- Identifiants
    code_prelevement TEXT PRIMARY KEY,    -- Code unique point prélèvement
    urn_prelevement TEXT,                 -- URN unique
    code_ouvrage TEXT,                    -- Code ouvrage parent

    -- Localisation
    longitude DOUBLE PRECISION,           -- Longitude WGS84
    latitude DOUBLE PRECISION,            -- Latitude WGS84
    code_commune_insee TEXT,              -- Code INSEE commune
    code_departement TEXT,                -- Code département

    -- Caractéristiques
    libelle_point TEXT,                   -- Libellé point
    type_point TEXT,                      -- Type (Puits, Forage, etc.)
    usage TEXT,                           -- Usage (AEP, Irrigation, etc.)
    date_ouverture DATE,                  -- Date ouverture
    date_fermeture DATE,                  -- Date fermeture
    date_maj TIMESTAMP                    -- Dernière mise à jour
);
```

---

## Conventions et Standards

### Nommage Tables

- **Format** : `{api}_{entity}` (ex: `piezometry_chroniques`, `quality_rivers_stations`)
- **Pluriel** : Pour collections (`stations`, `chroniques`, `analyses`)
- **Snake_case** : Toujours en minuscules avec underscores

### Nommage Colonnes

- **Snake_case** : `code_station`, `date_mesure`, `niveau_eau_ngf`
- **Préfixe `code_`** : Pour identifiants et clés étrangères (`code_bss`, `code_parametre`)
- **Préfixe `libelle_`** : Pour labels textuels (`libelle_station`, `libelle_parametre`)
- **Suffixe `_date`** : Pour dates (`date_mesure`, `date_prelevement`)
- **Suffixe `_timestamp`** : Pour timestamps (`timestamp_mesure`, `date_maj`)

### Types de Données

| Donnée | Type PostgreSQL | Exemple |
|--------|----------------|---------|
| Codes identifiants | `TEXT` | `code_bss`, `code_station` |
| Libellés/descriptions | `TEXT` | `libelle_station` |
| Nombres décimaux | `DOUBLE PRECISION` | `niveau_eau_ngf`, `resultat` |
| Dates | `DATE` | `date_mesure` (YYYY-MM-DD) |
| Timestamps | `TIMESTAMP` | `timestamp_mesure`, `date_maj` |
| Heures | `TIME` | `heure_prelevement` |
| Booléens | `BOOLEAN` | `en_service` |
| Coordonnées | `DOUBLE PRECISION` | `longitude`, `latitude` |

**Note** : PostGIS est activé mais les géométries ne sont pas créées automatiquement. Les coordonnées sont stockées comme `DOUBLE PRECISION` (`longitude`, `latitude`).

### Clés Primaires

**Stations/Référentiels** : Identifiant unique simple
```sql
PRIMARY KEY (code_bss)              -- piezometry_stations
PRIMARY KEY (code_station)          -- quality_rivers_stations
PRIMARY KEY (code_prelevement)      -- prelevements_points
PRIMARY KEY (code_analyse)          -- quality_rivers_analyses
```

**Chroniques/Observations** : Clés composites (station + timestamp/date + optionnel)
```sql
PRIMARY KEY (code_bss, timestamp_mesure)
-- piezometry_chroniques

PRIMARY KEY (code_station, date_obs_elab, grandeur_hydro_elab)
-- hydrometry_obs_elab

PRIMARY KEY (code_station, date_prelevement, code_operation)
-- quality_rivers_operations
```

### Déduplication (MERGE/UPSERT)

DLT gère automatiquement la déduplication via `write_disposition` :

**Mode FULL** : `write_disposition=replace`
- Tronque la table puis charge toutes les données
- Utilisation : refresh complet référentiels

**Modes YEAR/INCREMENTAL** : `write_disposition=append`
- MERGE/UPSERT sur clés primaires
- Si record existe (PK match) → UPDATE
- Si record nouveau → INSERT
- Utilisation : chargements incrémentaux sans doublons

### Performance

**Index automatiques DLT** :
- DLT crée automatiquement des index B-tree sur les clés primaires
- Pas besoin de créer manuellement les index standards

**Index personnalisés** (si requis pour analytics) :
```sql
-- Index sur dates pour requêtes temporelles
CREATE INDEX idx_piezo_chron_date
ON hubeau.piezometry_chroniques (timestamp_mesure);

-- Index sur codes stations pour JOINs
CREATE INDEX idx_qual_analyses_station
ON hubeau.quality_rivers_analyses (code_station);

-- Index sur paramètres pour filtres
CREATE INDEX idx_qual_analyses_param
ON hubeau.quality_rivers_analyses (code_parametre);

-- Index spatiaux PostGIS (si géométries créées)
CREATE INDEX idx_stations_location
ON hubeau.piezometry_stations
USING GIST (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326));
```

### Maintenance

**Vacuum automatique** : PostgreSQL le gère automatiquement

**Vacuum manuel** (si nécessaire après gros chargements) :
```sql
VACUUM ANALYZE hubeau.piezometry_chroniques;
VACUUM ANALYZE hubeau.quality_rivers_analyses;
```

**Stats optimizer** :
```sql
ANALYZE hubeau.piezometry_stations;
```

---

## Requêtes Courantes

### Compter records par table

```sql
SELECT
    schemaname,
    tablename,
    n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE schemaname = 'hubeau'
ORDER BY n_live_tup DESC;
```

### Lister toutes les tables Hub'Eau

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'hubeau'
  AND table_name NOT LIKE '_dlt%'
ORDER BY table_name;
```

### Vérifier dernière mise à jour

```sql
-- Dernière mesure piézométrique
SELECT MAX(timestamp_mesure) AS derniere_mesure
FROM hubeau.piezometry_chroniques;

-- Dernière analyse qualité
SELECT MAX(date_prelevement) AS dernier_prelevement
FROM hubeau.quality_rivers_analyses;
```

### Statistiques qualité données

```sql
-- Distribution qualifications piézométrie
SELECT
    code_qualification,
    COUNT(*) AS nb_mesures,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM hubeau.piezometry_chroniques
GROUP BY code_qualification
ORDER BY nb_mesures DESC;
```

### Stations avec données récentes

```sql
-- Stations piézo actives (mesures < 30 jours)
SELECT
    s.code_bss,
    s.libelle_pe,
    MAX(c.timestamp_mesure) AS derniere_mesure,
    COUNT(*) AS nb_mesures_30j
FROM hubeau.piezometry_stations s
JOIN hubeau.piezometry_chroniques c USING (code_bss)
WHERE c.timestamp_mesure >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY s.code_bss, s.libelle_pe
HAVING COUNT(*) > 0
ORDER BY derniere_mesure DESC;
```

---

## Ressources

- **PostgreSQL 16** : https://www.postgresql.org/docs/16/
- **PostGIS** : https://postgis.net/docs/
- **DLT** : https://dlthub.com/docs
- **Hub'Eau APIs** : https://hubeau.eaufrance.fr/

---

**Architecture Simple** : Hub'Eau APIs → DLT → PostgreSQL
**Un schéma, 22 tables, zéro transformation** 🌊
