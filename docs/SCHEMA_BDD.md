# Schéma Base de Données

Structure des tables PostgreSQL du pipeline Hub'Eau.

## Architecture

```
Hub'Eau APIs ──┐
               ├──▶ DLT ──▶ bronze.* ──▶ dbt ──▶ silver.* ──▶ dbt ──▶ gold.*
ERA5 API ──────┘
```

## Schémas

| Schéma | Gestion | Contenu |
|--------|---------|---------|
| `bronze` | DLT + dbt seeds | Tables brutes (`*_raw`) + référentiels |
| `silver` | dbt staging | Tables nettoyées (`stg_*`) |
| `gold` | dbt intermediate + marts | Tables transformées (`int_*` + marts) |

## 🔥 Optimisations TimescaleDB

Les tables suivantes sont converties en **Hypertables** pour la performance :
- **Silver** : `stg_piezo_chroniques`, `stg_hydrometry_obs_elab`, `stg_era5_timeseries`
- **Gold** : `int_daily_measurements`, `int_era5_for_stations`
- **Marts** : `hubeau_daily_chroniques` (Compressée), `fct_monthly_chroniques`, `fct_yearly_stats`

---

## Tables Bronze (DLT)

Tables créées automatiquement par DLT au premier run.

### Piézométrie

| Table | Description | Volume estimé |
|-------|-------------|---------------|
| `piezometry_stations_raw` | Stations BSS | ~23k |
| `piezometry_chroniques_raw` | Mesures niveaux nappes | ~23M |

**Colonnes principales** :
- `piezometry_stations_raw` : `code_bss`, `x`, `y`, `nom_commune`, `code_departement`, etc.
- `piezometry_chroniques_raw` : `code_bss`, `date_mesure`, `niveau_nappe_eau`, `profondeur_nappe`, etc.

### Hydrométrie

| Table | Description | Volume estimé |
|-------|-------------|---------------|
| `hydrometry_sites_raw` | Sites hydrométriques | ~5k |
| `hydrometry_stations_raw` | Stations hydrométriques | ~5k |
| `hydrometry_obs_elab_raw` | Observations élaborées | ~15M |

**Colonnes principales** :
- `hydrometry_stations_raw` : `code_station`, `x`, `y`, `code_entite`, etc.
- `hydrometry_obs_elab_raw` : `code_entite`, `date_obs_elab`, `resultat_obs_elab`, etc.

### ERA5 (Copernicus)

| Table | Description | Volume estimé |
|-------|-------------|---------------|
| `era5_france_meteo_raw` | Fichiers NetCDF bruts | ~38 fichiers |
| `era5_france_timeseries` | Time series extraites | ~300M |

**Colonnes principales** :
- `era5_france_meteo_raw` : `file_id`, `netcdf_data` (BYTEA), `start_year`, `end_year`, etc.
- `era5_france_timeseries` : `time`, `latitude`, `longitude`, `temperature_2m`, `total_precipitation`, `potential_evaporation`

### Référentiels (Seeds)

| Table | Description | Volume |
|-------|-------------|--------|
| `tme_entites_hydrogeo` | Entités hydrogéologiques (TME) | ~2k |

**Source** : Seed dbt (`src/dbt_hubeau/seeds/tme_entites_hydrogeo.csv`)

### Métadonnées DLT

| Table | Description |
|-------|-------------|
| `_dlt_loads` | Historique des chargements |
| `_dlt_pipeline_state` | État des pipelines |

---

## Tables Silver (dbt staging)

Tables nettoyées et standardisées depuis bronze.

| Table | Description | Source | Filtres |
|-------|-------------|--------|---------|
| `stg_piezo_chroniques` | Chroniques piézo nettoyées | `bronze.piezometry_chroniques_raw` | NULL filtrés |
| `stg_piezo_stations` | Stations piézo nettoyées | `bronze.piezometry_stations_raw` | Coordonnées non-nulles |
| `stg_hydrometry_stations` | Stations hydro nettoyées | `bronze.hydrometry_stations_raw` | Coordonnées non-nulles |
| `stg_hydrometry_obs_elab` | Observations hydro nettoyées | `bronze.hydrometry_obs_elab_raw` | Observations non-nulles |
| `stg_era5_timeseries` | Time series ERA5 nettoyées | `bronze.era5_france_timeseries` | Observations non-nulles |
| `stg_tme_entites` | TME nettoyé | `bronze.tme_entites_hydrogeo` | Valeurs 'X' converties en NULL |

**Transformations appliquées** :
- Type casting (VARCHAR → NUMERIC, DATE, etc.)
- Renommage colonnes (standardisation)
- Filtrage des valeurs NULL
- Nettoyage des valeurs invalides ('X' → NULL)

---

## Tables Gold (dbt intermediate + marts)

Tables transformées et prêtes pour l'analyse.

### Intermediate

| Table | Description | Source |
|-------|-------------|--------|
| `int_daily_measurements` | Mesures quotidiennes agrégées (piézo) | `silver.stg_piezo_chroniques` |
| `int_station_era5_mapping` | Mapping stations → grille ERA5 + métadonnées TME | `silver.stg_piezo_stations` + `silver.stg_tme_entites` |
| `int_era5_for_stations` | ERA5 filtré pour les points de grille utilisés | `silver.stg_era5_timeseries` |

**Détails** :
- `int_daily_measurements` : Agrégation par `code_bss` et `date_mesure` (AVG)
- `int_station_era5_mapping` : Mapping spatial + jointure avec TME
- `int_era5_for_stations` : Filtrage ERA5 sur les points de grille utilisés par les stations

### Marts

#### `hubeau_daily_chroniques`

**Table finale** : Combine piézométrie + météo ERA5 + métadonnées TME.

| Colonne | Type | Description |
|---------|------|-------------|
| `code_bss` | VARCHAR | ID station BSS |
| `date` | DATE | Date mesure |
| **Observations** | | |
| `niveau_nappe_eau` | NUMERIC | Niveau nappe (m) - **NON NULL** |
| `profondeur_nappe` | NUMERIC | Profondeur (m) - **NON NULL** |
| `temperature_2m` | NUMERIC | Température ERA5 (°C) - **NON NULL** |
| `total_precipitation` | NUMERIC | Précipitations ERA5 (mm) - **NON NULL** |
| `potential_evaporation` | NUMERIC | Évaporation ERA5 (mm) - **NON NULL** |
| **Métadonnées station** | | |
| `codes_bdlisa` | VARCHAR | Codes BD-LISA |
| `code_commune_insee` | VARCHAR | Code INSEE |
| `nom_commune` | VARCHAR | Nom commune |
| `altitude_station` | NUMERIC | Altitude (m) |
| `code_departement` | VARCHAR | Code département |
| `nom_departement` | VARCHAR | Nom département |
| **Métadonnées TME** | | |
| `code_eh` | VARCHAR | Code entité hydrogéologique |
| `libelle_eh` | VARCHAR | Libellé EH |
| `niveau_eh` | VARCHAR | Niveau EH |
| `etat_eh` | VARCHAR | État EH |
| `nature_eh` | VARCHAR | Nature EH |
| `milieu_eh` | VARCHAR | Milieu EH |
| `theme_eh` | VARCHAR | Thème EH |
| `origine_eh` | VARCHAR | Origine EH |
| **Coordonnées** | | |
| `station_latitude` | NUMERIC | Latitude station réelle |
| `station_longitude` | NUMERIC | Longitude station réelle |
| `era5_latitude` | NUMERIC | Latitude point grille ERA5 |
| `era5_longitude` | NUMERIC | Longitude point grille ERA5 |

**Index** :
- `(code_bss, date)` - Recherche par station et date
- `(date)` - Recherche par date
- `(code_departement)` - Recherche par département
- `(code_eh)` - Recherche par entité hydrogéologique

**Contraintes** :
- **Toutes les colonnes d'observation sont NON NULL** (INNER JOIN)
- Une ligne = une mesure piézo + météo ERA5 + métadonnées TME pour une date donnée

---

## Index Automatiques

dbt crée automatiquement les index suivants au premier run :

```sql
-- ERA5
CREATE INDEX IF NOT EXISTS idx_era5_lat_lon_time 
    ON bronze.era5_france_timeseries (latitude, longitude, time);
CREATE INDEX IF NOT EXISTS idx_era5_time 
    ON bronze.era5_france_timeseries (time);

-- Piézométrie
CREATE INDEX IF NOT EXISTS idx_piezo_chroniques_full 
    ON bronze.piezometry_chroniques_raw (code_bss, date_mesure);
CREATE INDEX IF NOT EXISTS idx_piezo_stations_coords 
    ON bronze.piezometry_stations_raw (x, y);
CREATE INDEX IF NOT EXISTS idx_piezo_stations_code_bss 
    ON bronze.piezometry_stations_raw (code_bss);
```

---

## Requêtes Courantes

### Volume des Tables

```sql
SELECT 
    schemaname, 
    tablename, 
    n_live_tup AS rows,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY schemaname, n_live_tup DESC;
```

### Dernière Donnée Disponible

```sql
SELECT MAX(date) AS derniere_date 
FROM gold.hubeau_daily_chroniques;
```

### Nombre de Stations par Département

```sql
SELECT 
    code_departement, 
    nom_departement,
    COUNT(DISTINCT code_bss) AS nb_stations,
    COUNT(*) AS nb_mesures
FROM gold.hubeau_daily_chroniques
GROUP BY code_departement, nom_departement
ORDER BY nb_mesures DESC;
```

### Données pour une Station

```sql
SELECT 
    date,
    niveau_nappe_eau,
    profondeur_nappe,
    temperature_2m,
    total_precipitation,
    potential_evaporation
FROM gold.hubeau_daily_chroniques
WHERE code_bss = 'BSS001XX0001'
ORDER BY date DESC
LIMIT 100;
```

### Statistiques par Entité Hydrogéologique

```sql
SELECT 
    code_eh,
    libelle_eh,
    COUNT(DISTINCT code_bss) AS nb_stations,
    COUNT(*) AS nb_mesures,
    MIN(date) AS date_debut,
    MAX(date) AS date_fin
FROM gold.hubeau_daily_chroniques
GROUP BY code_eh, libelle_eh
ORDER BY nb_mesures DESC;
```

### Température Moyenne par Mois

```sql
SELECT 
    DATE_TRUNC('month', date) AS mois,
    AVG(temperature_2m) AS temp_moyenne_celsius,
    SUM(total_precipitation) AS precip_totale_mm
FROM gold.hubeau_daily_chroniques
WHERE date >= '2023-01-01'
GROUP BY DATE_TRUNC('month', date)
ORDER BY mois;
```

---

## Schémas Création

Les schémas sont créés automatiquement :
- `bronze` : Créé par DLT au premier run
- `silver` : Créé par dbt au premier run
- `gold` : Créé par dbt au premier run

**Note** : Si les schémas n'existent pas, ils seront créés automatiquement lors du premier run des jobs.
