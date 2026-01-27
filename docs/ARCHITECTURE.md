# Architecture Hub'Eau Pipeline

## Vue d'Ensemble

Pipeline de données en architecture Medallion (Bronze → Silver → Gold) pour l'ingestion et la transformation de données hydrologiques françaises.

```
┌─────────────────────────────────────────────────────────────────┐
│                      SOURCES DE DONNÉES                         │
├────────────────────────────────┬────────────────────────────────┤
│         APIs Hub'Eau           │      ERA5 (Copernicus CDS)     │
│  • Piézométrie (v1)            │      • Météo France            │
│  • Hydrométrie (v2)          │      • 1950-2025                │
└────────────────────────────────┴────────────────────────────────┘
                    │                            │
                    ▼                            ▼
         ┌──────────────────────────────────────────────────────┐
         │              DLT (Data Load Tool)                    │
         │  • Extraction API avec pagination                    │
         │  • Déduplication automatique (MERGE)                 │
         │  • Retry automatique                                 │
         │  • Partitionnement par année                         │
         └──────────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────────────┐
         │         PostgreSQL - Schéma: bronze                  │
         │  Tables brutes : *_raw                               │
         │  Seeds : tme_entites_hydrogeo                        │
         └──────────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────────────┐
         │            dbt Staging (Nettoyage)                   │
         │  • Type casting automatique                          │
         │  • Filtrage des valeurs NULL                         │
         │  • Renommage colonnes                                │
         │  • Validation des données                            │
         └──────────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────────────┐
         │         PostgreSQL - Schéma: silver                  │
         │  Tables nettoyées : stg_*                            │
         └──────────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────────────┐
         │    dbt Intermediate + Marts (Transformation)         │
         │  • Mapping spatial stations → grille ERA5            │
         │  • Agrégation quotidienne                            │
         │  • Jointures piézo + météo + TME                     │
         └──────────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────────────┐
         │         PostgreSQL - Schéma: gold                    │
         │  Tables transformées : int_* + marts                 │
         │  Table finale : hubeau_daily_chroniques              │
         └──────────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────────────┐
         │              Dagster (Orchestration)                 │
         │  • UI Web (port 49500)                               │
         │  • Jobs pour DLT et dbt                              │
         │  • UI Web (port 49500)                               │
         │  • Jobs pour DLT et dbt                              │
         │  • Monitoring et logs                                │
         │                                                      │
         │      +-----------------------------------------+     │
         │      |           Visualisation & BI            |     │
         │      |  • CloudBeaver (SQL Admin) : port 49503 |     │
         │      |  • Superset (BI/Dashboards): port 49504 |     │
         │      +-----------------------------------------+     │
         └──────────────────────────────────────────────────────┘
```

## Composants

### 1. DLT - Ingestion (Bronze)

**Rôle** : Extraire les données des APIs et les charger dans PostgreSQL.

**Caractéristiques** :
- Pagination automatique
- Déduplication (MERGE/UPSERT)
- Retry avec backoff exponentiel
- Partitionnement par année pour les chroniques
- Schéma automatique (inférence)

**Tables créées** : `bronze.*_raw`

**Configuration** : `configs/hubeau/*.yml` et `configs/era5/*.yml`

### 2. dbt - Transformation (Silver/Gold)

**Rôle** : Nettoyer, transformer et structurer les données.

**Layers** :

| Layer | Schéma | Matérialisation | Rôle |
|-------|--------|-----------------|------|
| **Staging** | `silver` | Table | Typage, renommage, filtrage NULL |
| **Intermediate** | `gold` | Table | Mapping spatial, agrégation |
| **Marts** | `gold` | Table | Tables finales prêtes pour l'analyse |

**Hooks automatiques** :
- Création d'index sur les tables sources au premier run
- Vérification de l'existence des schémas

### 3. PostgreSQL - Stockage

**Version** : PostgreSQL 16 + PostGIS 3.4

**Schémas** :

| Schéma | Gestion | Contenu |
|--------|---------|---------|
| `bronze` | DLT + dbt seeds | Tables brutes (`*_raw`) + référentiels |
| `silver` | dbt staging | Tables nettoyées (`stg_*`) |
| `gold` | dbt intermediate + marts | Tables transformées (`int_*` + marts) |

**Index automatiques** :
- `bronze.era5_france_timeseries` : `(latitude, longitude, time)`, `(time)`
- `bronze.piezometry_chroniques_raw` : `(code_bss, date_mesure)`
- `bronze.piezometry_stations_raw` : `(code_bss)`, `(x, y)`

### 4. Dagster - Orchestration

**Services** :

| Service | Port | Rôle |
|---------|------|------|
| `dagster_webserver` | 49500 | UI web pour monitoring et exécution |
| `dagster_daemon` | - | Exécution des jobs et sensors |
| `dlt_worker` | 4000 | Code server pour exécution DLT/dbt |

**Jobs** :
- Jobs d'ingestion (DLT) : partitionnés ou non
- Jobs de transformation (dbt) : pipeline complet

### 5. Visualisation
- **CloudBeaver** (Admin SQL) : Interface pour requêter la base directement.
- **Apache Superset** (BI) : Création de tableaux de bord connectés aux tables `Gold`. Utilise **Redis** pour le cache.

## Flux de Données

### Ingestion (DLT)

```
Job Dagster → Asset DLT → API Hub'Eau/ERA5 → PostgreSQL bronze.*_raw
```

**Exemple** :
1. Lancer `piezometry_chroniques_job` avec partition `2024`
2. DLT extrait les données de l'API Hub'Eau pour 2024
3. Données chargées dans `bronze.piezometry_chroniques_raw` (MERGE)

### Transformation (dbt)

```
Job Dagster → dbt build → PostgreSQL silver.* → gold.*
```

**Exemple** :
1. Lancer `dbt_silver_gold_pipeline_job`
2. dbt exécute les modèles staging → `silver.stg_piezo_chroniques`
3. dbt exécute les modèles intermediate → `gold.int_daily_measurements`
4. dbt exécute les modèles marts → `gold.hubeau_daily_chroniques`

## Mapping Spatial ERA5 ↔ Stations Piézo

### Principe

Les données ERA5 sont sur une **grille régulière** de 0.1° (~11 km).
Les stations piézométriques sont à des coordonnées précises.

**Algorithme** : Arrondir les coordonnées de la station au point de grille le plus proche.

```sql
era5_latitude  = ROUND(station_latitude * 10) / 10
era5_longitude = ROUND(station_longitude * 10) / 10
```

### Exemple

| Station | Lat originale | Lon originale | → ERA5 Lat | → ERA5 Lon |
|---------|---------------|---------------|------------|------------|
| BSS001 | 48.723 | 2.598 | 48.7 | 2.6 |
| BSS002 | 48.756 | 2.612 | 48.8 | 2.6 |

### Visualisation

```
      2.5       2.6       2.7
       │         │         │
 48.8 ─┼─────────●─────────┼─  ← Point grille ERA5 (48.8, 2.6)
       │         │  •BSS002│
       │    •BSS001        │
 48.7 ─┼─────────●─────────┼─  ← Point grille ERA5 (48.7, 2.6)
       │         │         │
```

**Résultat** :
- BSS001 (48.723, 2.598) → arrondi → (48.7, 2.6)
- BSS002 (48.756, 2.612) → arrondi → (48.8, 2.6)

## Tables Principales

### Bronze (DLT)

| Table | Description | Volume |
|-------|-------------|--------|
| `piezometry_stations_raw` | Stations BSS | ~23k |
| `piezometry_chroniques_raw` | Mesures piézo | ~23M |
| `hydrometry_stations_raw` | Stations hydro | ~5k |
| `hydrometry_obs_elab_raw` | Observations hydro | ~15M |
| `era5_france_meteo_raw` | Fichiers NetCDF ERA5 | ~38 fichiers |
| `era5_france_timeseries` | Time series ERA5 | ~300M |
| `tme_entites_hydrogeo` | Référentiel TME (seed) | ~2k |

### Silver (dbt staging)

| Table | Description | Source |
|-------|-------------|--------|
| `stg_piezo_chroniques` | Chroniques piézo nettoyées | `bronze.piezometry_chroniques_raw` |
| `stg_piezo_stations` | Stations piézo nettoyées | `bronze.piezometry_stations_raw` |
| `stg_hydrometry_stations` | Stations hydro nettoyées | `bronze.hydrometry_stations_raw` |
| `stg_hydrometry_obs_elab` | Observations hydro nettoyées | `bronze.hydrometry_obs_elab_raw` |
| `stg_era5_timeseries` | Time series ERA5 nettoyées | `bronze.era5_france_timeseries` |
| `stg_tme_entites` | TME nettoyé | `bronze.tme_entites_hydrogeo` |

### Gold (dbt intermediate + marts)

#### Intermediate

| Table | Description |
|-------|-------------|
| `int_daily_measurements` | Mesures quotidiennes agrégées (piézo) |
| `int_station_era5_mapping` | Mapping stations → grille ERA5 + métadonnées TME |
| `int_era5_for_stations` | ERA5 filtré pour les points de grille utilisés |

#### Marts

| Table | Description |
|-------|-------------|
| **`hubeau_daily_chroniques`** | **Table finale : Piézo + Météo + TME** |

**Table principale** : `gold.hubeau_daily_chroniques`
- Combine piézométrie + météo ERA5 + métadonnées TME
- **Toutes les colonnes d'observation sont non-nulles** (INNER JOIN)
- Prête pour l'analyse

## Docker Services

```yaml
postgres:          # PostgreSQL 16 + PostGIS (données)
dagster_postgres:  # PostgreSQL 16 (métadonnées Dagster)
dlt_worker:        # Worker (DLT + dbt)
dagster_webserver: # UI Dagster
dagster_daemon:    # Scheduler
adminer:           # UI PostgreSQL (Legacy)
cloudbeaver:       # Admin SQL Universel
superset:          # BI & Dashboards
redis:             # Cache pour Superset
```

## Performance

### Optimisations

1. **Index automatiques** : Créés par dbt au premier run
2. **Partitionnement** : Jobs partitionnés par année pour traitement incrémental
3. **Filtrage précoce** : Filtrage des NULL dans silver pour réduire le volume
4. **Agrégation** : Agrégation quotidienne dans intermediate

### Volumes de Données

| Schéma | Volume estimé |
|--------|---------------|
| `bronze` | ~50 GB |
| `silver` | ~30 GB |
| `gold` | ~10 GB |

## Sécurité

- Mots de passe via variables d'environnement (`.env` ou GitLab CI/CD)
- Pas de credentials dans le code
- Utilisateur `readonly` possible pour l'accès en lecture seule (script `scripts/create_readonly_user.sh`)
