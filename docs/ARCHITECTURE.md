# Architecture Hub'Eau Pipeline

## Vue d'Ensemble

Pipeline de données en architecture Medallion (Bronze → Silver → Gold) pour l'ingestion et la transformation de données hydrologiques françaises.

```
┌─────────────────────────────────────────────────────────────────┐
│                      SOURCES DE DONNÉES                         │
├────────────────────────────────┬────────────────────────────────┤
│         APIs Hub'Eau           │      ERA5 (Copernicus CDS)     │
│  • Piézométrie (v1)            │      • Temps/Météo             │
│  • Hydrométrie (v2)            │      • 1950-2025 (Historique)  │
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
         │  Tables brutes : *_raw ; BDLISA + Sandre (ref_*_eh)  │
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
         │  Fact Tables : hubeau_daily, fct_monthly, fct_yearly │
         │  Optimisation : Hypertables TimescaleDB + Compression│
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
| `bronze` | DLT + assets Dagster | Tables brutes (`*_raw`) + BDLISA + nomenclatures Sandre (`ref_*_eh`) |
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
- **Apache Superset** (BI) : Objectif à terme — exploiter l’ensemble des données dans Superset (dashboards, cartes, calques : BDLISA, stations, chroniques, météo). Connexion aux tables Silver/Gold, PostGIS pour les calques cartographiques. Utilise **Redis** pour le cache. Voir [docs/SUPERSET.md](SUPERSET.md).

### 6. TimescaleDB (Performance)
- **Rôle** : Optimiser le stockage et la requête des séries temporelles.
- **Fonctionnalités** :
  - **Hypertables** : Partitionnement automatique par temps.
  - **Compression** : Réduction de la taille de stockage (90%+) sur les données historiques.
  - **Chunk Exclusion** : Scan uniquement les partitions nécessaires pour une requête donnée.

## Flux de Données

### Ingestion (DLT)

```
Job Hub'Eau → Asset DLT → API Hub'Eau → PostgreSQL bronze.*_raw
Job ERA5 → API CDS → (In-Memory Processing) → PostgreSQL bronze.era5_france_timeseries
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

Les données ERA5 sont sur une grille régulière de 0.1° (~11 km).
Pour chaque station piézométrique, nous cherchons le point de grille ERA5 le plus proche (Nearest Neighbor) pour lui attribuer les données météo locales.

**Algorithme** : Recherche du voisin le plus proche via **PostGIS KNN** (opérateur `<->`).
Cela garantit une précision géodésique bien supérieure à un simple arrondissement de coordonnées.

```sql
SELECT ...
FROM stations s
CROSS JOIN LATERAL (
    SELECT latitude, longitude
    FROM era5_grid e
    ORDER BY s.geom <-> e.geom  -- Nearest Neighbor (KNN)
    LIMIT 1
) e
```

### Visualisation

```
      Grid Point A      Grid Point B
           ●                 ●
           │                 │
           │        Station S│
           │           ★     │     Distance(S, A) = 4.2 km
           │          / \    │     Distance(S, B) = 3.1 km
           │         /   \---|---▶ Selected: B (Nearest)
           │        /        │
           ●                 ●
      Grid Point C      Grid Point D
```

**Résultat** :
Chaque station est reliée à son point de grille "réellement" le plus proche géographiquement.

## Tables Principales

### Bronze (DLT)

| Table | Description | Volume |
|-------|-------------|--------|
| `piezometry_stations_raw` | Stations BSS | ~23k |
| `piezometry_chroniques_raw` | Mesures piézo | ~23M |
| `hydrometry_stations_raw` | Stations hydro | ~5k |
| `hydrometry_obs_elab_raw` | Observations hydro | ~15M |
| `hydrometry_obs_elab_raw` | Observations hydro | ~15M |
| `era5_france_timeseries` | Time series ERA5 (Direct Load) | ~300M |
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
- **Hypertable** : Partitionnée par date (1 an) + Compressée (après 1 an)

**Nouveaux Marts Analytiques** :
- `fct_monthly_chroniques` : Agrégats mensuels + variations (Hypertable 5 ans)
- `fct_yearly_stats` : Bilans annuels + classifications (Hypertable 10 ans)
- `dim_piezo_stations` : Master data stations enrichi
- `agg_station_trends` : Tendances saisonnières et projections

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
