# Architecture

Vue d'ensemble technique du pipeline : couches de données, orchestration et
infrastructure. Pour le détail des tables, voir [SCHEMA_BDD.md](SCHEMA_BDD.md) ;
pour l'exploitation, voir [OPERATIONS.md](OPERATIONS.md).

## Vue d'ensemble

```
Sources externes        Ingestion      Stockage            Transformation     Consommation
─────────────────       ─────────      ────────            ──────────────     ────────────
Hub'Eau (piézo, hydro)    DLT     →   PostgreSQL    →          dbt        →   Applications aval
Copernicus CDS (ERA5)               + TimescaleDB                              (SQL sur Gold,
BDLISA (TME)                        + PostGIS                                   ex. observatoire
                                          ↑                                     Junon)
                                  Dagster (orchestration)
```

- **DLT** ingère les sources dans le schéma `bronze` (pagination, retry, déduplication MERGE).
- **dbt** transforme Bronze → Silver → Gold (DAG de modèles SQL versionnés, tests qualité).
- **Dagster** orchestre l'ensemble (ingestion planifiée, transformation événementielle).
- **PostgreSQL** stocke tout ; TimescaleDB optimise les séries temporelles, PostGIS le spatial.
- Les **tables Gold** sont consommées directement en SQL par les applications aval. Les
  dépendances aval sont déclarées dans `models/exposures.yml`.

## Architecture Medallion

Les données traversent trois couches de qualité croissante, chacune dans son schéma PostgreSQL.

| Couche | Schéma | Outil | Matérialisation | Contenu |
|--------|--------|-------|-----------------|---------|
| Bronze | `bronze` | DLT | MERGE | Données brutes telles que reçues (colonnes `text`) |
| Silver | `silver` | dbt `staging/` | table / incrémental | Données nettoyées et typées (7 modèles) |
| Rejects | `silver_rejects` | dbt `rejects/` | table | Lignes filtrées avec motif de rejet (3 modèles) |
| Gold | `gold` | dbt `intermediate/` + `marts/` | table / incrémental | Tables analytiques enrichies (6 + 10 modèles) |

Le routage d'un modèle vers son schéma est géré par la macro
`macros/generate_schema_name.sql`.

### Bronze — ingestion (DLT)

Chaque source est un asset Dagster qui exécute un pipeline DLT vers `bronze`. DLT gère
la pagination des APIs, le retry (backoff exponentiel sur les 5xx), l'inférence de schéma
et la déduplication MERGE sur clé primaire. Les chroniques sont partitionnées par année.

Tables principales : `piezometry_stations_raw`, `piezometry_chroniques_raw`,
`hydrometry_sites_raw`, `hydrometry_stations_raw`, `hydrometry_obs_elab_raw`,
`era5_france_timeseries`, `tme_entites_hydrogeo`.

### Silver — nettoyage (dbt staging)

Chaque modèle `stg_*` sélectionne les colonnes utiles depuis Bronze, caste les types
(`text` → numeric/date/timestamp via les macros `cast_silver_*`), déduplique
(`DISTINCT ON`) et filtre les lignes invalides. Les lignes rejetées partent dans
`silver_rejects` avec une colonne `rejection_reason` (audit). Les modèles de chroniques
sont incrémentaux (`delete+insert`, fenêtre de lookback de 7 jours).

### Gold — analytique (dbt intermediate + marts)

- **Intermediate** : jointures et préparation. Le mapping spatial station → point de
  grille ERA5 le plus proche se fait dans `int_station_era5_mapping` /
  `int_hydro_station_era5_mapping` via une jointure KNN PostGIS (`CROSS JOIN LATERAL`
  + opérateur `<->`).
- **Marts** : tables finales destinées à la BI.
  - Faits quotidiens : `hubeau_daily_chroniques` (piézo + météo), `hydro_daily_chroniques`
    (hydro + météo) — **hypertables** TimescaleDB compressées.
  - Faits agrégés : `fct_monthly_*`, `fct_yearly_*` (tables simples, `delete+insert`).
  - Dimensions : `dim_date`, `dim_geography`, `dim_piezo_stations`, `dim_hydro_stations`.

### Indices standardisés (assets Dagster)

Les tables `gold.station_reference_stats`, `gold.fct_monthly_index` et
`gold.station_current_index` ne sont **pas** produites par dbt mais par des assets
Dagster Python (`assets/*_index_assets.py`), qui calculent l'indice piézométrique/
hydrologique standardisé (IPS/SSFI, 7 classes). La méthode est centralisée dans
`ml/indices.py`. Détail dans [SCHEMA_BDD.md](SCHEMA_BDD.md).

## Orchestration

L'ingestion est **planifiée** (schedules). La transformation dbt est **événementielle**
(sensors), déclenchée dès que les données Bronze arrivent — aucun schedule basé sur l'heure
pour dbt.

### Schedules (ingestion uniquement)

Activés par `DAGSTER_ENABLE_SCHEDULES=true`. Heures en UTC.

Les noms ci-dessous sont ceux affichés dans l'UI Dagster.

| Schedule | Cron | Job |
|----------|------|-----|
| ERA5 (mise à jour incrémentale) | `0 3 * * *` | `era5_weekly_update_job` |
| Bronze piézométrie (7 derniers jours) | `0 4 * * *` | `daily_piezometry_bronze` |
| Bronze hydrométrie (7 derniers jours) | `0 4 * * *` | `daily_hydrometry_bronze` |
| Documentation dbt | `0 5 * * 0` (dim.) | `dbt_docs_job` |
| Référentiel TME (BDLISA) | `0 2 1 * *` (1er du mois) | `reference_data_bronze` |
| Contrôle de complétude | `0 6 * * 1` (lun.) | `data_completeness_check` |
| Baseline de référence IPS | `0 7 * * 0` (dim.) | `station_reference_stats_refresh` |

### Sensors (chaîne de transformation)

Activés par `DAGSTER_ENABLE_SENSORS=true`.

```
Bronze (piézo + hydro) matérialisé
  └─ bronze_to_transform_sensor ──► dbt_transform        (tous les modèles, incrémental)
       ├─ transform_to_index_sensor   ──► station_index_refresh  (IPS/SSFI)
       └─ transform_to_quality_sensor ──► dbt_quality_job        (freshness + tests dbt)
```

`dbt_transform` exécute tous les modèles dbt ; le DAG `ref()` ordonne automatiquement
staging → intermediate → marts. À son succès, deux sensors se déclenchent en parallèle :
le rafraîchissement des indices (données) et les contrôles qualité (alerting non bloquant —
un test en échec produit un run en échec mais ne bloque pas le rafraîchissement des données).

> Les runs sont sérialisés globalement (`max_concurrent_runs=1`) : un seul run à la fois.

## Infrastructure Docker

Sept services orchestrés par `docker-compose.yml`.

| Service | Conteneur | Rôle |
|---------|-----------|------|
| `postgres` | `brgm-postgres` | PostgreSQL 16 + TimescaleDB + PostGIS (données) |
| `postgres_tuning` | `brgm-postgres-tuning` | Application des paramètres de tuning PostgreSQL |
| `dagster_postgres` | `brgm-dagster-postgres` | Métadonnées Dagster |
| `dlt_worker` | `brgm-dlt-worker` | Code métier (assets, jobs, DLT, dbt) — code-server gRPC |
| `dagster_webserver` | `brgm-dagster-webserver` | UI Dagster |
| `dagster_daemon` | `brgm-dagster-daemon` | Daemon (schedules, sensors, file d'attente) |
| `adminer` | `brgm-adminer` | Administration PostgreSQL |

### Worker et orchestrateur

Deux images séparées qui communiquent en **gRPC** (configuration dans
`dagster_home/workspace.yaml`) :

- **Worker** (`docker/worker/Dockerfile`, ~2 Go) : contient tout le code métier
  (Python + GDAL/GEOS pour le géospatial). Le manifest dbt est généré au build
  (`dbt deps && dbt parse`). Tous les runs s'exécutent ici. Code monté en volume
  (hot-reload).
- **Orchestrateur** (`docker/orchestrator/Dockerfile`, ~500 Mo) : webserver + daemon,
  **aucun code métier** — seulement les paquets Dagster. Se connecte au worker en gRPC.
  À reconstruire uniquement lors d'une montée de version Dagster.

### Volumes

Les volumes de données sont **externes** : ils ne sont pas supprimés par
`docker compose down -v`. Ils doivent être créés une fois via `scripts/init_volumes.sh`
avant le premier `docker compose up`.

### Hot-reload

| Modification | Action |
|--------------|--------|
| Code Python (`src/hubeau_pipeline/`) | `docker compose restart dlt_worker` puis recharger la code location dans Dagster UI |
| Modèles dbt (`src/dbt_hubeau/models/`) | Recharger les définitions dans Dagster UI |
| Configs YAML (`configs/`) | Aucune (montés en volume) |
| Dépendances (`pyproject.toml`) | `docker compose build --no-cache dlt_worker && docker compose up -d` |

## Structure du code

```
src/
├── hubeau_pipeline/              # Pipeline Dagster (Python)
│   ├── definitions.py            # Assemblage : assets, jobs, schedules, sensors, resources
│   ├── resources.py              # Connexions (PostgreSQL, DLT, dbt)
│   ├── schedules.py              # 7 schedules (ingestion)
│   ├── sensors.py                # 3 sensors (chaîne dbt)
│   ├── assets/
│   │   ├── bronze/               # Assets d'ingestion DLT (Hub'Eau, ERA5, TME)
│   │   ├── dbt_assets.py         # Pont dbt → Dagster (tous les modèles)
│   │   └── *_index_assets.py     # Indices IPS/SSFI (assets Python)
│   ├── jobs/                     # Définitions de jobs (ingestion, dbt, bootstrap, indices)
│   ├── sources/                  # Clients sources (hubeau_csv_source, era5_source)
│   ├── ml/                       # Calcul des indices (indices.py + persistance)
│   └── io/                       # NoOpIOManager (DLT écrit directement dans PG)
│
└── dbt_hubeau/                   # Projet dbt
    ├── dbt_project.yml           # Config (variables, matérialisation, hooks)
    ├── profiles.yml              # Connexion PostgreSQL (env vars)
    ├── macros/                   # cast_silver, timescaledb, make_point, constraints, ...
    ├── seeds/                    # ref_stations_meteeau_bsn.csv
    └── models/{staging,rejects,intermediate,marts}/   # Modèles SQL + schema.yml

configs/                          # Configuration YAML des sources (hubeau, era5, bdlisa)
docker/                           # Dockerfiles + init SQL + config des services
scripts/                          # init_volumes.sh, create_readonly_user.sh, server_deploy.sh, ...
```

## Domaines de données

- **Piézométrie** : niveaux des nappes souterraines (stations + chroniques, depuis 1967).
- **Hydrométrie** : débits des rivières (sites → stations → observations, depuis 2000).
- **Climat** : réanalyse ERA5 (température, précipitations, évaporation) sur une grille
  France ~0.1° — voir [ERA5.md](ERA5.md).
- **Référentiel** : entités hydrogéologiques TME (BDLISA), utilisées pour enrichir les stations.
