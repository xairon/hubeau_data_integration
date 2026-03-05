# Guide d'Onboarding - Hub'Eau Data Pipeline

> Ce document est destiné à un nouveau développeur qui rejoint le projet. Il couvre tout : les concepts, les outils, l'architecture, les fichiers et les procédures opérationnelles.

---

## Table des matières

1. [Présentation du projet](#1-présentation-du-projet)
2. [Les technologies et leur rôle](#2-les-technologies-et-leur-rôle)
3. [Architecture globale](#3-architecture-globale)
4. [Structure du code](#4-structure-du-code)
5. [La couche Bronze : ingestion avec DLT](#5-la-couche-bronze--ingestion-avec-dlt)
6. [La couche Silver : staging avec dbt](#6-la-couche-silver--staging-avec-dbt)
7. [La couche Gold : analytique avec dbt](#7-la-couche-gold--analytique-avec-dbt)
8. [Orchestration avec Dagster](#8-orchestration-avec-dagster)
9. [Infrastructure Docker](#9-infrastructure-docker)
10. [Démarrage du projet](#10-démarrage-du-projet)
11. [Opérations courantes](#11-opérations-courantes)
12. [Dépannage](#12-dépannage)
13. [Glossaire](#13-glossaire)

---

## 1. Présentation du projet

### Objectif

Ce projet est un **entrepôt de données** (data warehouse) pour les données hydrologiques françaises. Il collecte automatiquement :

- **La piézométrie** : niveaux des nappes souterraines (~1 900 stations, données depuis 1967)
- **L'hydrométrie** : débits des rivières (~4 200 stations, données depuis 2000)
- **La météo ERA5** : données climatiques de réanalyse (température, précipitations, vent, humidité) sur une grille couvrant la France
- **Des référentiels** : entités hydrogéologiques (BDLISA/TME), nomenclatures SANDRE, découpage géographique

Ces données sont nettoyées, enrichies (jointure spatiale stations/grille météo), agrégées (jour, mois, année) et servies via des dashboards Apache Superset.

### Les sources de données

| Source | API / Format | Ce qu'on récupère |
|--------|-------------|-------------------|
| [Hub'Eau Piézométrie](https://hubeau.eaufrance.fr/page/api-piezometrie) | REST API → CSV | Stations + chroniques de niveaux piézométriques |
| [Hub'Eau Hydrométrie](https://hubeau.eaufrance.fr/page/api-hydrometrie) | REST API → CSV | Sites, stations + observations de débit |
| [Copernicus CDS](https://cds.climate.copernicus.eu/) | CDS API → NetCDF | Grille ERA5 sur la France (0.25° de résolution) |
| [BDLISA](https://bdlisa.eaufrance.fr/) | GeoPackage | Entités hydrogéologiques (géométries + attributs) |
| [SANDRE](https://www.sandre.eaufrance.fr/) | REST API → JSON | Nomenclatures de référence |

---

## 2. Les technologies et leur rôle

### Vue d'ensemble

```
DLT (ingestion) → PostgreSQL (stockage) → dbt (transformation) → Superset (visualisation)
                         ↑                        ↑
                   TimescaleDB              Dagster (orchestration)
                    + PostGIS
```

### DLT (Data Load Tool)

**Quoi** : Bibliothèque Python open-source d'ingestion de données.

**Pourquoi on l'utilise** : DLT gère automatiquement le schéma des tables, la pagination des APIs, la déduplication (MERGE), et l'écriture dans PostgreSQL. On n'écrit pas de SQL d'insertion — DLT s'en charge.

**Comment ça marche dans ce projet** :
- On définit des "sources" DLT (des fonctions Python qui `yield` des dictionnaires)
- Chaque source sait paginer l'API Hub'Eau ou lire un fichier NetCDF
- DLT écrit les données dans le schéma `bronze` de PostgreSQL
- Le mode `MERGE` déduplique automatiquement via une clé primaire

**Fichiers clés** :
- `src/hubeau_pipeline/sources/hubeau_csv_source.py` — Client API Hub'Eau (pagination, retry)
- `src/hubeau_pipeline/sources/era5_source.py` — Client Copernicus CDS (NetCDF)
- `src/hubeau_pipeline/assets/bronze/dlt_assets.py` — Définition des assets DLT Dagster

### dbt (Data Build Tool)

**Quoi** : Outil de transformation SQL. On écrit du SQL avec des templates Jinja2, et dbt gère l'ordre d'exécution, les dépendances entre modèles, et les tests de qualité.

**Pourquoi on l'utilise** : dbt permet de versionner nos transformations SQL dans Git, d'avoir un DAG (graphe de dépendances) automatique, et de tester la qualité des données (valeurs nulles, unicité, intégrité référentielle).

**Comment ça marche dans ce projet** :
- Chaque fichier `.sql` dans `src/dbt_hubeau/models/` est un "modèle" dbt
- Un modèle = une table ou vue PostgreSQL
- Les modèles se référencent entre eux via `{{ ref('nom_modele') }}`
- Les tests sont définis dans les fichiers `schema.yml` à côté des modèles
- Les macros (dans `macros/`) sont des fonctions Jinja2 réutilisables

**Fichiers clés** :
- `src/dbt_hubeau/dbt_project.yml` — Configuration globale (variables, matérialisation par défaut)
- `src/dbt_hubeau/profiles.yml` — Connexion PostgreSQL (variables d'environnement)
- `src/dbt_hubeau/models/` — Les 31 modèles SQL organisés par couche
- `src/dbt_hubeau/macros/` — 6 macros personnalisées

### Dagster

**Quoi** : Orchestrateur de pipelines de données. C'est l'équivalent d'Airflow mais avec une approche "asset-centric" (centrée sur les données, pas les tâches).

**Pourquoi on l'utilise** : Dagster nous donne une interface web pour lancer les pipelines, visualiser les dépendances, planifier les exécutions (schedules), et réagir à des événements (sensors).

**Comment ça marche dans ce projet** :
- Chaque table Bronze est un "asset" Dagster (via DLT)
- Tous les modèles dbt sont exposés comme un seul asset composite (`hubeau_dbt_assets`)
- Les "jobs" groupent des assets pour une exécution séquentielle ou parallèle
- Les "schedules" déclenchent des jobs à heures fixes (ingestion Bronze uniquement)
- Les "sensors" déclenchent la chaîne dbt quand les données Bronze arrivent (event-driven)

**Fichiers clés** :
- `src/hubeau_pipeline/definitions.py` — Point d'assemblage (assets + jobs + schedules + sensors + resources)
- `src/hubeau_pipeline/jobs/` — 22 définitions de jobs
- `src/hubeau_pipeline/schedules.py` — 5 planifications cron
- `src/hubeau_pipeline/sensors.py` — 3 sensors event-driven

### PostgreSQL + TimescaleDB + PostGIS

**Quoi** : Base de données relationnelle avec deux extensions :
- **TimescaleDB** : optimise les séries temporelles (hypertables, compression automatique)
- **PostGIS** : gère les données géospatiales (géométries, jointures spatiales)

**Pourquoi on l'utilise** :
- PostgreSQL est robuste et familier
- TimescaleDB compresse les données historiques à 90%+ et optimise les requêtes temporelles
- PostGIS permet les jointures spatiales (ex: trouver la cellule ERA5 la plus proche d'une station)

**Concepts clés** :
- **Hypertable** : table PostgreSQL partitionnée automatiquement par le temps (chunks de 1 mois ou 1 an)
- **Compression** : les chunks anciens sont compressés (90%+ d'économie d'espace)
- **KNN spatial** : opérateur `<->` pour trouver le point le plus proche

### Apache Superset

**Quoi** : Plateforme de Business Intelligence (dashboards, graphiques, cartes).

**Pourquoi on l'utilise** : Superset se connecte directement à PostgreSQL et permet de créer des dashboards interactifs sur les tables Gold sans écrire de code.

### Docker Compose

**Quoi** : Outil qui orchestre 13 conteneurs Docker pour faire tourner toute l'infrastructure.

**Pourquoi on l'utilise** : Un seul `docker compose up` lance tout : base de données, orchestrateur, worker, BI, monitoring.

---

## 3. Architecture globale

### Architecture Medallion (Bronze → Silver → Gold)

C'est un pattern classique en data engineering. Les données passent par 3 couches de qualité croissante :

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        SOURCES EXTERNES                                 │
│  Hub'Eau API (piézo + hydro)  │  Copernicus CDS (ERA5)  │  BDLISA/SANDRE│
└──────────────┬───────────────────────────┬───────────────────────┬──────┘
               │         DLT              │                       │
               ▼                          ▼                       ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  BRONZE (schéma: bronze)                                                │
│  Données brutes telles que reçues. Colonnes en text.                    │
│  Dédupliquées par DLT (MERGE). Partitionnées par année.                 │
│  Tables: piezometry_stations_raw, piezometry_chroniques_raw,            │
│          hydrometry_*, era5_france_timeseries, tme_*, sandre_*, ref_*    │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │  dbt staging/
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  SILVER (schéma: silver)                                                │
│  Données nettoyées et typées. Dédupliquées. Index créés.                │
│  Rejects isolés dans silver_rejects.                                    │
│  Tables: stg_piezo_chroniques, stg_hydrometry_obs_elab,                 │
│          stg_era5_timeseries, stg_piezo_stations, ...                    │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │  dbt intermediate/ + marts/
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  GOLD (schéma: gold)                                                    │
│  Tables analytiques finales, enrichies, agrégées.                       │
│                                                                          │
│  Intermediate: jointures spatiales ERA5, mappings stations-grille        │
│  Marts:                                                                  │
│    - hubeau_daily_chroniques (hypertable, piézo + météo, jour)           │
│    - hydro_daily_chroniques  (hypertable, hydro + météo, jour)           │
│    - fct_monthly_*, fct_yearly_* (agrégats mois/année)                   │
│    - dim_date, dim_geography, dim_*_stations (dimensions)                │
│    - agg_*_trends (tendances), stations_*_carte (cartes Superset)        │
└──────────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
                        ┌─────────────┐
                        │  SUPERSET    │
                        │  Dashboards  │
                        └─────────────┘
```

### Pipeline quotidien (event-driven)

L'ingestion est planifiée par horaires. La transformation dbt est 100% pilotée par des sensors (event-driven) :

```
3h00 UTC ─── ERA5 Smart Update (Bronze)
4h00 UTC ─── Hub'Eau Bronze : piézométrie + hydrométrie (parallèle)
                    │
                    ▼ sensor : bronze_to_shared_staging_sensor
                    │
             dbt Shared Staging (ERA5 timeseries + grid points)
                    │
                    ▼ sensor : shared_staging_to_domain_sensor
                    │
         ┌──────────┴──────────┐
         ▼                     ▼
    dbt Piézo pipeline    dbt Hydro pipeline    (parallèle)
         │                     │
         └──────────┬──────────┘
                    ▼ sensor : domain_to_dimensions_sensor
                    │
             dbt Shared Dimensions (dim_date, dim_geography)
```

### Schémas PostgreSQL

| Schéma | Couche | Contenu |
|--------|--------|---------|
| `bronze` | Bronze | Tables brutes DLT |
| `silver` | Silver | Tables staging dbt (nettoyées, typées) |
| `silver_rejects` | Rejects | Lignes filtrées (qualité insuffisante) |
| `gold` | Gold | Tables intermédiaires + marts finaux |

---

## 4. Structure du code

### Arborescence simplifiée

```
hubeau_data_integration/
│
├── src/
│   ├── hubeau_pipeline/          # Code Python — orchestration Dagster
│   │   ├── __init__.py           # Point d'entrée (exporte `defs`)
│   │   ├── definitions.py        # Assemblage Dagster (assets, jobs, schedules, sensors, resources)
│   │   ├── resources.py          # Connexions : PostgreSQLResource, DagsterDltResource
│   │   ├── schedules.py          # 5 planifications cron (ingestion seulement)
│   │   ├── sensors.py            # 3 sensors event-driven (chaîne dbt)
│   │   ├── utils.py              # Helpers (parsing env vars, comptage lignes DLT)
│   │   │
│   │   ├── assets/
│   │   │   ├── dbt_assets.py     # Pont Dagster ↔ dbt (expose les modèles dbt comme assets)
│   │   │   └── bronze/           # Assets d'ingestion DLT
│   │   │       ├── dlt_assets.py           # Hub'Eau + ERA5 (10 assets principaux)
│   │   │       ├── era5_assets.py          # ERA5 historique + update hebdo
│   │   │       ├── tme_entites_assets.py   # Enrichissement TME depuis BDLISA
│   │   │       ├── sandre_nomenclatures_assets.py  # Nomenclatures SANDRE
│   │   │       ├── referentiel_geo_assets.py       # Géographie (régions, départements)
│   │   │       ├── bdlisa_assets.py        # GeoPackage loader
│   │   │       └── bdlisa_csv_assets.py    # Fallback CSV pour BDLISA
│   │   │
│   │   ├── jobs/                 # Définitions de jobs (22 au total)
│   │   │   ├── hubeau_jobs.py    # Jobs piézométrie + hydrométrie (8)
│   │   │   ├── era5_jobs.py      # Jobs ERA5 (2)
│   │   │   ├── dbt_jobs.py       # Jobs de transformation dbt (9)
│   │   │   ├── reference_data_jobs.py    # Job données de référence (1)
│   │   │   └── full_bootstrap_job.py     # Chargement initial complet (1)
│   │   │
│   │   ├── sources/              # Clients de sources de données
│   │   │   ├── hubeau_csv_source.py  # Client Hub'Eau (pagination, retry, CSV)
│   │   │   └── era5_source.py        # Client Copernicus CDS (NetCDF, xarray)
│   │   │
│   │   └── io/
│   │       └── io_managers.py    # NoOpIOManager (DLT écrit directement dans PG)
│   │
│   └── dbt_hubeau/               # Projet dbt — transformations SQL
│       ├── dbt_project.yml       # Config globale (variables, matérialisation, hooks)
│       ├── profiles.yml          # Connexion PostgreSQL (env vars)
│       ├── packages.yml          # Dépendances (dbt_utils)
│       │
│       ├── macros/               # Fonctions Jinja2 réutilisables
│       │   ├── generate_schema_name.sql    # Routage vers le bon schéma (silver, gold)
│       │   ├── cast_silver.sql             # Cast typé depuis Bronze (text → numeric/date/...)
│       │   ├── timescaledb.sql             # Gestion hypertables + compression
│       │   ├── make_point.sql              # Création de géométries PostGIS
│       │   ├── constraints.sql             # PK/FK idempotentes
│       │   └── incremental_predicates.sql  # Filtres pour delete+insert incrémental
│       │
│       └── models/               # 31 modèles SQL
│           ├── staging/          # Silver (7 modèles) — nettoyage et typage
│           ├── rejects/          # Rejects (3 modèles) — lignes filtrées
│           ├── intermediate/     # Gold intermédiaire (7 modèles) — jointures spatiales
│           └── marts/            # Gold final (14 modèles) — tables analytiques
│
├── configs/                      # Configuration YAML des sources
│   ├── hubeau/                   # Endpoints Hub'Eau (URL, pagination, clés primaires)
│   ├── era5/                     # Paramètres ERA5 (bbox France, variables, résolution)
│   └── bdlisa/                   # Configuration GeoPackage BDLISA
│
├── docker/                       # Fichiers Docker
│   ├── worker/Dockerfile         # Image worker (~2 Go, Python + GDAL/GEOS)
│   ├── orchestrator/Dockerfile   # Image orchestrateur (~500 Mo, léger)
│   ├── postgres/                 # Init SQL + tuning Patroni
│   ├── superset/                 # Config Superset + init script
│   ├── grafana/                  # Dashboards pré-configurés
│   ├── prometheus/               # Config métriques
│   └── monitoring/               # Exporters custom
│
├── scripts/                      # Scripts utilitaires
│   ├── init_volumes.sh           # Création des volumes Docker (OBLIGATOIRE avant 1er lancement)
│   ├── create_readonly_user.sh   # Utilisateur PostgreSQL en lecture seule
│   ├── server_deploy.sh          # Déploiement production
│   └── diagnose_*.py/sql         # Scripts de diagnostic
│
├── docker-compose.yml            # Orchestration de 13 conteneurs
├── pyproject.toml                # Dépendances Python + config linting
├── .env.example                  # Template des variables d'environnement
└── .gitlab-ci.yml                # CI/CD (génération docs dbt → GitLab Pages)
```

### Où trouver quoi ?

| Je veux... | Fichier(s) |
|------------|------------|
| Ajouter un nouvel endpoint API | `configs/hubeau/` + `src/hubeau_pipeline/sources/hubeau_csv_source.py` + `assets/bronze/dlt_assets.py` |
| Ajouter un modèle dbt | `src/dbt_hubeau/models/<couche>/` + `schema.yml` dans le même dossier |
| Modifier un job Dagster | `src/hubeau_pipeline/jobs/` |
| Changer un schedule | `src/hubeau_pipeline/schedules.py` |
| Modifier la config Docker | `docker-compose.yml` + `docker/<service>/` |
| Ajouter une macro dbt | `src/dbt_hubeau/macros/` |
| Comprendre le routage des schémas | `src/dbt_hubeau/macros/generate_schema_name.sql` |
| Voir les tests de qualité | `src/dbt_hubeau/models/*/schema.yml` |

---

## 5. La couche Bronze : ingestion avec DLT

### Principe

La couche Bronze stocke les données **brutes**, telles qu'elles arrivent des APIs. Les colonnes sont généralement en `text` (pas de typage). La déduplication est assurée par DLT via une clé primaire et le mode `MERGE`.

### Comment fonctionne un asset DLT

Prenons l'exemple simplifié de `piezometry_chroniques_daily_raw` dans `dlt_assets.py` :

```python
@asset(group_name="bronze_piezometry")
def piezometry_chroniques_daily_raw(context, dlt: DagsterDltResource, pg: PostgreSQLResource):
    # 1. Charger la config YAML
    config = load_config("configs/hubeau/piezometry_chroniques.yml")

    # 2. Créer la source DLT (générateur Python qui yield des dicts)
    source = hubeau_chroniques_daily(config, lookback_days=7)

    # 3. Créer le pipeline DLT (destination = PostgreSQL, schéma = bronze)
    pipeline = dlt.pipeline(
        pipeline_name="piezometry_chroniques",
        destination="postgres",
        dataset_name="bronze",
    )

    # 4. Exécuter : DLT pagine l'API, déduplique, et écrit dans PG
    yield from pipeline.run(source, write_disposition="merge", primary_key=["code_bss", "date_mesure"])
```

**Ce que DLT fait automatiquement** :
- Crée la table si elle n'existe pas
- Infère le schéma depuis les données
- Pagine l'API (suit les liens `rel="next"`)
- Retente en cas d'erreur 503 (backoff exponentiel)
- Déduplique via `MERGE` sur la clé primaire

### Fichiers de configuration (`configs/hubeau/`)

Chaque endpoint a un fichier YAML qui définit l'URL, la taille de lot, les colonnes, etc. Exemple :

```yaml
# configs/hubeau/piezometry_chroniques.yml
base_url: "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes"
endpoint: "chroniques"
format: "csv"
batch_size: 20000
primary_key: ["code_bss", "date_mesure"]
```

### Client API Hub'Eau (`hubeau_csv_source.py`)

Ce fichier contient la logique de pagination et de retry :

- **Pagination** : suit le header HTTP `Link: <url>; rel="next"`
- **Retry** : 5 tentatives avec backoff exponentiel (2s → 4s → 8s → ..., max 120s)
- **Gestion d'erreurs** : retry automatique sur 408, 429, 500, 502, 503, 504
- **Parsing CSV** : détecte automatiquement le séparateur (`;` ou `,`)

### Tables Bronze principales

| Table | Source | Clé primaire | Volume approximatif |
|-------|--------|-------------|---------------------|
| `piezometry_stations_raw` | Hub'Eau | code_bss | ~1 900 lignes |
| `piezometry_chroniques_raw` | Hub'Eau | code_bss, date_mesure | ~23 M lignes |
| `hydrometry_sites_raw` | Hub'Eau | code_site | ~2 400 lignes |
| `hydrometry_stations_raw` | Hub'Eau | code_station | ~4 200 lignes |
| `hydrometry_obs_elab_raw` | Hub'Eau | code_station, date_obs_elab | ~15 M lignes |
| `era5_france_timeseries` | Copernicus | latitude, longitude, time | ~50 M lignes |
| `tme_entites_hydrogeo` | BDLISA | code_entite | ~45 000 lignes |

---

## 6. La couche Silver : staging avec dbt

### Principe

La couche Silver nettoie et type les données Bronze. Chaque modèle `stg_*` :
1. Sélectionne les colonnes utiles depuis la table Bronze (`{{ source('bronze', 'table') }}`)
2. Caste les types (`text` → `numeric`, `date`, `timestamp`) via les macros `cast_silver_*`
3. Déduplique avec `DISTINCT ON`
4. Filtre les lignes invalides (les rejects vont dans `silver_rejects`)

### Macros de casting (`cast_silver.sql`)

```sql
-- Convertit une colonne text en numeric, en gérant NULL, '', et la chaîne 'NULL'
{{ cast_silver_numeric('colonne_source', 'nom_cible') }}
-- Génère : NULLIF(NULLIF(TRIM(colonne_source), ''), 'NULL')::NUMERIC AS nom_cible
```

Macros disponibles : `cast_silver_numeric`, `cast_silver_int`, `cast_silver_date`, `cast_silver_timestamp`, `cast_silver_text`.

### Stratégie incrémentale

Les modèles de chroniques (piézo, hydro) utilisent `delete+insert` avec une fenêtre de lookback :

```sql
-- stg_piezo_chroniques.sql
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='code_bss || date_mesure'
  )
}}

SELECT ...
FROM {{ source('bronze', 'piezometry_chroniques_raw') }}
{% if is_incremental() %}
WHERE _dlt_load_id >= (
  SELECT MAX(_dlt_load_id) FROM {{ this }}
  WHERE date_mesure >= CURRENT_DATE - INTERVAL '{{ var("piezometry_incremental_lookback_days") }} days'
)
{% endif %}
```

### Tables de rejects

Les lignes qui ne passent pas les filtres de qualité sont isolées dans `silver_rejects` :

```sql
-- stg_piezo_chroniques_rejected.sql
-- Lignes avec date invalide, valeur hors plage, ou code_bss manquant
```

Cela permet de tracer les données exclues sans polluer les tables propres.

### Modèles Silver

| Modèle | Source Bronze | Stratégie | Notes |
|--------|-------------|-----------|-------|
| `stg_piezo_chroniques` | piezometry_chroniques_raw | Incrémental delete+insert (7j) | DISTINCT ON (code_bss, date_mesure) |
| `stg_piezo_stations` | piezometry_stations_raw | Table complète | Géométrie PostGIS |
| `stg_hydrometry_obs_elab` | hydrometry_obs_elab_raw | Incrémental delete+insert (7j) | |
| `stg_hydrometry_stations` | hydrometry_stations_raw | Table complète | |
| `stg_hydrometry_sites` | hydrometry_sites_raw | Table complète | |
| `stg_era5_timeseries` | era5_france_timeseries | Incrémental append + hypertable | Compression TimescaleDB |
| `stg_tme_entites` | tme_entites_hydrogeo | Table complète | Enrichissement géographique |

---

## 7. La couche Gold : analytique avec dbt

### Architecture en deux niveaux

```
Silver ──► intermediate/ (jointures, enrichissement) ──► marts/ (tables finales)
```

### Les intermédiaires (`intermediate/`)

Ces modèles préparent les données pour les marts :

| Modèle | Rôle |
|--------|------|
| `int_era5_grid_points` | Points de grille ERA5 uniques sur la France |
| `int_station_era5_mapping` | Jointure spatiale PostGIS : chaque station piézo → cellule ERA5 la plus proche (KNN) |
| `int_hydro_station_era5_mapping` | Idem pour les stations hydrométriques |
| `int_era5_for_stations` | Séries temporelles ERA5 filtrées pour les stations piézo |
| `int_era5_for_hydro_stations` | Idem pour les stations hydro |
| `int_daily_measurements` | Mesures piézo dédupliquées et validées |
| `int_hydro_daily_measurements` | Mesures hydro dédupliquées et validées |

**Jointure spatiale KNN** (dans `int_station_era5_mapping`) :
```sql
-- Trouve le point ERA5 le plus proche de chaque station
CROSS JOIN LATERAL (
    SELECT latitude, longitude
    FROM {{ ref('int_era5_grid_points') }} grid
    ORDER BY grid.geometry <-> station.geometry  -- opérateur KNN PostGIS
    LIMIT 1
) nearest
```

### Les marts (`marts/`)

#### Tables de faits quotidiennes (hypertables TimescaleDB)

| Table | Contenu | Granularité | Volume |
|-------|---------|------------|--------|
| `hubeau_daily_chroniques` | Niveau piézo + météo ERA5 + enrichissement TME | Jour × Station | ~23 M lignes |
| `hydro_daily_chroniques` | Débit hydro + météo ERA5 + enrichissement TME | Jour × Station | ~15 M lignes |

Ces tables utilisent le pattern **append + hypertable_delete** :
- **Pre-hook** : supprime les 30 derniers jours (avec décompression des chunks)
- **Modèle SQL** : réinsère les 30 derniers jours (enrichis)
- **Post-hooks** : crée la PK + convertit en hypertable + active la compression

#### Tables de faits agrégées

| Table | Granularité | Stratégie |
|-------|------------|-----------|
| `fct_monthly_chroniques` | Mois × Station (piézo) | delete+insert (25 mois) |
| `fct_monthly_hydro` | Mois × Station (hydro) | delete+insert (25 mois) |
| `fct_yearly_stats` | Année × Station (piézo) | delete+insert |
| `fct_yearly_hydro` | Année × Station (hydro) | delete+insert |

#### Dimensions

| Table | Contenu |
|-------|---------|
| `dim_date` | Dimension date (1900-2100) : jour, semaine, mois, trimestre, année, saison |
| `dim_geography` | Régions, départements, zones hydrologiques |
| `dim_piezo_stations` | Stations piézo enrichies (KPI, alertes, géographie) |
| `dim_hydro_stations` | Stations hydro enrichies |

#### Analyses et cartographie

| Table | Usage |
|-------|-------|
| `agg_station_trends` | Tendances piézo : pentes saisonnières, variations annuelles |
| `agg_hydro_trends` | Tendances hydro |
| `stations_piezo_carte` | Stations piézo avec dernier niveau (pour cartes Superset) |
| `stations_hydro_carte` | Stations hydro avec dernier débit (pour cartes Superset) |

### Tests de qualité dbt

Les tests sont définis dans les fichiers `schema.yml` à côté des modèles :

```yaml
# schema.yml
models:
  - name: hubeau_daily_chroniques
    columns:
      - name: code_bss
        tests:
          - not_null
      - name: date
        tests:
          - not_null
      - name: niveau_eau_ngf
        tests:
          - dbt_utils.accepted_range:
              min_value: -200
              max_value: 5000
```

Exécution : `docker exec brgm-dlt-worker dbt test`

---

## 8. Orchestration avec Dagster

### Concepts Dagster utilisés

| Concept | Ce que c'est | Où dans le code |
|---------|-------------|-----------------|
| **Asset** | Une donnée matérialisée (table) avec des dépendances | `assets/bronze/*.py`, `assets/dbt_assets.py` |
| **Job** | Un ensemble d'assets à exécuter ensemble | `jobs/*.py` |
| **Schedule** | Un déclencheur temporel (cron) | `schedules.py` |
| **Sensor** | Un déclencheur événementiel (observe un changement) | `sensors.py` |
| **Resource** | Une connexion partagée (DB, DLT, dbt) | `resources.py` |
| **IO Manager** | Gère le stockage des résultats d'assets | `io/io_managers.py` |

### Le fichier `definitions.py` : le point central

C'est ici que tout est assemblé :

```python
defs = Definitions(
    assets=all_assets,           # 14 assets Bronze + 1 asset dbt (31 modèles)
    jobs=all_jobs,               # 22 jobs
    schedules=all_schedules,     # 5 schedules (contrôlés par DAGSTER_ENABLE_SCHEDULES)
    sensors=all_sensors,         # 3 sensors (contrôlés par DAGSTER_ENABLE_SENSORS)
    resources={
        "pg": PostgreSQLResource(...),     # Connexion PostgreSQL
        "dlt": DagsterDltResource(),       # Intégration DLT
        "dbt": DbtCliResource(...),        # Intégration dbt
        "noop_io_manager": NoOpIOManager() # Pas de stockage Dagster (DLT écrit dans PG)
    }
)
```

### Les jobs importants

| Job | Usage | Quand l'utiliser |
|-----|-------|-----------------|
| `full_bootstrap_job` | Chargement initial complet | Premier démarrage uniquement |
| `piezometry_chroniques_daily_job` | Ingestion piézo quotidienne | Automatique via schedule |
| `hydrometry_obs_daily_job` | Ingestion hydro quotidienne | Automatique via schedule |
| `era5_weekly_update_job` | Mise à jour ERA5 | Automatique via schedule |
| `dbt_shared_staging_job` | Staging ERA5 + grid points | Déclenché par sensor |
| `dbt_piezo_pipeline_daily_job` | Pipeline piézo complet (Silver→Gold) | Déclenché par sensor |
| `dbt_hydro_pipeline_daily_job` | Pipeline hydro complet (Silver→Gold) | Déclenché par sensor |
| `dbt_shared_dimensions_job` | Dimensions communes (date, géo) | Déclenché par sensor |
| `dbt_full_pipeline_job` | Pipeline dbt complet (tous les modèles) | Manuel, debug |

### La chaîne de sensors

Les sensors forment une chaîne event-driven qui remplace les schedules pour dbt :

1. **`bronze_to_shared_staging_sensor`** : observe les assets Bronze chroniques → lance `dbt_shared_staging_job`
2. **`shared_staging_to_domain_sensor`** : observe le staging partagé → lance les pipelines piézo + hydro en parallèle
3. **`domain_to_dimensions_sensor`** : observe les deux pipelines domaine → lance `dbt_shared_dimensions_job`

Avantage : pas besoin de deviner combien de temps prend chaque étape. La suivante démarre dès que la précédente finit.

### Interface web Dagster

Accessible sur `http://localhost:49500` :
- **Assets** : graphe de dépendances, statut de matérialisation
- **Runs** : historique des exécutions, logs détaillés
- **Jobs** : lancement manuel, vue Launchpad
- **Schedules/Sensors** : activation/désactivation, historique des déclenchements

---

## 9. Infrastructure Docker

### Les 13 services

```
┌─────────────────────────────────────────────────────────────────────────┐
│ DATA                                                                    │
│  ┌──────────┐  ┌──────────────────┐  ┌───────┐                         │
│  │ postgres │  │ dagster_postgres  │  │ redis │                         │
│  │ (TimescaleDB │ (métadonnées    │  │ (cache│                         │
│  │  + PostGIS)  │  Dagster)       │  │  Sup.)│                         │
│  │  :49502   │  │  interne        │  │ int.  │                         │
│  └──────────┘  └──────────────────┘  └───────┘                         │
│                                                                         │
│ ORCHESTRATION                                                           │
│  ┌──────────────┐  ┌───────────────┐  ┌──────────────────┐             │
│  │ dlt_worker   │  │ dagster       │  │ dagster_daemon   │             │
│  │ (code-server)│  │ _webserver    │  │ (schedules,      │             │
│  │ GRPC :4000   │  │ UI :49500     │  │  sensors)        │             │
│  │ dbt :49505   │  │               │  │                  │             │
│  └──────────────┘  └───────────────┘  └──────────────────┘             │
│                                                                         │
│ OUTILS SQL                           BI                                 │
│  ┌──────────┐  ┌─────────────┐  ┌──────────┐                           │
│  │ adminer  │  │ cloudbeaver │  │ superset │                           │
│  │ :49501   │  │ :49503      │  │ :49504   │                           │
│  └──────────┘  └─────────────┘  └──────────┘                           │
│                                                                         │
│ MONITORING                                                              │
│  ┌────────────┐  ┌─────────┐  ┌──────────┐  ┌──────────────────┐      │
│  │ prometheus │  │ grafana │  │ cadvisor │  │ postgres_exporter│      │
│  │ :49508     │  │ :49507  │  │ interne  │  │ interne          │      │
│  └────────────┘  └─────────┘  └──────────┘  └──────────────────┘      │
└─────────────────────────────────────────────────────────────────────────┘
```

### Communication Worker ↔ Orchestrateur

Le worker et l'orchestrateur sont deux images Docker séparées qui communiquent via **gRPC** :

- **Worker** (`dlt_worker`) : contient tout le code Python (assets, jobs, DLT, dbt). Expose un code-server gRPC sur le port 4000.
- **Orchestrateur** (`dagster_webserver` + `dagster_daemon`) : ne contient PAS de code métier. Se connecte au worker via gRPC pour savoir quels assets/jobs existent et pour lancer les exécutions.

Configuration dans `docker/orchestrator/dagster_home/workspace.yaml` :
```yaml
load_from:
  - grpc_server:
      host: dlt_worker
      port: 4000
```

### Volumes Docker (externes)

Les volumes sont **externes** : ils ne sont PAS supprimés par `docker compose down -v`. Il faut les créer manuellement au premier lancement :

```bash
bash scripts/init_volumes.sh
# Crée : brgm_postgres_data, brgm_dagster_pg_data, brgm_cloudbeaver_data
```

### Variables d'environnement importantes

| Variable | Défaut | Rôle |
|----------|--------|------|
| `PG_PASSWORD` | (requis) | Mot de passe PostgreSQL principal |
| `PG_HOST` | postgres | Hostname PostgreSQL |
| `PG_PORT` | 5432 | Port PostgreSQL (interne) |
| `PG_DB` | postgres | Base de données |
| `PG_USER` | postgres | Utilisateur PostgreSQL |
| `DAGSTER_ENABLE_SCHEDULES` | false | Active/désactive les schedules |
| `DAGSTER_ENABLE_SENSORS` | false | Active/désactive les sensors |
| `SUPERSET_SECRET_KEY` | (requis) | Clé de chiffrement Superset |
| `SUPERSET_ADMIN_PASSWORD` | (requis) | Mot de passe admin Superset |
| `CDSAPI_KEY` | (optionnel) | Clé API Copernicus CDS pour ERA5 |

### Hot-reload

| Ce qui change | Que faire |
|---------------|-----------|
| Code Python (`src/hubeau_pipeline/`) | `docker compose restart dlt_worker` |
| Modèles dbt (`src/dbt_hubeau/models/`) | Recharger les définitions dans Dagster UI (bouton "Reload") |
| Configs YAML (`configs/`) | Rien, les fichiers sont montés en volume |
| Dépendances Python (`pyproject.toml`) | `docker compose build --no-cache dlt_worker && docker compose up -d` |

---

## 10. Démarrage du projet

### Prérequis

- **Docker** (avec Docker Compose v2)
- **Git**
- **~10 Go de RAM** disponibles (les conteneurs consomment ~8 Go au total)
- **~50 Go de disque** (données complètes : ~30 Go compressés)

### Installation pas à pas

```bash
# 1. Cloner le repo
git clone <repository-url>
cd hubeau_data_integration

# 2. Créer les volumes Docker (OBLIGATOIRE, une seule fois)
bash scripts/init_volumes.sh

# 3. Créer le fichier .env
cp .env.example .env
# Éditer .env avec vos mots de passe

# 4. Construire et lancer
docker compose up -d --build

# 5. Vérifier que tout est healthy (attendre ~60 secondes)
docker compose ps
# Tous les services doivent être "healthy" ou "running"

# 6. Accéder au Dagster UI
# Ouvrir http://localhost:49500
```

### Chargement initial des données

**Option A : Bootstrap complet** (recommandé, prend plusieurs heures)
1. Ouvrir Dagster UI → Jobs → `full_bootstrap_job` → Launchpad → Launch Run
2. Ce job charge : référentiels → stations → chroniques (par année) → ERA5 → dbt

**Option B : Chargement progressif** (pour tester rapidement)
1. Lancer `reference_data_bronze_job` (données TME/SANDRE)
2. Lancer `all_stations_job` (métadonnées stations)
3. Lancer un job de chroniques pour une année récente
4. Lancer `dbt_full_pipeline_job` pour les transformations

### Vérification

```bash
# Compter les lignes par schéma
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
SELECT schemaname, tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY schemaname, n_live_tup DESC;"

# Lancer les tests dbt
docker exec brgm-dlt-worker dbt test
```

---

## 11. Opérations courantes

### Commandes dbt

```bash
# Exécuter tous les modèles
docker exec brgm-dlt-worker dbt run

# Exécuter un modèle spécifique
docker exec brgm-dlt-worker dbt run --select hubeau_daily_chroniques

# Exécuter un modèle + ses descendants
docker exec brgm-dlt-worker dbt run --select stg_piezo_chroniques+

# Forcer la reconstruction complète d'un modèle incrémental
docker exec brgm-dlt-worker dbt run --full-refresh --select hubeau_daily_chroniques

# Lancer les tests
docker exec brgm-dlt-worker dbt test

# Tester un seul modèle
docker exec brgm-dlt-worker dbt test --select hubeau_daily_chroniques

# Vérifier la fraîcheur des sources
docker exec brgm-dlt-worker dbt source freshness

# Générer la documentation
docker exec brgm-dlt-worker dbt docs generate

# Recalculer le mapping stations-ERA5 (après changement TME)
docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ \
  --vars '{"recompute_station_era5_mapping": true}'
```

### Commandes Docker

```bash
# Voir les logs d'un service
docker compose logs -f dlt_worker

# Redémarrer le worker (après modification Python)
docker compose restart dlt_worker

# Rebuild complet (après modification dépendances)
docker compose down && docker compose build --no-cache && docker compose up -d

# Accéder au shell du worker
docker exec -it brgm-dlt-worker bash

# Accéder à PostgreSQL
docker exec -it brgm-postgres psql -U postgres -d postgres
```

### Commandes PostgreSQL utiles

```sql
-- Lister les tables par schéma
\dt bronze.*
\dt silver.*
\dt gold.*

-- Voir les hypertables
SELECT hypertable_name, num_chunks, compression_enabled
FROM timescaledb_information.hypertables;

-- Voir la compression
SELECT hypertable_name,
       pg_size_pretty(before_compression_total_bytes) as avant,
       pg_size_pretty(after_compression_total_bytes) as apres,
       round((1 - after_compression_total_bytes::numeric / before_compression_total_bytes) * 100, 1) as ratio_pct
FROM timescaledb_information.compression_settings cs
JOIN hypertable_compression_stats(cs.hypertable_schema || '.' || cs.hypertable_name) ON true;

-- Voir les tailles des tables
SELECT schemaname, tablename,
       pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as taille
FROM pg_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;
```

### Linting Python

```bash
ruff check src/               # Lint
ruff check --fix src/          # Lint + auto-fix
black src/                     # Formatage
mypy src/                      # Vérification de types
```

---

## 12. Dépannage

### Le worker ne démarre pas

```bash
docker compose logs dlt_worker
# Causes fréquentes : conflit de port, image corrompue
docker compose build --no-cache dlt_worker
docker compose up -d dlt_worker
```

### Hub'Eau API retourne 503

L'API est surchargée. Attendre 15-30 min et relancer le job depuis Dagster UI. Le retry automatique (5 tentatives, backoff) gère la plupart des cas.

### ERA5 timeout

Vérifier le statut du service CDS : https://cds.climate.copernicus.eu/. Le retry est intégré.

### Erreur "Module not found" après mise à jour

```bash
docker compose build --no-cache dlt_worker
docker compose up -d
```

### Données manquantes dans Gold

1. Vérifier que les données Bronze existent : `\dt bronze.*` + comptage
2. Vérifier que Silver s'est exécuté : `docker exec brgm-dlt-worker dbt run --select stg_piezo_chroniques`
3. Vérifier le mapping ERA5 : `docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ --vars '{"recompute_station_era5_mapping": true}'`

### Labels TME manquants

Reconstruire le mapping : `docker exec brgm-dlt-worker dbt run --select int_station_era5_mapping+ --vars '{"recompute_station_era5_mapping": true}'`

### Tests dbt en échec

```bash
# Identifier le test qui échoue
docker exec brgm-dlt-worker dbt test --select model_name

# Voir les lignes en erreur
docker exec brgm-dlt-worker dbt test --select model_name --store-failures
# Les résultats sont dans le schéma dbt_test__audit
```

---

## 13. Glossaire

| Terme | Signification |
|-------|--------------|
| **Asset** | Dans Dagster : une donnée matérialisée (table, fichier) avec un DAG de dépendances |
| **BDLISA** | Base de Données des Limites des Systèmes Aquifères (référentiel hydrogéologique français) |
| **Bronze** | Couche de données brutes (raw) |
| **Chunk** | Dans TimescaleDB : partition temporelle d'une hypertable |
| **Chronique** | Série temporelle de mesures (niveau d'eau ou débit) |
| **CDS** | Climate Data Store (Copernicus) — API pour télécharger les données ERA5 |
| **code_bss** | Identifiant unique BSS (Banque du Sous-Sol) d'un point d'eau |
| **code_station** | Identifiant unique d'une station hydrométrique |
| **DAG** | Directed Acyclic Graph — graphe de dépendances entre modèles dbt |
| **dbt** | Data Build Tool — outil de transformation SQL |
| **DLT** | Data Load Tool — bibliothèque Python d'ingestion |
| **ERA5** | European ReAnalysis 5th generation — données climatiques haute résolution |
| **Full-refresh** | Reconstruction complète d'un modèle dbt (supprime et recrée la table) |
| **Gold** | Couche de données analytiques finales |
| **gRPC** | Protocole de communication binaire (entre worker et orchestrateur Dagster) |
| **Hub'Eau** | Plateforme d'accès aux données hydrologiques françaises (eaufrance) |
| **Hydrométrie** | Mesure des débits des cours d'eau |
| **Hypertable** | Table TimescaleDB partitionnée automatiquement par le temps |
| **Incrémental** | Stratégie dbt qui ne traite que les nouvelles données (pas tout recalculer) |
| **Job** | Dans Dagster : un ensemble d'assets à exécuter |
| **KNN** | K-Nearest Neighbors — algorithme du plus proche voisin (jointure spatiale) |
| **Lookback** | Fenêtre temporelle de recalcul (ex: 7 jours) |
| **Manifest** | Fichier JSON généré par dbt décrivant tous les modèles et leurs dépendances |
| **Mart** | Table finale destinée aux utilisateurs métier (reportings, dashboards) |
| **Medallion** | Architecture en couches (Bronze → Silver → Gold) |
| **MERGE** | Stratégie DLT : insert ou update selon la clé primaire |
| **NetCDF** | Format de fichier pour données climatiques multidimensionnelles |
| **Patroni** | Outil de haute disponibilité PostgreSQL (utilisé par l'image TimescaleDB) |
| **Piézométrie** | Mesure des niveaux d'eau dans les nappes souterraines |
| **PostGIS** | Extension PostgreSQL pour les données géospatiales |
| **QmnJ** | Débit moyen journalier (en l/s) |
| **SANDRE** | Service d'Administration Nationale des Données et Référentiels sur l'Eau |
| **Schedule** | Planification cron dans Dagster |
| **Sensor** | Déclencheur événementiel dans Dagster |
| **Silver** | Couche de données nettoyées et typées |
| **Staging** | Synonyme de Silver dans ce projet |
| **TimescaleDB** | Extension PostgreSQL pour les séries temporelles |
| **TME** | Table de correspondance des Masses d'Eau (entités hydrogéologiques) |

---

## Ressources complémentaires

- **Documentation dbt** : http://localhost:49505 (après `docker exec brgm-dlt-worker dbt docs serve --port 8080`)
- **Dagster UI** : http://localhost:49500
- **Grafana** : http://localhost:49507 (admin/admin)
- **Superset** : http://localhost:49504
- **CLAUDE.md** : Guide de développement détaillé (conventions, commandes avancées)
- **docs/CONFIGURATION.md** : Variables d'environnement et paramétrage
- **docs/SCHEMA_BDD.md** : Schéma complet des tables
- **docs/OPERATIONS.md** : Runbook, dépannage, sauvegarde et restauration
- **docs/MONITORING.md** : Configuration du monitoring
- **docs/SUPERSET.md** : Configuration BI et dashboards
- **docs/ERA5.md** : Architecture d'ingestion des données climatiques
- **docs/TIMESCALEDB.md** : Hypertables, compression, indexation
