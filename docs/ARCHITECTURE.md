# Architecture Hub'Eau Data Pipeline

## Vue d'ensemble

Le projet Hub'Eau Data Pipeline est une solution complète d'ingestion et de traitement des données hydrologiques françaises. Il collecte automatiquement les données depuis les APIs Hub'Eau et les stocke dans une base PostgreSQL structurée.

## Architecture technique

```
┌─────────────────────────────────────────────────────────────┐
│                         GitLab CI/CD                        │
│              (Déploiement automatique sur push)             │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                    VPS Production                           │
│  ┌────────────────────────────────────────────────────┐    │
│  │                  Dagster Orchestrator               │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐    │    │
│  │  │ Webserver├──►│  Daemon  ├──►│   Scheduler  │    │    │
│  │  └──────────┘  └──────────┘  └──────────────┘    │    │
│  └────────────────────┬───────────────────────────────┘    │
│                       │ gRPC                                │
│  ┌────────────────────▼───────────────────────────────┐    │
│  │                   DLT Worker                        │    │
│  │         (Ingestion depuis APIs Hub'Eau)            │    │
│  └────────────────────┬───────────────────────────────┘    │
│                       │                                     │
│  ┌────────────────────▼───────────────────────────────┐    │
│  │                PostgreSQL Database                  │    │
│  │              Schema: hubeau (tables)               │    │
│  └─────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

## Composants principaux

### 1. Orchestration - Dagster

**Rôle**: Planification et exécution des pipelines de données

**Composants**:
- **Webserver**: Interface utilisateur (port 8080)
- **Daemon**: Exécution des jobs et sensors
- **Scheduler**: Planification des runs
- **PostgreSQL Dagster**: Métadonnées Dagster

**Assets définis**:
- Assets de référence (stations, sites, ouvrages)
- Assets de données temporelles (chroniques, mesures)
- Partitionnement par année (2020-2024)

### 2. Ingestion - DLT (Data Load Tool)

**Rôle**: Extraction et chargement des données depuis les APIs Hub'Eau

**Caractéristiques**:
- Ingestion directe dans PostgreSQL
- Tables pré-créées via script SQL d'initialisation
- Support du mode merge (upsert) avec clés primaires
- Retry automatique en cas d'erreur
- Rate limiting pour respecter les quotas API

**Sources configurées**:
- Piézométrie (nappes phréatiques)
- Hydrométrie (cours d'eau)
- Qualité des eaux (rivières et nappes)
- Prélèvements
- Écoulements
- Température
- Hydrobiologie

### 3. Stockage - PostgreSQL

**Rôle**: Base de données principale

**Structure**:
- Schema `hubeau`: Toutes les données Hub'Eau
- Tables pré-créées via script SQL d'initialisation
- Index optimisés pour requêtes temporelles et géographiques
- Support PostGIS pour données géospatiales
- Triggers pour mise à jour automatique des timestamps

### 4. Déploiement - Docker & GitLab CI/CD

**Conteneurs Docker**:
```yaml
services:
  dagster_postgres    # Base Dagster (métadonnées)
  postgres           # Base données Hub'Eau
  dlt_worker         # Worker DLT
  dagster_webserver  # UI Dagster
  dagster_daemon     # Daemon Dagster
  adminer           # Interface DB
```

**Pipeline CI/CD**:
1. Push sur branche `main`
2. Build des images Docker
3. Déploiement automatique sur VPS
4. Restart des services

## Structure de la base de données

### Schema `hubeau`

Le schema est créé automatiquement au démarrage via `/docker/init-scripts/postgres/01_create_schema.sql`.

**Tables principales**:

#### Tables de référence (stations, sites)
- `piezometry_stations` - Stations piézométriques
- `hydrometry_sites` - Sites hydrométriques
- `hydrometry_stations` - Stations hydrométriques
- `quality_rivers_stations` - Stations qualité rivières
- `quality_groundwater_stations` - Stations qualité nappes
- `temperature_stations` - Stations température
- `ecoulement_stations` - Stations écoulement
- `hydrobio_stations` - Stations hydrobiologie
- `prelevements_ouvrages` - Ouvrages de prélèvement

#### Tables de données temporelles
- `piezometry_chroniques` - Mesures piézométriques
- `hydrometry_observations` - Observations hydrométriques
- `quality_rivers_analyses` - Analyses qualité rivières
- `quality_groundwater_analyses` - Analyses qualité nappes
- `temperature_chroniques` - Mesures température
- `ecoulement_campagnes` - Campagnes écoulement
- `hydrobio_indices` - Indices biologiques
- `prelevements_chroniques` - Volumes prélevés

#### Tables système DLT
- `_dlt_loads` - Historique des chargements
- `_dlt_pipeline_state` - État des pipelines
- `_dlt_version` - Versions des schémas

### Index et optimisations

**Index temporels**:
- Sur toutes les colonnes de date (`date_mesure`, `date_obs`, `date_prelevement`)
- Sur les colonnes `year` (générées automatiquement)

**Index géographiques**:
- Sur `code_commune_insee`, `code_departement`
- Index spatiaux PostGIS disponibles (commentés par défaut)

**Triggers**:
- `updated_at` mis à jour automatiquement sur modification

## Flux de données

### 1. Ingestion initiale
```
API Hub'Eau → DLT → PostgreSQL (tables stations/référentiels)
```

### 2. Mise à jour des chroniques
```
Sensor Dagster → Job partitionné → DLT → PostgreSQL (données temporelles)
```

### 3. Backfill automatique
```
Sensor détection partitions manquantes → Jobs backfill → DLT → PostgreSQL
```

Note: Le sensor de backfill ne se déclenche pas sur une nouvelle installation (protection intégrée).

## Configuration

### Variables d'environnement principales

**PostgreSQL données**:
- `PG_HOST`: Hôte PostgreSQL (défaut: postgres)
- `PG_PASSWORD`: Mot de passe (depuis GitLab CI/CD)
- `PG_DB`: Base de données (défaut: postgres)
- `PG_USER`: Utilisateur (défaut: postgres)
- `HUBEAU_SCHEMA`: Schema (défaut: hubeau)

**Dagster**:
- `DAGSTER_PG_HOST`: Hôte PostgreSQL Dagster
- `DAGSTER_PG_PASSWORD`: Mot de passe Dagster

**DLT**:
- `DESTINATION__POSTGRES__CREDENTIALS__HOST`: postgres
- `DESTINATION__POSTGRES__CREDENTIALS__DATABASE`: postgres
- `DESTINATION__POSTGRES__CREDENTIALS__USERNAME`: postgres
- `DESTINATION__POSTGRES__CREDENTIALS__PASSWORD`: ${PG_PASSWORD}

### Configuration des sources

Les sources sont configurées via fichiers YAML dans `configs/hubeau/`:
- Configuration des endpoints API
- Paramètres de pagination
- Mapping des champs
- Filtres temporels et géographiques

## Jobs et Sensors Dagster

### Jobs principaux

- **`sync_all_yearly_data`**: Synchronise toutes les données pour une année
- **`hubeau_piezometry_job`**: Données piézométriques
- **`hubeau_hydrometry_job`**: Données hydrométriques
- **`hubeau_quality_job`**: Qualité des eaux
- **`hubeau_temperature_job`**: Température des cours d'eau

### Sensors

- **`backfill_missing_partitions_sensor`**:
  - Détecte les partitions manquantes
  - Ne se déclenche pas sur nouvelle installation
  - Limite à 3 backfills par exécution
  - Variable `FORCE_INITIAL_BACKFILL=true` pour forcer

## Optimisations

### Performance
- Pipelines DLT isolés par asset pour éviter conflits de schéma
- Utilisation de `/tmp` pour fichiers temporaires DLT
- Rate limiting pour respecter quotas API
- Index PostgreSQL sur colonnes clés
- Write disposition "merge" pour upserts efficaces

### Résilience
- Retry automatique sur erreurs API
- Health checks Docker
- Backfill sensor pour rattraper données manquantes
- Logs centralisés dans Dagster
- État DLT persisté dans PostgreSQL

### Monitoring
- Dagster UI pour suivi des runs
- Adminer pour inspection base de données
- Logs structurés avec niveaux (INFO, WARNING, ERROR)
- Métriques d'exécution dans Dagster

## Sécurité

- Mots de passe stockés dans GitLab CI/CD Variables
- Pas de secrets dans le code ou fichiers de config
- Connexions PostgreSQL via réseau Docker interne
- Ports exposés uniquement si nécessaire
- `.dlt/*.toml` dans `.gitignore` pour éviter fuite de credentials

## Maintenance

### Logs
- **Dagster UI**: http://localhost:8080 - Tous les logs d'exécution
- **Docker logs**: `docker logs <container_name>`
- **PostgreSQL logs**: Dans le container PostgreSQL

### Backup
- Volume Docker PostgreSQL persistant: `/srv/brgm-data/postgres`
- État Dagster: `/srv/brgm-data/dagster_pg`
- Possibilité de backup via `pg_dump`

### Mise à jour
1. Push sur `main` déclenche déploiement automatique
2. GitLab CI/CD build les nouvelles images
3. Déploiement sur VPS avec health checks
4. Rollback possible via GitLab

## Troubleshooting

### Problème: DLT crée trop de tables enfants
**Solution**: Les tables sont pré-créées via script SQL, DLT utilise la structure existante

### Problème: "could not translate host name postgres"
**Solution**: Vérifier que le service `postgres` est bien défini dans docker-compose

### Problème: Backfill automatique non désiré
**Solution**: Le sensor détecte automatiquement les nouvelles installations et ne backfill pas

### Problème: Données non mises à jour
**Vérifier**:
1. Dagster UI pour les logs d'erreur
2. Connexion PostgreSQL fonctionne
3. API Hub'Eau accessible
4. Credentials corrects dans GitLab Variables