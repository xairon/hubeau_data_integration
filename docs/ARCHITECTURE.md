# Architecture Hub'Eau Pipeline

## Vue d'Ensemble

Pipeline simple d'ingestion des données Hub'Eau vers PostgreSQL avec orchestration Dagster.

```
┌─────────────────────────────────────────────┐
│           APIs Hub'Eau (8 APIs)             │
│  Piézométrie | Hydrométrie | Qualité | ...  │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│              DLT (Ingestion)                │
│  - Format CSV                               │
│  - Déduplication (MERGE)                    │
│  - Retry automatique                        │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│           PostgreSQL Database               │
│  Schema: hubeau                             │
│  - Tables stations (9)                      │
│  - Tables chroniques (11)                   │
│  - Métadonnées DLT (3)                      │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│        Dagster (Orchestration)              │
│  - UI Web (port 8080)                       │
│  - Jobs par API                             │
│  - Sensors monitoring                       │
│  - Schedules automatiques                   │
└─────────────────────────────────────────────┘
```

## Composants

### 1. DLT - Ingestion

**Rôle** : Extraction Hub'Eau → Chargement PostgreSQL

**Fonctionnalités** :
- Format CSV direct
- Déduplication automatique (MERGE/UPSERT)
- Retry sur erreur API
- Rate limiting
- 3 modes : FULL / YEAR / INCREMENTAL

**Configuration** : 28 fichiers YAML dans `configs/hubeau/`

### 2. PostgreSQL - Stockage

**Rôle** : Base de données unique

**Structure** :
- Schema `hubeau` avec toutes les données
- Tables pré-créées via script SQL
- Index optimisés (temporels, géographiques)
- Métadonnées DLT (_dlt_loads, _dlt_pipeline_state)

**Initialisation** : Script `/docker/init-scripts/postgres/01_create_schema.sql`

### 3. Dagster - Orchestration

**Rôle** : Planification et monitoring

**Composants** :
- Webserver UI (http://localhost:8080)
- Daemon (exécution jobs/sensors)
- Jobs par API (11 jobs)
- Sensors (alertes, monitoring)

**Base métadonnées** : PostgreSQL Dagster (séparée)

### 4. Docker - Déploiement

**Services** :
```yaml
- postgres           # Base données Hub'Eau
- dagster_postgres   # Base métadonnées Dagster
- dagster_webserver  # UI Dagster
- dagster_daemon     # Daemon Dagster
- worker             # Worker DLT
```

## Flux de Données

### Ingestion Standard

```
1. Dagster Job déclenché (manuel ou automatique)
2. DLT récupère données depuis Hub'Eau API
3. Transformation CSV
4. Chargement PostgreSQL (MERGE)
5. Dagster log résultats
```

### Modes d'Ingestion

Voir [MODES_INGESTION.md](MODES_INGESTION.md)

## Configuration

### Variables d'Environnement

Voir [CONFIGURATION.md](CONFIGURATION.md)

### Configurations YAML

28 fichiers dans `configs/hubeau/` :
- 1 fichier = 1 endpoint API
- Configuration endpoint, pagination, filtres
- Clés primaires pour déduplication

Exemple `piezometry_chroniques.yml` :
```yaml
name: piezometry_chroniques
base_url: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes
path: /chroniques
primary_keys: [code_bss, date_mesure]
replication_key: date_mesure
```

## Jobs Dagster

Un job par API :
- `piezometry_job`
- `quality_rivers_job`
- `hydrometry_job`
- `temperature_job`
- `ecoulement_job`
- `hydrobio_job`
- `prelevements_job`
- `quality_groundwater_job`

Jobs globaux :
- `all_stations_job` (9 assets stations)
- `all_chroniques_job` (11 assets chroniques)
- `all_hubeau_job` (tous assets)

## Sensors

**Monitoring** :
- `error_detection_sensor` : Détection erreurs
- `pipeline_failure_alert_sensor` : Alertes échecs
- `long_running_pipeline_sensor` : Pipelines lents
- `repeated_failure_sensor` : Échecs répétés

## Monitoring

**Dagster UI** :
- Vue d'ensemble des runs
- Logs détaillés par asset
- Métriques d'exécution
- Graphe de dépendances

**PostgreSQL** :
- Adminer (http://localhost:8081)
- Requêtes SQL directes
- Volumétrie tables

## Déploiement

### Local (Docker Compose)

```bash
docker-compose up -d
```

Services démarrés :
- PostgreSQL données (port 5432)
- PostgreSQL Dagster (interne)
- Dagster Webserver (port 8080)
- Dagster Daemon
- Worker DLT
- Adminer (port 8081)

### Production (GitLab CI/CD)

Push sur `main` → déploiement automatique VPS

Pipeline :
1. Build images Docker
2. Deploy sur VPS
3. Health checks
4. Rollback si échec

Voir [.gitlab-ci.yml](../.gitlab-ci.yml)

## Optimisations

### Performance
- Exécution séquentielle (1 asset à la fois) pour éviter OOM
- Index PostgreSQL optimisés
- Rate limiting API Hub'Eau
- Batch size adaptatif

### Résilience
- Retry automatique sur erreurs
- Health checks Docker
- Logs structurés
- État DLT persisté

## Sécurité

- Mots de passe dans GitLab CI/CD Variables (masked)
- Pas de secrets dans le code
- Réseau Docker interne
- `.dlt/*.toml` gitignored

## Création Automatique de Tables

### PostgresBulkDestinationV2

DLT créera automatiquement les tables lors du premier run :

1. **Pandas infère les types** depuis les CSV Hub'Eau
2. **Table créée** avec colonnes TEXT (ultra-safe)
3. **COPY bulk** des données (100k records en 1-2s)
4. **Auto-fix** si erreur de type (ALTER COLUMN → TEXT)

**Localisation** : `src/hubeau_pipeline/destinations/postgres_optimized_v2.py:282`

**Stratégie ULTRA-SAFE** :
- Tout est créé en TEXT sauf datetime évident
- Zéro erreur COPY (text accepte tout)
- Pas de retries multiples
- Performance optimale

**Documentation détaillée** : Voir [AUTO_SCHEMA_CREATION.md](AUTO_SCHEMA_CREATION.md)

### Gestion Base Existante

✅ **Message PostgreSQL normal** : "_Database directory appears to contain a database; Skipping initialization_"

**Explication** :
- PostgreSQL détecte que `/var/lib/postgresql/data` existe déjà
- Les scripts d'init (`01_init_minimal.sql`) sont skip
- **Ce n'est PAS une erreur !** C'est le comportement standard.

✅ **Comportement attendu** :
- Si schéma `hubeau` existe → On l'utilise directement
- Si tables existent → MERGE/UPSERT des nouvelles données
- Si tables n'existent pas → Création automatique au premier run

❌ **Seul cas problématique** :
- Schéma `hubeau` n'existe pas → Erreur DLT
- **Solution** : Exécuter manuellement `01_init_minimal.sql` ou reset PostgreSQL

**Vérification santé base** :
```bash
# Accéder PostgreSQL
docker exec -it brgm-postgres psql -U postgres -d postgres

# Vérifier schéma hubeau
\dn hubeau

# Lister tables (si données déjà chargées)
\dt hubeau.*

# Si aucune table: NORMAL! Tables créées au premier run d'asset
```

## Troubleshooting

### "Database directory appears to contain a database"

✅ **Ce n'est PAS une erreur !**

Ce message PostgreSQL est **normal** et signifie que la base existe déjà. PostgreSQL skip l'init, c'est attendu.

**Actions** :
- Si les conteneurs démarrent → Tout va bien, ignorer le message
- Si le worker `brgm-dlt-worker` est unhealthy → Vérifier les logs

### Container `brgm-dlt-worker` unhealthy

**Causes possibles** :
1. Port 4000 déjà utilisé
2. Erreur Python au démarrage
3. PostgreSQL pas accessible
4. Dagster module non trouvé

**Diagnostic** :
```bash
docker compose logs dlt_worker
docker compose ps
```

**Solution** :
```bash
docker compose down
docker compose up -d
```

### Erreur : "could not translate host name postgres"
→ Vérifier service `postgres` dans docker-compose

### Erreur : DLT connection failed
→ Vérifier variables `PG_HOST`, `PG_PASSWORD` dans `.env`

### Erreur : Dagster UI inaccessible
→ Vérifier `dagster_webserver` container status

### Reset complet de la base

```bash
# Supprimer volumes Docker (ATTENTION: perte données)
docker compose down -v

# Recréation complète
docker compose up -d
```

### Logs
```bash
# Daemon Dagster
docker compose logs -f dagster_daemon

# Worker DLT
docker compose logs -f dlt_worker

# PostgreSQL Hub'Eau
docker compose logs -f postgres

# PostgreSQL Dagster
docker compose logs -f dagster_postgres

# Tous les services
docker compose logs -f
```

### Vérification santé services

```bash
# Script automatique
./scripts/check_services.sh

# Manuel
docker compose ps
```
