# Plan d'implémentation - Bronze Layer Architecture

**Date:** 2025-01-04
**Objectif:** Refactoriser le pipeline Hub'Eau pour utiliser une architecture Bronze/Silver avec DLT standard
**Status:** Partiellement complété (3/7 étapes)

---

## 🎯 Vision globale

### Architecture Bronze/Silver

```
API Hub'Eau → DLT Source (parallelized=True) → DLT Postgres (standard) → Tables _raw (Bronze)
                                                                                    ↓
                                                                            dbt (Silver - futur)
```

**Bronze Layer (_raw tables):**
- Pas de PK/FK contraintes
- Tous les duplicats conservés (API retourne ~50% duplicats pour certaines stations)
- Tables suffixées `_raw`
- Colonne `_ingested_at` pour audit
- Destination: DLT `postgres` standard (pas custom)

**Silver Layer (futur - dbt):**
- Déduplication
- Application PK/FK
- Qualité données
- Vues/tables finales

---

## ✅ Étapes complétées (3/7)

### 1. ✅ Configuration DLT workers

**Fichier créé:** `.dlt/config.toml`

```toml
[extract]
workers = 5  # Parallelization for HTTP fetching

[normalize]
workers = 3  # Process pool for normalization

[load]
workers = 20  # Thread pool for loading
```

**Impact:** DLT utilisera `parallelized=True` avec 5 workers pour fetch parallèle.

---

### 2. ✅ DB Helpers

**Fichier créé:** `src/hubeau_pipeline/utils/db_helpers.py`

**Fonctions:**
- `get_connection()` - Connection PostgreSQL via env vars
- `delete_year_data(table, year, date_column)` - DELETE year avant re-run (idempotence)
- `get_max_date(table, date_column)` - Query MAX(date) pour incrémental
- `table_exists(table)` - Check existence table

**Usage:**
```python
from hubeau_pipeline.utils.db_helpers import delete_year_data

# Avant de charger 2024
deleted = delete_year_data("temperature_chroniques_raw", "2024", "date_mesure_temp")
# → DELETE FROM hubeau.temperature_chroniques_raw WHERE EXTRACT(YEAR FROM date_mesure_temp) = 2024
```

---

### 3. ✅ Source DLT simplifiée

**Fichier réécrit:** `src/hubeau_pipeline/sources/hubeau_csv_source.py`

**Changements:**
- **AVANT:** 637 lignes, ThreadPoolExecutor custom, FK filtering, complexe
- **APRÈS:** 320 lignes (-50%), DLT `parallelized=True`, simple

**3 ressources DLT:**

#### a) `hubeau_stations()` - FULL mode
```python
@dlt.resource(parallelized=True, write_disposition="replace")
def hubeau_stations(config: Dict[str, Any]) -> Iterator[List[Dict]]:
    """
    Stations - FULL load (TRUNCATE + INSERT)
    Usage: temperature_stations, piezometry_stations, etc.
    """
    # Fetch all pages
    # DLT parallelize avec 5 workers (config.toml)
```

#### b) `hubeau_chroniques_year()` - YEAR partition
```python
@dlt.resource(parallelized=True, write_disposition="append")
def hubeau_chroniques_year(config: Dict, year: str) -> Iterator[List[Dict]]:
    """
    Chroniques - Year partition (INSERT only)
    Usage: Backfill historique, re-process année
    """
    # date_debut = "{year}-01-01"
    # date_fin = "{year}-12-31"
    # Fetch year data
```

#### c) `hubeau_chroniques_incremental()` - Incremental
```python
@dlt.resource(parallelized=True, write_disposition="append")
def hubeau_chroniques_incremental(
    config: Dict,
    last_date: dlt.sources.incremental = None
) -> Iterator[List[Dict]]:
    """
    Chroniques - Incremental (depuis dernière date)
    Usage: Daily/hourly runs production
    DLT track automatiquement MAX(date)
    """
    # Si table vide: charge année précédente
    # Sinon: charge depuis last_date.last_value
```

**Ce qui a été RETIRÉ:**
- ❌ ThreadPoolExecutor custom (remplacé par DLT `parallelized=True`)
- ❌ FK filtering logic (Bronze = charge tout)
- ❌ Parent key loading (pas de FK en Bronze)

**Ce qui a été GARDÉ:**
- ✅ Pagination custom (nécessaire pour Hub'Eau API)
- ✅ Date filters (`date_debut_mesure`, `date_fin_mesure`)
- ✅ Rate limiting (0.3s entre requêtes)
- ✅ Retry strategy (5 retries, backoff)

---

## ⏳ Étapes à compléter (4/7)

### 4. ⏳ Créer 20 schemas SQL _raw manquants

**Schemas déjà créés (2/22):**
- ✅ `scripts/schema/temperature_chroniques_raw.sql`
- ✅ `scripts/schema/temperature_stations_raw.sql`

**Schemas à créer (20/22):**

#### Tables "stations" (FULL mode):
1. `piezometry_stations_raw.sql`
2. `hydrometry_sites_raw.sql`
3. `hydrometry_stations_raw.sql`
4. `hydrobio_stations_raw.sql`
5. `quality_rivers_stations_raw.sql`
6. `quality_groundwater_stations_raw.sql`
7. `ecoulement_stations_raw.sql`

#### Tables "chroniques/observations" (YEAR + INCREMENTAL):
8. `piezometry_chroniques_raw.sql`
9. `hydrometry_obs_elab_raw.sql`
10. `hydrobio_indices_raw.sql`
11. `hydrobio_taxons_raw.sql`
12. `quality_rivers_analyses_raw.sql`
13. `quality_rivers_conditions_raw.sql`
14. `quality_rivers_operations_raw.sql`
15. `quality_groundwater_analyses_raw.sql`
16. `ecoulement_campagnes_raw.sql`
17. `ecoulement_observations_raw.sql`
18. `prelevements_chroniques_raw.sql`
19. `prelevements_ouvrages_raw.sql`
20. `prelevements_points_raw.sql`

**Template à suivre:**

```sql
-- Table: {table_name}_raw (Bronze Layer)
-- Source: Hub'Eau API - RAW data (no PK/FK, duplicates allowed)
-- Purpose: Ingest all data as-is from API, transformations in Silver layer (dbt)

CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.{table_name}_raw (
    -- Copier TOUTES les colonnes du schema original
    -- RETIRER les "NOT NULL"
    -- RETIRER la ligne "PRIMARY KEY (...)"

    -- Ajouter colonne audit
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index sur colonne date (pour incremental + performance)
CREATE INDEX IF NOT EXISTS idx_{table_name}_raw_{date_column}
ON hubeau.{table_name}_raw({date_column});

-- Index sur code_station si existe (pour queries)
CREATE INDEX IF NOT EXISTS idx_{table_name}_raw_code_station
ON hubeau.{table_name}_raw(code_station);

COMMENT ON TABLE hubeau.{table_name}_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
```

**Comment créer:**

1. Pour chaque table, lire le schema original (`scripts/schema/{table}.sql`)
2. Copier structure dans nouveau fichier `scripts/schema/{table}_raw.sql`
3. Appliquer transformations:
   - Retirer `NOT NULL` de toutes les colonnes
   - Retirer ligne `PRIMARY KEY (...)`
   - Retirer `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`
   - Ajouter `_ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
   - Ajouter index sur date_column
   - Ajouter index sur code_station (si existe)

**Exemple complet (temperature_chroniques_raw.sql déjà fait):**

```sql
-- Table: temperature_chroniques_raw (Bronze Layer)
CREATE SCHEMA IF NOT EXISTS hubeau;

CREATE TABLE IF NOT EXISTS hubeau.temperature_chroniques_raw (
    code_commune TEXT,
    code_cours_eau TEXT,
    code_parametre BIGINT,
    code_qualification TEXT,
    code_station BIGINT,  -- ← Retiré NOT NULL
    code_unite TEXT,
    date_mesure_temp TIMESTAMP,  -- ← Retiré NOT NULL
    heure_mesure_temp TEXT,  -- ← Retiré NOT NULL
    latitude DOUBLE PRECISION,
    libelle_commune TEXT,
    libelle_cours_eau TEXT,
    libelle_parametre TEXT,
    libelle_qualification TEXT,
    libelle_station TEXT,
    longitude DOUBLE PRECISION,
    resultat DOUBLE PRECISION,
    symbole_unite TEXT,
    uri_cours_eau TEXT,
    uri_station TEXT,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    -- ← Retiré PRIMARY KEY (code_station, date_mesure_temp, heure_mesure_temp)
);

-- ← Retiré FOREIGN KEY

CREATE INDEX IF NOT EXISTS idx_temperature_chroniques_raw_date_mesure_temp
ON hubeau.temperature_chroniques_raw(date_mesure_temp);

CREATE INDEX IF NOT EXISTS idx_temperature_chroniques_raw_code_station
ON hubeau.temperature_chroniques_raw(code_station);

COMMENT ON TABLE hubeau.temperature_chroniques_raw IS
'Bronze layer: Raw data from Hub''Eau API - duplicates allowed, no constraints, transformations in dbt';
```

---

### 5. ⏳ Mettre à jour 20 configs YAML

**Configs déjà modifiés (2/22):**
- ✅ `configs/hubeau/temperature_chroniques.yml`
- ✅ `configs/hubeau/temperature_stations.yml`

**Configs à modifier (20/22):**

1. `piezometry_chroniques.yml`
2. `piezometry_stations.yml`
3. `hydrometry_obs_elab.yml`
4. `hydrometry_sites.yml`
5. `hydrometry_stations.yml`
6. `hydrobio_indices.yml`
7. `hydrobio_stations.yml`
8. `hydrobio_taxons.yml`
9. `quality_rivers_analyses.yml`
10. `quality_rivers_conditions.yml`
11. `quality_rivers_operations.yml`
12. `quality_rivers_stations.yml`
13. `quality_groundwater_analyses.yml`
14. `quality_groundwater_stations.yml`
15. `ecoulement_campagnes.yml`
16. `ecoulement_observations.yml`
17. `ecoulement_stations.yml`
18. `prelevements_chroniques.yml`
19. `prelevements_ouvrages.yml`
20. `prelevements_points.yml`

**Changements à faire dans chaque fichier:**

```yaml
resource:
  name: {resource_name}_raw  # ← Ajouter suffix "_raw"
  endpoint: /{endpoint}.csv
  base_url: https://hubeau.eaufrance.fr/api/v1/{api}
  primary_key: null  # ← Changer de liste → null (Bronze = no PK)
extraction:
  default_params: {}
performance:
  parallelism: 5  # ← Peut garder (pas utilisé, DLT config.toml contrôle)
  batch_size: 5000
  retry_times: 5
  retry_delay: 2.0
  rate_limit: 0.3
```

**Exemple (temperature_chroniques.yml déjà fait):**

**AVANT:**
```yaml
resource:
  name: temperature_chroniques
  endpoint: /chronique.csv
  base_url: https://hubeau.eaufrance.fr/api/v1/temperature
  primary_key:
  - code_station
  - date_mesure_temp
  - heure_mesure_temp
```

**APRÈS:**
```yaml
resource:
  name: temperature_chroniques_raw  # ← Ajouté _raw
  endpoint: /chronique.csv
  base_url: https://hubeau.eaufrance.fr/api/v1/temperature
  # Bronze layer: no PK/FK constraints, duplicates allowed
  primary_key: null  # ← Changé en null
```

---

### 6. ⏳ Réécrire dlt_assets.py avec 22 assets

**Fichier à modifier:** `src/hubeau_pipeline/assets/bronze/dlt_assets.py`

**Structure:**
- 22 assets Dagster (1 par table Hub'Eau)
- Pattern "stations" (FULL mode, 7 assets)
- Pattern "chroniques" (YEAR + INCREMENTAL, 15 assets)

#### Pattern "stations" (FULL mode)

**Tables stations:**
1. temperature_stations_raw
2. piezometry_stations_raw
3. hydrometry_sites_raw
4. hydrometry_stations_raw
5. hydrobio_stations_raw
6. quality_rivers_stations_raw
7. quality_groundwater_stations_raw
8. ecoulement_stations_raw

**Template asset:**

```python
import dlt
from dagster import asset
import yaml
from hubeau_pipeline.sources.hubeau_csv_source import hubeau_stations

@asset(
    compute_kind="dlt",
    group_name="{api_name}"  # Ex: "temperature", "piezometry", etc.
)
def {table}_raw(context):
    """
    {Table} - FULL load (replace all)
    No partitions, no incremental
    """
    # Load config
    config_path = "configs/hubeau/{table}.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Create DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="hubeau_{table}",
        destination="postgres",  # ← DLT standard destination
        dataset_name="hubeau",
        credentials={
            "database": os.getenv("PG_DB"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT")
        }
    )

    # Run with FULL mode resource
    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="{table}_raw"
    )

    context.log.info(f"✅ Loaded {load_info}")
    return load_info
```

**Exemple concret:**

```python
@asset(compute_kind="dlt", group_name="temperature")
def temperature_stations_raw(context):
    """Temperature stations - FULL load (replace all)"""
    config_path = "configs/hubeau/temperature_stations.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = dlt.pipeline(
        pipeline_name="hubeau_temperature_stations",
        destination="postgres",
        dataset_name="hubeau",
        credentials={...}
    )

    load_info = pipeline.run(
        hubeau_stations(config),
        table_name="temperature_stations_raw"
    )

    context.log.info(f"✅ Loaded {load_info}")
    return load_info
```

---

#### Pattern "chroniques" (YEAR + INCREMENTAL)

**Tables chroniques/observations:**
1. temperature_chroniques_raw
2. piezometry_chroniques_raw
3. hydrometry_obs_elab_raw
4. hydrobio_indices_raw
5. hydrobio_taxons_raw
6. quality_rivers_analyses_raw
7. quality_rivers_conditions_raw
8. quality_rivers_operations_raw
9. quality_groundwater_analyses_raw
10. ecoulement_campagnes_raw
11. ecoulement_observations_raw
12. prelevements_chroniques_raw
13. prelevements_ouvrages_raw
14. prelevements_points_raw

**Template asset:**

```python
import dlt
from dagster import asset, YearlyPartitionsDefinition
import yaml
import os
from hubeau_pipeline.sources.hubeau_csv_source import (
    hubeau_chroniques_year,
    hubeau_chroniques_incremental
)
from hubeau_pipeline.utils.db_helpers import delete_year_data

@asset(
    compute_kind="dlt",
    group_name="{api_name}",
    partitions_def=YearlyPartitionsDefinition(start_date="2020-01-01")
)
def {table}_raw(context):
    """
    {Table} - Support YEAR partition + INCREMENTAL

    - With partition: Load specific year (backfill)
    - Without partition: Incremental from last date
    """
    # Load config
    config_path = "configs/hubeau/{table}.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Create DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="hubeau_{table}",
        destination="postgres",
        dataset_name="hubeau",
        credentials={
            "database": os.getenv("PG_DB"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT")
        }
    )

    # Check if partition mode
    if context.has_partition_key:
        # ===== YEAR PARTITION MODE =====
        year = context.partition_key
        context.log.info(f"🗓️  YEAR PARTITION mode: {year}")

        # Delete existing data for this year (idempotence)
        deleted = delete_year_data(
            table_name="{table}_raw",
            year=year,
            date_column="{date_column}"  # Ex: "date_mesure_temp"
        )
        context.log.info(f"🗑️  Deleted {deleted} existing records for year {year}")

        # Load year data
        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="{table}_raw"
        )
    else:
        # ===== INCREMENTAL MODE =====
        context.log.info(f"📈 INCREMENTAL mode (from last date)")

        # Use DLT incremental (tracks last date automatically)
        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("{date_column}")
            ),
            table_name="{table}_raw"
        )

    context.log.info(f"✅ Loaded: {load_info}")
    return load_info
```

**Exemple concret:**

```python
@asset(
    compute_kind="dlt",
    group_name="temperature",
    partitions_def=YearlyPartitionsDefinition(start_date="2020-01-01")
)
def temperature_chroniques_raw(context):
    """
    Temperature chroniques - YEAR partition + INCREMENTAL
    """
    config_path = "configs/hubeau/temperature_chroniques.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline = dlt.pipeline(
        pipeline_name="hubeau_temperature_chroniques",
        destination="postgres",
        dataset_name="hubeau",
        credentials={...}
    )

    if context.has_partition_key:
        year = context.partition_key
        context.log.info(f"🗓️  YEAR PARTITION: {year}")

        deleted = delete_year_data(
            "temperature_chroniques_raw",
            year,
            "date_mesure_temp"
        )
        context.log.info(f"🗑️  Deleted {deleted} records")

        load_info = pipeline.run(
            hubeau_chroniques_year(config, year=year),
            table_name="temperature_chroniques_raw"
        )
    else:
        context.log.info(f"📈 INCREMENTAL mode")

        load_info = pipeline.run(
            hubeau_chroniques_incremental(
                config,
                last_date=dlt.sources.incremental("date_mesure_temp")
            ),
            table_name="temperature_chroniques_raw"
        )

    context.log.info(f"✅ Loaded: {load_info}")
    return load_info
```

---

#### Mapping tables → date_column

**Important:** Chaque table a une colonne date différente pour l'incrémental.

| Table | date_column |
|-------|-------------|
| temperature_chroniques_raw | `date_mesure_temp` |
| piezometry_chroniques_raw | `date_mesure` |
| hydrometry_obs_elab_raw | `date_obs_elab` |
| hydrobio_indices_raw | `date_prelevement` |
| quality_rivers_analyses_raw | `date_prelevement` |
| quality_groundwater_analyses_raw | `date_prelevement` |
| ecoulement_observations_raw | `date_observation` |
| prelevements_chroniques_raw | `annee` (attention: pas timestamp!) |

**À vérifier dans les schemas originaux pour confirmer les noms de colonnes dates.**

---

### 7. ⏳ Rebuild et tester

#### Étape 7.1: Rebuild Docker

```bash
# Arrêter conteneurs
docker compose down

# Rebuild images (code source a changé)
docker compose build --no-cache dlt_worker dagster_webserver dagster_daemon

# Redémarrer tout
docker compose up -d

# Vérifier santé
docker ps
```

#### Étape 7.2: Test 1 - Temperature stations (FULL)

**Dans Dagster UI (http://localhost:8080):**

1. Aller dans **Assets**
2. Trouver `temperature_stations_raw`
3. Cliquer **Materialize**
4. Observer logs:
   - "Loading X records from Y pages"
   - DLT parallelization avec 5 workers
   - Durée ~20 secondes (850 stations)

**Vérifier en base:**
```sql
SELECT COUNT(*) FROM hubeau.temperature_stations_raw;
-- Attendu: 850 records

SELECT * FROM hubeau.temperature_stations_raw LIMIT 5;
-- Vérifier _ingested_at existe
```

#### Étape 7.3: Test 2 - Temperature chroniques (YEAR partition)

**Dans Dagster UI:**

1. Aller dans **Assets**
2. Trouver `temperature_chroniques_raw`
3. Cliquer **Materialize** → Choisir partition **2024**
4. Observer logs:
   - "YEAR PARTITION mode: 2024"
   - "Deleted X existing records for year 2024"
   - "Loading 1,526,051 records from 1527 pages"
   - Progress logs every 10 pages
   - Durée ~5-8 minutes (avec 5 workers)

**Vérifier en base:**
```sql
SELECT COUNT(*) FROM hubeau.temperature_chroniques_raw
WHERE EXTRACT(YEAR FROM date_mesure_temp) = 2024;
-- Attendu: ~1,526,051 records (avec duplicats!)

SELECT
    COUNT(*) as total,
    COUNT(DISTINCT (code_station, date_mesure_temp, heure_mesure_temp)) as unique_pk
FROM hubeau.temperature_chroniques_raw;
-- Si unique_pk < total → confirme qu'on garde duplicats ✅
```

#### Étape 7.4: Test 3 - Temperature chroniques (INCREMENTAL)

**Dans Dagster UI:**

1. Aller dans **Assets**
2. Trouver `temperature_chroniques_raw`
3. Cliquer **Materialize** → **SANS choisir partition**
4. Observer logs:
   - "INCREMENTAL mode"
   - "First load: loading from 2024-01-04" (si table vide)
   - OU "Incremental: loading since 2024-12-31" (si table a data)

**Vérifier state DLT:**
```bash
# DLT sauvegarde state dans ~/.dlt/pipelines/
docker exec brgm-dlt-worker ls -la ~/.dlt/pipelines/hubeau_temperature_chroniques/
```

#### Étape 7.5: Test 4 - Re-run année (idempotence)

**Test idempotence:**

1. Materialiser partition 2024 une première fois
2. Compter records: `SELECT COUNT(*) → N`
3. Materialiser partition 2024 une deuxième fois
4. Compter records: `SELECT COUNT(*) → N` (même nombre!)
5. Logs montrent "Deleted X records for year 2024" avant reload

✅ **Idempotence confirmée** si même nombre après re-run.

---

## 📊 Résumé des modes d'ingestion

| Mode | Usage | Disposition | Exemple |
|------|-------|-------------|---------|
| **FULL** (stations) | Tout remplacer | `replace` (TRUNCATE+INSERT) | temperature_stations_raw |
| **YEAR** (partition) | Backfill historique | `append` (INSERT) + DELETE year avant | temperature_chroniques_raw [2024] |
| **INCREMENTAL** | Production daily | `append` (INSERT) + DLT track MAX(date) | temperature_chroniques_raw (no partition) |

---

## 🎯 Résultats attendus

### Performance attendue:

| Table | Records | Pages | Durée (5 workers) |
|-------|---------|-------|-------------------|
| temperature_stations | 850 | 1 | ~20 secondes |
| temperature_chroniques (2024) | 1,526,051 | 1,527 | ~5-8 minutes |
| piezometry_chroniques (2024) | ~500,000 | ~500 | ~3-5 minutes |

### Stockage attendu (avec duplicats):

| Table | Records uniques | Records totaux (avec duplicats) | % duplicats |
|-------|----------------|----------------------------------|-------------|
| temperature_chroniques_raw | ~763,000 | ~1,526,000 | ~50% |
| piezometry_chroniques_raw | ~500,000 | ~500,000 | ~0% |

**Note:** Les duplicats seront nettoyés en Silver layer (dbt).

---

## 🚨 Points d'attention

### 1. Duplicats massifs dans Bronze

**Station 1115000 (température):** 100% de duplicats!
- Même `(code_station, date_mesure_temp, heure_mesure_temp)`
- Valeurs `resultat` légèrement différentes (9.583°C vs 9.509°C)
- API retourne mesures répétées

**Impact:**
- Tables _raw auront ~50% de records en plus
- C'est NORMAL en Bronze (on garde tout)
- Silver (dbt) fera `DROP DUPLICATES` avec `keep='last'`

### 2. Colonne `annee` vs timestamp

**Table `prelevements_chroniques`:**
- Colonne `annee` (INTEGER), pas TIMESTAMP
- Incrémental DLT ne marchera pas directement
- Solution: Utiliser seulement YEAR partition pour cette table (pas incremental)

### 3. Re-run partitions

**Avec DELETE before run:**
- ✅ Idempotent (même résultat)
- ✅ Pas de duplicats pour re-runs
- ❌ Pas d'historique des loads

**Alternative (si besoin historique):**
- Ajouter `_run_id` UUID
- Garder toutes les versions
- Dédup par `MAX(_run_id)` en Silver

### 4. DLT state management

**DLT sauvegarde state dans:**
```
~/.dlt/pipelines/{pipeline_name}/
```

**Pour reset incremental state:**
```bash
rm -rf ~/.dlt/pipelines/hubeau_temperature_chroniques/
```

### 5. Credentials PostgreSQL

**DLT nécessite credentials explicit dans `pipeline.run()`:**

```python
pipeline = dlt.pipeline(
    destination="postgres",
    credentials={
        "database": os.getenv("PG_DB"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT")
    }
)
```

**Alternative:** Utiliser `.dlt/secrets.toml`:
```toml
[destination.postgres.credentials]
database = "hubeau"
user = "postgres"
password = "xxx"
host = "postgres"
port = 5432
```

---

## 📚 Références

### Documentation DLT:
- **Parallelization:** https://dlthub.com/docs/reference/performance
- **Incremental loading:** https://dlthub.com/docs/general-usage/incremental-loading
- **Postgres destination:** https://dlthub.com/docs/dlt-ecosystem/destinations/postgres

### Documentation Hub'Eau API:
- **Temperature:** https://hubeau.eaufrance.fr/page/api-temperature
- **Piezometry:** https://hubeau.eaufrance.fr/page/api-piezometrie
- **Hydrometry:** https://hubeau.eaufrance.fr/page/api-hydrometrie

---

## 🔄 Prochaines étapes (après Bronze)

### Silver Layer (dbt)

**À implémenter plus tard:**

1. **Modèles dbt pour chaque table:**
   ```sql
   -- models/silver/temperature_chroniques.sql
   SELECT DISTINCT ON (code_station, date_mesure_temp, heure_mesure_temp)
       *
   FROM {{ source('bronze', 'temperature_chroniques_raw') }}
   ORDER BY code_station, date_mesure_temp, heure_mesure_temp, _ingested_at DESC
   ```

2. **Tests dbt:**
   - Unicité PK
   - FK valides
   - Pas de nulls sur colonnes importantes

3. **Documentation dbt:**
   - Description colonnes
   - Lineage Bronze → Silver

---

## ✅ Checklist finale avant test

Avant de lancer le test, vérifier:

- [ ] `.dlt/config.toml` existe avec workers config
- [ ] `db_helpers.py` créé avec toutes fonctions
- [ ] `hubeau_csv_source.py` réécrit (320 lignes)
- [ ] 22 schemas SQL `*_raw.sql` créés (20 à faire)
- [ ] 22 configs YAML mis à jour avec `primary_key: null` (20 à faire)
- [ ] `dlt_assets.py` réécrit avec 22 assets (à faire)
- [ ] Docker rebuild: `docker compose build --no-cache`
- [ ] Vérifier logs: pas d'erreur import
- [ ] Test temperature_stations_raw: FULL mode OK
- [ ] Test temperature_chroniques_raw: YEAR partition OK
- [ ] Test temperature_chroniques_raw: INCREMENTAL OK
- [ ] Vérifier duplicats conservés en base
- [ ] Vérifier idempotence (re-run year → même COUNT)

---

**FIN DU DOCUMENT**

Quand tu relances une nouvelle conversation, dis:
> "Implémente les étapes 4-7 du document IMPLEMENTATION_PLAN_BRONZE_LAYER.md"

Et je continuerai depuis là! 🚀
