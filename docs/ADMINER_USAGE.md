# Adminer - Administration PostgreSQL

Adminer est une interface web légère pour administrer PostgreSQL.

## Accès

- **URL**: http://localhost:8081
- **Système**: PostgreSQL
- **Serveur**: `postgres`
- **Utilisateur**: `postgres`
- **Mot de passe**: Voir `.env` (variable `PG_PASSWORD`)
- **Base de données**: `postgres`

## Navigation

### 1. Explorer le schéma Hub'Eau

Après connexion:
1. Cliquer sur "Select schema" en haut
2. Choisir `hubeau`
3. Voir toutes les tables DLT

### 2. Tables importantes

#### Tables système DLT
- `_dlt_pipeline_state`: État incrémental des pipelines
- `_dlt_loads`: Historique des chargements
- `_dlt_version`: Version du schéma DLT

#### Tables données Hub'Eau

**Stations (référence)**:
- `piezometry_stations`
- `hydrometry_stations`
- `quality_rivers_stations`
- `quality_groundwater_stations`
- `hydrobio_stations`
- `ecoulement_stations`
- `prelevements_ouvrages`
- `temperature_stations`

**Observations (chroniques)**:
- `piezometry_chroniques_historical`
- `hydrometry_obs_elab`
- `quality_rivers_analyses`
- `quality_groundwater_analyses`
- `hydrobio_taxons`, `hydrobio_indices`
- `ecoulement_observations`
- `prelevements_chroniques`
- `temperature_chroniques`

## Queries SQL utiles

### Compter les lignes par table

```sql
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'hubeau'
ORDER BY tablename;
```

### Voir les derniers chargements DLT

```sql
SELECT
    load_id,
    schema_name,
    status,
    inserted_at
FROM hubeau._dlt_loads
ORDER BY inserted_at DESC
LIMIT 10;
```

### Vérifier l'état incrémental

```sql
SELECT
    version,
    engine_version,
    pipeline_name,
    state,
    _dlt_load_id,
    _dlt_id
FROM hubeau._dlt_pipeline_state
ORDER BY _dlt_id DESC
LIMIT 5;
```

### Compter les stations par type

```sql
-- Piézométrie
SELECT COUNT(DISTINCT code_bss) FROM hubeau.piezometry_stations;

-- Hydrométrie
SELECT COUNT(DISTINCT code_station) FROM hubeau.hydrometry_stations;

-- Qualité rivières
SELECT COUNT(DISTINCT code_station) FROM hubeau.quality_rivers_stations;
```

### Voir les données récentes

```sql
-- Dernières mesures piézométrie
SELECT
    code_bss,
    timestamp_mesure,
    niveau_nappe_ngf,
    _dlt_load_id
FROM hubeau.piezometry_chroniques_historical
ORDER BY timestamp_mesure DESC
LIMIT 100;
```

## Adminer vs PgAdmin

| Fonctionnalité | Adminer | PgAdmin |
|----------------|---------|---------|
| **Interface** | Simple, une page | Complexe, multi-panels |
| **Performance** | Très rapide | Peut être lent |
| **Queries SQL** | ✅ Excellent | ✅ Excellent |
| **Visualisation** | ✅ Simple | ✅ Avancée |
| **Export données** | ✅ CSV, SQL | ✅ Multiples formats |
| **Gestion users** | ⚠️ Basique | ✅ Avancée |
| **Backup/Restore** | ⚠️ Basique | ✅ Avancée |

**Recommandation**:
- Adminer pour exploration rapide et queries ad-hoc
- PgAdmin pour administration complète (backups, users, etc.)
