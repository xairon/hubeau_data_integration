# Schema Optimization - Hub'Eau PostgreSQL

## 🎯 Objectif

Optimiser automatiquement les schémas PostgreSQL après ingestion des données Hub'Eau.

### Problème Résolu

Les données Hub'Eau sont ingérées avec tous les champs en **TEXT** (stratégie ultra-safe pour éviter les erreurs COPY).

Après ingestion, le SchemaOptimizer analyse les données réelles et :
- ✅ Infère les types optimaux (INTEGER, FLOAT, TIMESTAMP, BOOLEAN, GEOMETRY)
- ✅ Détecte automatiquement les Primary Keys (patterns `code_*`, `*_id`)
- ✅ Détecte les Foreign Keys (relations entre tables)
- ✅ Crée les index appropriés (PK, FK, spatial GIST)
- ✅ Support PostGIS (conversion coordonnées → GEOMETRY)

## 📊 Stratégie 2-Phases

### Phase 1 : Ingestion ULTRA-SAFE (déjà implémentée)
```python
# PostgresBulkDestinationV2
# Tout est créé en TEXT pour éviter les erreurs COPY
CREATE TABLE hubeau.piezometry_stations_csv (
    code_bss TEXT,
    nom_commune TEXT,
    altitude TEXT,  -- devrait être FLOAT
    date_debut_mesure TEXT,  -- devrait être TIMESTAMP
    ...
)
```

**Avantages** :
- Zéro erreur pendant COPY (TEXT accepte tout)
- Performance optimale (pas de retry loops)
- Simplicité de maintenance

### Phase 2 : Optimisation POST-INGESTION (ce module)
```python
# SchemaOptimizer analyse les données et optimise
ALTER TABLE hubeau.piezometry_stations_csv
    ALTER COLUMN altitude TYPE DOUBLE PRECISION,
    ALTER COLUMN date_debut_mesure TYPE TIMESTAMP,
    ADD CONSTRAINT piezometry_stations_csv_pkey PRIMARY KEY (code_bss),
    ...

CREATE INDEX idx_piezometry_stations_csv_code_commune
    ON hubeau.piezometry_stations_csv(code_commune);
```

**Avantages** :
- Inférence précise basée sur données réelles
- Statistiques complètes pour choix du bon type
- Pas d'impact sur la vitesse d'ingestion

## 🚀 Utilisation

### Option 1 : Via Dagster UI

1. Ouvrir Dagster UI : `http://localhost:8080`
2. Aller dans "Assets"
3. Trouver l'asset `optimize_hubeau_schema`
4. Cliquer sur "Materialize"
5. Configurer (optionnel) :
   ```yaml
   schema: hubeau
   tables: null  # null = toutes les tables
   dry_run: false  # true = simulation sans modifications
   ```

### Option 2 : Via Python (script standalone)

```python
from hubeau_pipeline.schema import SchemaOptimizer

# Configuration PostgreSQL
conn_params = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'your_password'
}

# Créer optimizer
optimizer = SchemaOptimizer(conn_params)

# Option A : Optimiser toutes les tables
results = optimizer.optimize_schema(
    schema='hubeau',
    tables=None,  # None = toutes
    dry_run=False
)

# Option B : Analyser UNE table sans modifications
plan = optimizer.analyze_table('hubeau', 'piezometry_stations_csv')
print(f"Primary Keys détectées: {plan.primary_keys}")
print(f"Foreign Keys détectées: {plan.foreign_keys}")
print(f"Index à créer: {len(plan.indexes_to_create)}")

# Option C : Optimiser UNE table spécifique
plan = optimizer.analyze_table('hubeau', 'piezometry_stations_csv')
stats = optimizer.optimize_table(plan, dry_run=False)
print(f"Types changés: {stats['types_changed']}")
print(f"Index créés: {stats['indexes_created']}")
```

### Option 3 : Via CLI (dagster materialize)

```bash
# Dry-run (simulation)
dagster asset materialize \
    --select optimize_hubeau_schema \
    --config '{"schema": "hubeau", "dry_run": true}' \
    -m hubeau_pipeline

# Optimisation réelle
dagster asset materialize \
    --select optimize_hubeau_schema \
    --config '{"schema": "hubeau", "dry_run": false}' \
    -m hubeau_pipeline
```

## 🔍 Détails Techniques

### 1. Inférence de Types

L'optimizer analyse les valeurs réelles dans chaque colonne :

```python
# Échantillonner 100 valeurs
SELECT column_name FROM table LIMIT 100

# Détecter le type optimal
if all(values match r'^-?[0-9]+$'):
    type = INTEGER/BIGINT (selon max value)
elif all(values can be float()):
    type = DOUBLE PRECISION
elif all(values match ISO timestamp pattern):
    type = TIMESTAMP
elif all(values in ['true', 'false', '0', '1']):
    type = BOOLEAN
else:
    type = TEXT  # garder tel quel
```

### 2. Détection de Primary Keys

Patterns Hub'Eau reconnus :
- `code_*` : `code_bss`, `code_station`, `code_commune`
- `*_id` : `station_id`, `observation_id`
- `id` : identifiant simple

Vérification :
```sql
SELECT
    COUNT(*) as total,
    COUNT(DISTINCT code_bss) as distinct_count
FROM hubeau.piezometry_stations_csv

-- Si distinct_count == total → PRIMARY KEY !
```

### 3. Détection de Foreign Keys

Patterns Hub'Eau :
- `code_station` → référence `stations.code_station`
- `code_commune` → référence `communes.code_commune`
- `code_bss` → référence `piezometry_stations.code_bss`

Vérification que la table cible existe et contient bien les valeurs référencées.

### 4. Création d'Index

Index automatiques :
- **PK** : UNIQUE BTREE sur primary keys
- **FK** : BTREE sur foreign keys (performance JOIN)
- **Spatial** : GIST sur colonnes GEOMETRY (longitude/latitude)

```sql
-- Primary Key index
CREATE UNIQUE INDEX idx_piezometry_stations_csv_pk
    ON hubeau.piezometry_stations_csv(code_bss);

-- Foreign Key index
CREATE INDEX idx_piezometry_stations_csv_code_commune
    ON hubeau.piezometry_stations_csv(code_commune);

-- Spatial index (PostGIS)
CREATE INDEX idx_piezometry_stations_csv_geom
    ON hubeau.piezometry_stations_csv
    USING GIST(geom);
```

### 5. Support PostGIS (Geometry)

Détection automatique de coordonnées GPS :
- `longitude` + `latitude`
- `coord_x` + `coord_y`
- `coordonnee_x` + `coordonnee_y`

Création colonne GEOMETRY :
```sql
-- Ajouter colonne géométrique
ALTER TABLE hubeau.piezometry_stations_csv
    ADD COLUMN geom GEOMETRY(Point, 4326);

-- Peupler depuis longitude/latitude
UPDATE hubeau.piezometry_stations_csv
    SET geom = ST_SetSRID(
        ST_MakePoint(longitude::FLOAT, latitude::FLOAT),
        4326
    )
    WHERE longitude IS NOT NULL AND latitude IS NOT NULL;

-- Index spatial
CREATE INDEX idx_piezometry_stations_csv_geom
    ON hubeau.piezometry_stations_csv USING GIST(geom);
```

## 📈 Performance

### Temps d'Exécution Typique

| Tables | Colonnes | Temps (dry_run) | Temps (réel) |
|--------|----------|-----------------|--------------|
| 1      | 20       | ~2s             | ~5s          |
| 10     | 200      | ~15s            | ~45s         |
| 25     | 500      | ~30s            | ~2min        |

**Note** : L'optimisation peut être longue sur grosses tables (plusieurs millions de lignes) car :
- Analyse statistique des valeurs
- ALTER TABLE = réécriture complète de la table
- Création d'index = scan complet

**Recommandation** : Lancer l'optimisation **après** ingestion, en dehors des heures de pointe.

## 🔧 Configuration Avancée

### Patterns Personnalisés

Vous pouvez ajouter vos propres patterns dans `schema_optimizer.py` :

```python
class SchemaOptimizer:
    # Ajouter patterns PK
    PK_PATTERNS = [
        r'^code_',
        r'_id$',
        r'^my_custom_pk_pattern$',  # ← NOUVEAU
    ]

    # Ajouter patterns FK
    FK_PATTERNS = {
        'code_station': ['stations', 'station'],
        'my_ref_column': ['my_target_table'],  # ← NOUVEAU
    }
```

### Dry-Run Recommandé

Toujours tester avec `dry_run=True` d'abord :

```python
# 1. Dry-run pour voir les changements
results = optimizer.optimize_schema(
    schema='hubeau',
    dry_run=True  # ← Juste affichage, pas de modifications
)

# 2. Vérifier les logs
# [DRY] ALTER TABLE hubeau.piezometry_stations_csv ...
# [DRY] CREATE INDEX idx_...

# 3. Si OK, lancer pour de vrai
results = optimizer.optimize_schema(
    schema='hubeau',
    dry_run=False  # ← Modifications réelles
)
```

## ⚠️ Limitations et Conseils

### 1. ALTER TABLE peut être lent

Sur tables volumineuses (>1M lignes), `ALTER COLUMN TYPE` peut prendre plusieurs minutes car PostgreSQL réécrit toute la table.

**Conseil** : Lancer l'optimisation **après ingestion complète**, pas entre chaque batch.

### 2. Types incompatibles

Si une colonne TEXT contient des valeurs invalides pour le type inféré, l'ALTER échouera.

**Exemple** :
```sql
-- altitude contient "N/A" au lieu de nombres
ALTER TABLE ... ALTER COLUMN altitude TYPE DOUBLE PRECISION;
-- ERROR: invalid input syntax for type double precision: "N/A"
```

**Solution** : Le SchemaOptimizer log l'erreur et continue avec les autres colonnes. Vous pouvez nettoyer manuellement :
```sql
UPDATE hubeau.piezometry_stations_csv
    SET altitude = NULL
    WHERE altitude = 'N/A' OR altitude !~ '^[0-9.]+$';
```

### 3. Foreign Keys et données orphelines

Si vous avez des valeurs dans `code_station` qui n'existent pas dans la table `stations`, la création de FK échouera.

**Solution** : Nettoyer d'abord :
```sql
DELETE FROM hubeau.observations
WHERE code_station NOT IN (
    SELECT code_station FROM hubeau.stations
);
```

Ou utiliser `ON DELETE SET NULL` si approprié.

## 📚 Exemples Complets

### Exemple 1 : Workflow Complet Ingestion + Optimisation

```python
from hubeau_pipeline.destinations import postgres_bulk_destination_v2
from hubeau_pipeline.schema import SchemaOptimizer

# 1. Ingestion (Phase 1 - ULTRA-SAFE)
postgres_bulk_destination_v2.load_batch(
    table_name='piezometry_stations_csv',
    data=data_from_api,
    write_disposition='replace'
)
# → Table créée avec tous les champs en TEXT

# 2. Optimisation (Phase 2 - INTELLIGENT)
optimizer = SchemaOptimizer(conn_params)
plan = optimizer.analyze_table('hubeau', 'piezometry_stations_csv')
stats = optimizer.optimize_table(plan)

# Résultat :
# - code_bss: TEXT → TEXT (garde tel quel, c'est une PK)
# - altitude: TEXT → DOUBLE PRECISION
# - date_debut_mesure: TEXT → TIMESTAMP
# - PRIMARY KEY (code_bss)
# - INDEX sur code_commune
```

### Exemple 2 : Audit de Schéma

```python
# Analyser toutes les tables sans modifications
optimizer = SchemaOptimizer(conn_params)

tables = ['piezometry_stations_csv', 'quality_rivers_analyses_csv']

for table in tables:
    plan = optimizer.analyze_table('hubeau', table)

    print(f"\n📊 {table}")
    print(f"  PK: {plan.primary_keys}")
    print(f"  FK: {len(plan.foreign_keys)}")
    print(f"  Index: {len(plan.indexes_to_create)}")

    for col in plan.columns:
        if col.current_type != col.inferred_type:
            print(f"  {col.column_name}: {col.current_type} → {col.inferred_type}")
```

## 🤝 Intégration dans le Pipeline Dagster

Les assets d'optimisation sont automatiquement disponibles dans Dagster UI après ingestion.

**Workflow recommandé** :

```
1. Ingestion stations     → piezometry_stations_csv (TEXT partout)
2. Ingestion chroniques   → piezometry_chroniques_csv (TEXT partout)
3. Optimisation schéma    → optimize_hubeau_schema (types + PK + FK + index)
4. Data quality checks    → basic_database_check (validation)
```

Vous pouvez créer un **schedule** pour optimiser automatiquement après ingestion :

```python
from dagster import schedule, RunRequest

@schedule(
    job_name="optimize_schema_job",
    cron_schedule="0 4 * * *",  # 04h00 quotidien
)
def daily_schema_optimization():
    return RunRequest(
        run_config={
            "ops": {
                "optimize_hubeau_schema": {
                    "config": {
                        "schema": "hubeau",
                        "dry_run": False
                    }
                }
            }
        }
    )
```

## 🐛 Troubleshooting

### Erreur : "column does not exist"

Si une colonne référencée n'existe pas, vérifiez que :
1. Le nom de la colonne est correct (sensible à la casse)
2. La table a bien été créée par l'ingestion

### Erreur : "duplicate key value violates unique constraint"

Si la création de PK échoue, c'est qu'il y a des doublons :
```sql
-- Trouver les doublons
SELECT code_bss, COUNT(*)
FROM hubeau.piezometry_stations_csv
GROUP BY code_bss
HAVING COUNT(*) > 1;

-- Supprimer les doublons (garder le plus récent)
DELETE FROM hubeau.piezometry_stations_csv a
USING hubeau.piezometry_stations_csv b
WHERE a.ctid < b.ctid AND a.code_bss = b.code_bss;
```

### Performance dégradée après optimisation

Si les requêtes sont plus lentes après optimisation, vérifiez :
1. Que les index ont bien été créés : `\d+ hubeau.table_name`
2. Que les statistiques sont à jour : `ANALYZE hubeau.table_name;`
3. Que le query planner utilise les index : `EXPLAIN ANALYZE SELECT ...`

## 📝 TODO Future

- [ ] Support AUTO VACUUM après ALTER TABLE
- [ ] Détection automatique de colonnes ENUM (pour types custom)
- [ ] Compression automatique (TOAST) pour colonnes texte longues
- [ ] Partitioning automatique par date pour grosses tables
- [ ] Support CHECK constraints (ex: latitude BETWEEN -90 AND 90)
- [ ] Génération automatique de vues optimisées (dénormalisation)
