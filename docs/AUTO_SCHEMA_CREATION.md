# Création Automatique de Schéma PostgreSQL

> **Architecture** : DLT + Pandas → Inférence automatique des types → PostgreSQL
> **Version** : 4.0 - PostgresBulkDestinationV2
> **Date** : 2025-10-24

## Table des Matières

1. [Concept](#concept)
2. [Fonctionnement Technique](#fonctionnement-technique)
3. [Stratégie ULTRA-SAFE](#stratégie-ultra-safe)
4. [Cycle de Vie d'une Table](#cycle-de-vie-dune-table)
5. [Gestion Base Existante](#gestion-base-existante)
6. [Auto-Fix des Erreurs de Type](#auto-fix-des-erreurs-de-type)
7. [Exemples Pratiques](#exemples-pratiques)
8. [Optimisation Post-Ingestion](#optimisation-post-ingestion)

---

## Concept

### Zéro Maintenance de Schéma

Le pipeline Hub'Eau utilise une approche **révolutionnaire** : **pas de définition manuelle de schéma SQL**.

**Comment ?**
1. DLT reçoit les données CSV depuis Hub'Eau API
2. Pandas charge le CSV et infère les types automatiquement
3. `PostgresBulkDestinationV2` crée la table PostgreSQL depuis le DataFrame
4. Les données sont chargées via COPY bulk ultra-rapide
5. Les runs suivants utilisent MERGE/UPSERT sur clés primaires

**Avantages** :
- ✅ Zéro fichier SQL CREATE TABLE à maintenir
- ✅ Adaptation automatique aux changements d'API Hub'Eau
- ✅ Pas d'erreurs de type lors du COPY (stratégie TEXT ultra-safe)
- ✅ Déploiement simplifié (pas de migrations)
- ✅ Optimisation des types en post-processing si nécessaire

---

## Fonctionnement Technique

### Pipeline Complet

```
┌─────────────────────────────────────────────────────────┐
│ 1. Hub'Eau API → CSV Response                          │
│    GET /chroniques?date_debut=2024-01-01                │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ 2. Pandas DataFrame                                     │
│    df = pd.DataFrame(csv_data)                          │
│    - Inférence automatique des types                    │
│    - date_mesure → datetime64                           │
│    - code_bss → object (str)                            │
│    - niveau_eau_ngf → float64                           │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ 3. PostgresBulkDestinationV2._create_table_from_dataframe() │
│    - Vérifie si table existe (information_schema)       │
│    - Si non → CREATE TABLE avec types inférés           │
│    - Stratégie: TEXT par défaut (ultra-safe)            │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ 4. COPY Bulk PostgreSQL                                │
│    COPY hubeau.piezometry_chroniques FROM STDIN         │
│    - 100k records en 1-2 secondes                       │
│    - TEXT accepte tout → zéro erreur                    │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ 5. Runs Suivants                                        │
│    - Table existe déjà                                  │
│    - MERGE/UPSERT sur primary keys                      │
│    - Déduplication automatique                          │
└─────────────────────────────────────────────────────────┘
```

### Code Source

**Localisation** : `src/hubeau_pipeline/destinations/postgres_optimized_v2.py:282`

```python
def _create_table_from_dataframe(self, df: pd.DataFrame, table_name: str, conn):
    """
    Crée la table si elle n'existe pas - STRATÉGIE ULTRA-SAFE

    Tout est créé en TEXT sauf datetime évident
    Optimisation des types se fait APRÈS ingestion

    Avantages:
    - Zéro erreur COPY (text accepte tout)
    - Pas de retries multiples
    - Performance optimale en post-processing
    """
    with conn.cursor() as cursor:
        # Inférence ULTRA-SAFE: TEXT par défaut
        col_defs = []
        for col in df.columns:
            dtype = df[col].dtype

            # Seulement datetime est typé directement (évident et safe)
            if 'datetime' in str(dtype):
                pg_type = 'TIMESTAMP'
            else:
                # TOUT LE RESTE en TEXT - ultra-safe!
                pg_type = 'TEXT'

            col_defs.append(f"{col} {pg_type}")

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema_name}.{table_name} (
                {', '.join(col_defs)}
            )
        """
        cursor.execute(create_sql)
        conn.commit()
        logger.info(f"✅ Table {table_name} créée avec {len(col_defs)} colonnes")
```

---

## Stratégie ULTRA-SAFE

### Pourquoi TEXT par Défaut ?

**Problème classique** : Inférence de type trop agressive
```sql
-- Pandas infère: integer
CREATE TABLE stations (code_station INTEGER);

-- Erreur COPY si données réelles:
-- "075ABCD" → ERROR: invalid input syntax for integer
```

**Solution Hub'Eau** : TEXT par défaut
```sql
-- PostgresBulkDestinationV2 crée:
CREATE TABLE stations (code_station TEXT);

-- COPY réussit toujours:
-- "075ABCD" → OK
-- "123456" → OK
-- "" → OK (NULL)
```

### Types Inférés Automatiquement

| Type Pandas | Type PostgreSQL | Raison |
|-------------|-----------------|--------|
| `datetime64[ns]` | `TIMESTAMP` | Safe et évident |
| `object` (str) | `TEXT` | Ultra-safe, accepte tout |
| `float64` | `TEXT` | Safe (évite erreurs virgule française) |
| `int64` | `TEXT` | Safe (codes peuvent contenir lettres) |
| `bool` | `TEXT` | Safe (variantes "true", "1", "oui") |

**Note** : L'optimisation des types se fait après ingestion si nécessaire (voir section dédiée).

---

## Cycle de Vie d'une Table

### Premier Run (Table n'existe pas)

```bash
# 1. Lancement Dagster Asset
dagster asset materialize piezometry_chroniques_csv
```

```python
# 2. DLT récupère CSV Hub'Eau
response = httpx.get("https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques")
df = pd.DataFrame(response.json()["data"])
# df contient 100k records, 12 colonnes
```

```sql
-- 3. PostgresBulkDestinationV2 détecte table inexistante
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'hubeau' AND table_name = 'piezometry_chroniques';
-- Résultat: 0 rows → Table n'existe pas

-- 4. Création automatique
CREATE TABLE IF NOT EXISTS hubeau.piezometry_chroniques (
    code_bss TEXT,
    timestamp_mesure TIMESTAMP,
    niveau_eau_ngf TEXT,
    profondeur_nappe TEXT,
    altitude_station TEXT,
    ...
);
-- ✅ Table créée avec 12 colonnes

-- 5. COPY bulk
COPY hubeau.piezometry_chroniques FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t');
-- ✅ 100k records chargés en 1.5 secondes
```

### Runs Suivants (Table existe)

```bash
# 1. Lancement Asset (mise à jour incrémentale)
dagster asset materialize piezometry_chroniques_csv --config '{"mode": "INCREMENTAL"}'
```

```python
# 2. DLT récupère seulement derniers 7 jours
response = httpx.get("...chroniques?date_debut=2024-10-17")
df = pd.DataFrame(response.json()["data"])
# df contient 5k records
```

```sql
-- 3. Table existe déjà
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'hubeau' AND table_name = 'piezometry_chroniques';
-- Résultat: 12 colonnes → Table existe

-- 4. UPSERT via staging table
CREATE TEMP TABLE staging_1729778450 AS SELECT * FROM hubeau.piezometry_chroniques LIMIT 0;
COPY staging_1729778450 FROM STDIN;
-- 5k records copiés vers staging

-- 5. MERGE (INSERT + UPDATE)
INSERT INTO hubeau.piezometry_chroniques (code_bss, timestamp_mesure, ...)
SELECT * FROM staging_1729778450
ON CONFLICT (code_bss, timestamp_mesure)  -- Clés primaires
DO UPDATE SET
    niveau_eau_ngf = EXCLUDED.niveau_eau_ngf,
    profondeur_nappe = EXCLUDED.profondeur_nappe,
    ...;
-- ✅ 5k records mergés (3k INSERT, 2k UPDATE)
```

---

## Gestion Base Existante

### Message PostgreSQL Normal

**Lors du démarrage Docker Compose**, PostgreSQL affiche :

```
PostgreSQL Database directory appears to contain a database; Skipping initialization
```

### ✅ Ce Message est NORMAL et ATTENDU

**Explication** :
1. PostgreSQL détecte que `/var/lib/postgresql/data` contient déjà une base
2. Les scripts d'init (`01_init_minimal.sql`, `99-verify-initialization.sql`) sont **skip**
3. PostgreSQL démarre directement avec la base existante

**Ce n'est PAS une erreur !** C'est le comportement standard de PostgreSQL.

### Comportement selon État Base

| État Base | Comportement Pipeline | Action |
|-----------|----------------------|--------|
| **Nouvelle** (vide) | Scripts init exécutés → Schéma `hubeau` créé → PostGIS activé | Aucune |
| **Existe, schéma OK** | Scripts skip → Schéma `hubeau` utilisé | Aucune |
| **Existe, tables OK** | MERGE/UPSERT des données | Aucune |
| **Existe, tables manquantes** | Création automatique des tables manquantes | Aucune |
| **Existe, schéma manquant** | ❌ Erreur DLT (schema not found) | Exécuter `01_init_minimal.sql` manuellement |

### Vérification Santé Base

```bash
# Accéder au conteneur PostgreSQL
docker exec -it brgm-postgres psql -U postgres -d postgres

# Vérifier schéma hubeau
\dn hubeau
# Résultat attendu:
#   List of schemas
#   Name   |  Owner
# ---------+----------
#  hubeau  | postgres

# Lister tables Hub'Eau
\dt hubeau.*
# Résultat (si données déjà chargées):
#  Schema |          Name            | Type  |  Owner
# --------+--------------------------+-------+----------
#  hubeau | piezometry_chroniques    | table | postgres
#  hubeau | piezometry_stations      | table | postgres
#  ...

# Si aucune table: NORMAL! Tables créées au premier run d'asset
```

---

## Auto-Fix des Erreurs de Type

### Problème : Données Texte dans Colonne Numérique

Parfois, l'API Hub'Eau renvoie des codes alphanumériques dans des colonnes attendues numériques.

**Exemple** :
```csv
code_entite_hydro,debit_moyen
O07-0400,12.5
O07-0401,15.3
```

`code_entite_hydro` semble numérique mais contient "O07-0400" (lettre O) → Erreur COPY

### Solution Automatique

**Localisation** : `src/hubeau_pipeline/destinations/postgres_optimized_v2.py:383`

```python
except errors.InvalidTextRepresentation as e:
    # Erreur: texte dans colonne numérique
    error_msg = str(e)
    if 'invalid input syntax for type' in error_msg:
        # Extraction nom colonne depuis erreur
        match = re.search(r'column (\w+):', error_msg)
        if match:
            problematic_column = match.group(1)

            # AUTO-FIX: ALTER vers TEXT
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    ALTER TABLE {self.schema_name}.{table_name}
                    ALTER COLUMN {problematic_column} TYPE TEXT
                """)
                conn.commit()

            # RETRY COPY
            cursor.copy_expert(copy_sql, output)
            conn.commit()
            # ✅ COPY réussi après auto-fix
```

**Résultat** :
```sql
-- Avant auto-fix:
code_entite_hydro DOUBLE PRECISION  -- ❌ Erreur COPY

-- Après auto-fix:
code_entite_hydro TEXT  -- ✅ COPY réussi
```

### Récursivité Auto-Fix

Si **plusieurs colonnes** sont mal typées, l'auto-fix s'exécute jusqu'à **10 fois maximum** :

```python
max_retries = 10  # Max 10 colonnes à corriger
for retry_attempt in range(max_retries):
    try:
        cursor.copy_expert(copy_sql, output)
        return  # Success!
    except errors.InvalidTextRepresentation as retry_error:
        # Encore une autre colonne mal typée
        # → ALTER vers TEXT et continuer
```

---

## Exemples Pratiques

### Exemple 1 : Première Ingestion Piézométrie

```bash
# Démarrer pipeline
docker compose up -d

# Accéder Dagster UI
open http://localhost:8080

# Matérialiser asset (Launchpad)
Asset: piezometry_chroniques_csv
Config:
  mode: FULL
  date_debut: 2024-01-01
```

**Logs Dagster** :
```
2024-10-24 14:30:00 - INFO - 🚀 Chargement 156789 records → piezometry_chroniques (replace)
2024-10-24 14:30:00 - WARNING - ⚠️ Table piezometry_chroniques n'existe pas - création automatique
2024-10-24 14:30:01 - INFO - ✅ Table piezometry_chroniques créée avec 12 colonnes
2024-10-24 14:30:03 - INFO - ✅ COPY: 156789 records → piezometry_chroniques
```

**PostgreSQL** :
```sql
SELECT COUNT(*) FROM hubeau.piezometry_chroniques;
-- 156789

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'hubeau' AND table_name = 'piezometry_chroniques';
-- code_bss          | text
-- timestamp_mesure  | timestamp without time zone
-- niveau_eau_ngf    | text
-- ...
```

### Exemple 2 : Mise à Jour Incrémentale

```bash
# Run quotidien (cron ou Dagster schedule)
dagster asset materialize piezometry_chroniques_csv --config '{"mode": "INCREMENTAL", "days": 7}'
```

**Logs** :
```
2024-10-24 14:35:00 - INFO - 🚀 Chargement 4523 records → piezometry_chroniques (merge)
2024-10-24 14:35:01 - INFO - ✅ UPSERT: 4523/4523 records modifiés
```

**PostgreSQL** :
```sql
SELECT COUNT(*) FROM hubeau.piezometry_chroniques;
-- 159234  (156789 + 2445 nouveaux, 2078 updates)

-- Dernière mesure
SELECT MAX(timestamp_mesure) FROM hubeau.piezometry_chroniques;
-- 2024-10-24 12:00:00
```

### Exemple 3 : Auto-Fix Type

**Scénario** : API Hub'Eau renvoie codes alphanumériques dans champ numérique attendu.

**Logs** :
```
2024-10-24 14:40:00 - INFO - 🚀 Chargement 8234 records → hydrometry_obs_elab (replace)
2024-10-24 14:40:01 - ERROR - ❌ Erreur COPY: invalid input syntax for type double precision: "O07-0400"
2024-10-24 14:40:01 - WARNING - 🔧 Colonne code_site contient du texte mais est typée comme numérique - AUTO-FIX en TEXT
2024-10-24 14:40:01 - INFO - ✅ Colonne code_site convertie en TEXT
2024-10-24 14:40:01 - INFO - 🔄 Retry COPY après correction de schéma...
2024-10-24 14:40:02 - INFO - ✅ COPY réussi: 8234 records → hydrometry_obs_elab
```

**PostgreSQL** :
```sql
-- Schéma corrigé automatiquement
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'hubeau' AND table_name = 'hydrometry_obs_elab' AND column_name = 'code_site';
-- code_site | text  (était double precision avant auto-fix)
```

---

## Optimisation Post-Ingestion

### Pourquoi Post-Optimiser ?

**Stratégie actuelle** : TEXT par défaut → Performance COPY maximale, zéro erreur

**Inconvénient** : Requêtes analytiques moins optimales sur colonnes TEXT numériques
```sql
-- TEXT → Pas d'optimisation range scan
SELECT * FROM hubeau.piezometry_chroniques
WHERE niveau_eau_ngf::DOUBLE PRECISION > 100.0;  -- Cast requis, lent
```

**Solution** : Optimiser les types APRÈS ingestion complète

### SchemaOptimizer (Optionnel)

**Concept** : Analyser données réelles → Déterminer type optimal → ALTER COLUMN

```python
# Pseudo-code (à implémenter si besoin)
class SchemaOptimizer:
    def optimize_table(self, table_name: str):
        """Optimise types colonnes basé sur données réelles"""

        # 1. Analyser colonnes TEXT
        text_columns = self._get_text_columns(table_name)

        for col in text_columns:
            # 2. Échantillonner données (10k rows)
            sample = self._sample_column(table_name, col, limit=10000)

            # 3. Détecter type optimal
            optimal_type = self._infer_optimal_type(sample)
            # "123.45" (100% valid) → DOUBLE PRECISION
            # "2024-10-24" (100% valid) → DATE
            # "O07-0400" (alphanumeric) → TEXT (garder)

            # 4. ALTER si bénéfice
            if optimal_type != 'TEXT':
                self._alter_column_type(table_name, col, optimal_type)
                # ALTER TABLE hubeau.piezometry_chroniques
                # ALTER COLUMN niveau_eau_ngf TYPE DOUBLE PRECISION USING niveau_eau_ngf::DOUBLE PRECISION;
```

**Utilisation** :
```bash
# Après ingestion FULL complète
dagster asset materialize optimize_piezometry_schema
```

**Avantages** :
- Ingestion ultra-rapide (TEXT)
- Requêtes optimales (types corrects après optimisation)
- Pas de maintenance manuelle

---

## Ressources

- **Code Source** : `src/hubeau_pipeline/destinations/postgres_optimized_v2.py`
- **Script Init** : `docker/init-scripts/postgres/01_init_minimal.sql`
- **Doc Architecture** : [ARCHITECTURE.md](ARCHITECTURE.md)
- **Doc Schéma** : [SCHEMA_BDD.md](SCHEMA_BDD.md)

---

**Création Automatique = Zéro Maintenance** 🚀
