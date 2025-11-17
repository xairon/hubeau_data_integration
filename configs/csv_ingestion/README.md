# CSV Ingestion - Configuration Guide

## Overview

Ce système permet d'ingérer **n'importe quel CSV** dans PostgreSQL sans écrire de code.

**Workflow:**
```
1. Créer un fichier YAML de config (1 minute)
2. Déposer le CSV dans /app/data/csv_inbox/
3. Asset Dagster automatiquement généré
4. Sensor détecte le fichier et déclenche l'ingestion
5. Données atterrissent dans staging.<table_name>
6. DBT transforme staging → production
```

## Quick Start

### 1. Créer une config YAML

Créer `configs/csv_ingestion/mon_fichier.yml`:

```yaml
source:
  # Pattern du fichier (supporte wildcards)
  file_pattern: "mon_fichier*.csv"

  # Séparateur (optionnel, détecté auto)
  delimiter: ","

  # Colonnes de date à parser (optionnel)
  date_columns:
    - created_at
    - updated_at

destination:
  # Nom de la table (préfixé auto par "staging_")
  table_name: mon_fichier

  # Mode: replace (full refresh) ou append (incrémental)
  write_disposition: replace

  # Primary key pour déduplication (optionnel)
  primary_key:
    - id

metadata:
  description: "Description des données"
  owner: "Data Team"
  tags:
    - bronze
    - staging
```

### 2. Déposer le CSV

```bash
# Local
cp mon_fichier.csv data/csv_inbox/

# Docker
docker cp mon_fichier.csv brgm-dlt-worker:/app/data/csv_inbox/
```

### 3. Matérialiser l'asset

**Option A: Automatique (avec sensor)**
- Activer le sensor `csv_file_watcher` dans Dagster UI
- Le sensor détecte le fichier et déclenche l'ingestion automatiquement

**Option B: Manuel**
- Aller dans Dagster UI → Assets
- Chercher `csv_mon_fichier`
- Cliquer "Materialize"

### 4. Vérifier les données

```sql
-- Connexion à PostgreSQL
SELECT * FROM staging.staging_mon_fichier LIMIT 10;
```

## Exemples

### Exemple 1: CSV simple

**Config:** `configs/csv_ingestion/customers.yml`
```yaml
source:
  file_pattern: "customers.csv"
destination:
  table_name: customers
  write_disposition: replace
  primary_key: [customer_id]
```

**Résultat:**
- Table créée: `staging.staging_customers`
- Asset Dagster: `csv_customers`

### Exemple 2: CSV avec dates

**Config:** `configs/csv_ingestion/transactions.yml`
```yaml
source:
  file_pattern: "transactions*.csv"
  date_columns:
    - transaction_date
    - created_at
destination:
  table_name: transactions
  write_disposition: append
  primary_key: [transaction_id]
```

### Exemple 3: Multiple files (wildcard)

**Config:** `configs/csv_ingestion/sales.yml`
```yaml
source:
  file_pattern: "sales_*.csv"  # Matches sales_2024.csv, sales_2025.csv
destination:
  table_name: sales
  write_disposition: append
```

## Schema Auto-Detection

DLT détecte automatiquement les types de colonnes:

- **Integers** → `BIGINT`
- **Floats** → `DOUBLE PRECISION`
- **Dates** → `DATE` ou `TIMESTAMP`
- **Text** → `TEXT` ou `VARCHAR`
- **Booleans** → `BOOLEAN`

Pas besoin de définir le schéma SQL manuellement !

## Normalisation des colonnes

Les noms de colonnes sont automatiquement normalisés:

```python
# Avant
"Date de création", "Montant (€)", "Statut validé"

# Après
"date_de_creation", "montant_eur", "statut_valide"
```

## Sensors

### csv_file_watcher

Surveille `/app/data/csv_inbox/` et déclenche l'ingestion automatiquement.

**Activation:**
```
Dagster UI → Automation → Sensors → csv_file_watcher → Start
```

**Fréquence:** Toutes les 60 secondes

### csv_archive_cleaner

Archive les CSVs traités (optionnel).

## Troubleshooting

### Asset n'apparaît pas dans Dagster UI

**Cause:** Config YAML non chargée

**Solution:**
```bash
# Redémarrer le worker
docker compose restart dlt_worker dagster_webserver
```

### Erreur "No matching config"

**Cause:** Nom du CSV ne correspond pas au pattern

**Solution:**
- CSV: `piezometers_2024.csv`
- Config: `piezometers.yml` avec `file_pattern: "piezometers*.csv"`

### Erreur "Schema evolution"

**Cause:** Structure du CSV a changé

**Solution:**
- Utiliser `write_disposition: replace` pour full refresh
- Ou supprimer la table manuellement: `DROP TABLE staging.staging_<table>;`

## Avantages vs ancien système

| Aspect | Ancien (piezometers_csv.py) | Nouveau (config-driven) |
|--------|----------------------------|-------------------------|
| Code par CSV | ✗ Asset + Job + SQL schema | ✓ 1 fichier YAML |
| Temps setup | ✗ 30-60 minutes | ✓ 1 minute |
| Maintenance | ✗ Schéma SQL manuel | ✓ Auto-détection DLT |
| Scalabilité | ✗ 1 asset = 1 CSV | ✓ 1 asset = N CSVs |
| Consistency | ✗ Pattern différent Hub'Eau | ✓ Même pattern DLT |
| State tracking | ✗ Aucun | ✓ DLT state management |

## Next Steps

1. Créer vos configs YAML pour vos CSVs
2. Activer le sensor `csv_file_watcher`
3. Créer des modèles DBT pour transformer staging → production
4. Supprimer les anciens assets CSV manuels (piezometers_csv.py)
