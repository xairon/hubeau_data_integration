# Modes d'Ingestion Hub'Eau

## Vue d'Ensemble

Le pipeline Hub'Eau supporte 3 modes d'ingestion pour gérer différents cas d'usage :

| Mode | Description | Usage | Partition |
|------|-------------|-------|-----------|
| **FULL** | Remplace toutes les données | Installation initiale, refresh complet | `full` |
| **YEAR** | Charge une année spécifique | Backfill ciblé, correction données | `2020` à `2025` |
| **INCREMENTAL** | Charge les nouvelles données depuis dernière exécution | MAJ quotidienne automatique | *(sans partition)* |

## 1. Mode FULL (Assets Stations)

### Comportement

- Remplace **toutes** les données à chaque exécution
- Utilise `write_disposition="replace"` dans DLT
- Pas de partitions
- Toujours le même résultat

### Assets Concernés

**Stations/Référentiels (11 assets)** :
- `piezometry_stations_raw`
- `hydrometry_sites_raw`
- `hydrometry_stations_raw`
- `quality_rivers_stations_raw`
- `quality_groundwater_stations_raw`
- `temperature_stations_raw`
- `hydrobio_stations_raw`
- `ecoulement_stations_raw`
- `ecoulement_campagnes_raw`
- `prelevements_ouvrages_raw`
- `prelevements_points_raw`

### Usage

```bash
# Dans Dagster UI
Assets → Sélectionner asset → Materialize
```

Pas de configuration de partition nécessaire.

## 2. Mode YEAR (Assets Chroniques Partitioned)

### Comportement

- Charge **une année spécifique** de données
- Supprime les données existantes de cette année avant chargement (idempotence)
- Utilise les partitions Dagster : `full`, `2020`, `2021`, `2022`, `2023`, `2024`, `2025`
- Filtrage API : `date_debut_mesure=YYYY-01-01&date_fin_mesure=YYYY-12-31`

### Partitions Disponibles

| Partition | Comportement |
|-----------|--------------|
| `full` | Charge **TOUT** l'historique (aucun filtre de date) |
| `2020` | Charge uniquement l'année 2020 |
| `2021` | Charge uniquement l'année 2021 |
| ... | ... |
| `2025` | Charge uniquement l'année 2025 |

### Assets Concernés

**Chroniques/Observations (11 assets)** :
- `piezometry_chroniques_raw`
- `hydrometry_obs_elab_raw`
- `quality_rivers_analyses_raw`
- `quality_rivers_conditions_raw`
- `quality_rivers_operations_raw`
- `quality_groundwater_analyses_raw`
- `temperature_chroniques_raw`
- `hydrobio_indices_raw`
- `hydrobio_taxons_raw`
- `ecoulement_observations_raw`
- `prelevements_chroniques_raw`

### Usage

#### Dagster UI

1. Aller dans **Assets**
2. Sélectionner un asset chronique (ex: `piezometry_chroniques_raw`)
3. Cliquer **Materialize**
4. Dans **Launchpad** :
   - Section **Partition**
   - Sélectionner `full`, `2020`, `2021`, etc.
5. Cliquer **Launch Run**

#### CLI

```bash
# Charge toute l'historique (partition "full")
dagster asset materialize \
  -m hubeau_pipeline \
  --select piezometry_chroniques_raw \
  --partition full

# Charge une année spécifique
dagster asset materialize \
  -m hubeau_pipeline \
  --select piezometry_chroniques_raw \
  --partition 2024
```

### Exemple : Backfill 2022-2024

```python
# Dans Dagster UI : Jobs → piezometry_chroniques_bronze
# Partition: Sélectionner 2022, 2023, 2024 (multi-sélection)
# Launch Run
```

Cela va exécuter 3 runs séparés, un par année.

## 3. Mode INCREMENTAL (Assets Chroniques Sans Partition)

### Comportement

- Charge **uniquement les nouvelles données** depuis la dernière exécution
- DLT gère automatiquement la date de dernière exécution via `incremental()`
- Filtrage API : `date_debut_mesure=LAST_DATE`
- Utilise `write_disposition="merge"` (UPSERT)

### Usage

```bash
# Dans Dagster UI
# Ne pas sélectionner de partition !
Assets → piezometry_chroniques_raw → Materialize (sans partition)
```

### Comment DLT Gère l'État

DLT stocke la dernière valeur de `replication_key` dans la table `_dlt_pipeline_state` :

```sql
SELECT * FROM staging._dlt_pipeline_state
WHERE pipeline_name = 'hubeau_piezometry_chroniques';
```

Au prochain run, DLT lit cette valeur et filtre : `date_debut_mesure >= last_date`.

### Configuration Automatique

Aucune configuration manuelle nécessaire. DLT détecte automatiquement :

1. **Premier run** : Charge les 30 derniers jours par défaut
2. **Runs suivants** : Charge depuis la dernière date

### Exemple : MAJ Quotidienne

```yaml
# Schedule Dagster (optionnel)
@schedule(
  job=piezometry_chroniques_job,
  cron_schedule="0 2 * * *"  # 2h du matin
)
```

## Comparaison des Modes

| Aspect | FULL | YEAR | INCREMENTAL |
|--------|------|------|-------------|
| **Données chargées** | Toutes | Année spécifique | Nouvelles uniquement |
| **Durée exécution** | Courte (stations) | Variable | Rapide |
| **Idempotence** | Oui (replace) | Oui (delete + insert) | Oui (merge) |
| **État DLT** | Non | Non | Oui |
| **Filtrage API** | Aucun | Date début/fin | Date depuis |
| **Use case** | Refresh référentiels | Backfill, correction | Quotidien |

## Recommandations

### Installation Initiale

1. **Charger les stations** (FULL) :
   ```bash
   # Job: all_stations_bronze
   # Pas de partition
   ```

2. **Charger l'historique complet** (YEAR partition `full`) :
   ```bash
   # Job: all_chroniques_bronze
   # Partition: full
   ```

### Maintenance Quotidienne

**Option A : Incremental automatique** (recommandé)
```yaml
# Schedule pour chaque API
cron: "0 2 * * *"  # 2h du matin
partition: null    # Pas de partition = incremental
```

**Option B : Year partition manuelle**
```bash
# Charger année courante
partition: 2024
```

### Correction de Données

**Backfill une année :**
```bash
# Job: piezometry_chroniques_bronze
# Partition: 2023
```

**Recharger tout :**
```bash
# 1. Stations
Job: all_stations_bronze (pas de partition)

# 2. Chroniques
Job: all_chroniques_bronze
Partition: full
```

## Troubleshooting

### "No data loaded" en mode INCREMENTAL

**Cause** : Aucune nouvelle donnée depuis le dernier run.

**Solution** : Normal si API Hub'Eau n'a pas de nouvelles données.

### Duplicate key errors en mode YEAR

**Cause** : Deletion échouée avant insertion.

**Solution** : Vérifier la fonction `delete_year_data()` dans `utils/db_helpers.py`.

### INCREMENTAL charge trop de données

**Cause** : État DLT perdu (table `_dlt_pipeline_state` vidée).

**Solution** : DLT va recharger les 30 derniers jours par défaut. Normal.

### YEAR partition "full" prend trop de temps

**Cause** : Hub'Eau a beaucoup de données historiques.

**Solution** :
- Utiliser partitions par année : `2020`, `2021`, etc.
- Exécuter en parallèle (backfill Dagster)

## Configuration Avancée

### Personnaliser la Période Incremental

Modifier le `lookback_days` dans le source DLT :

```python
# src/hubeau_pipeline/sources/hubeau_csv_source.py
def hubeau_chroniques_incremental(config, last_date, lookback_days=30):
    # ...
```

### Ajouter une Partition

Modifier `MODE_PARTITIONS` dans `assets/bronze/dlt_assets.py` :

```python
MODE_PARTITIONS = StaticPartitionsDefinition([
    "full",
    "2020", "2021", "2022", "2023", "2024", "2025", "2026"  # Ajouter 2026
])
```

## Références

- [DLT Incremental Loading](https://dlthub.com/docs/general-usage/incremental-loading)
- [Dagster Partitions](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions)
