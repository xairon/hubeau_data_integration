# Hub'Eau Source V2 - Guide de Migration

## Vue d'ensemble

Ce document détaille la migration de la source Hub'Eau custom vers une **architecture hybride optimale** utilisant les primitives natives DLT tout en conservant la logique métier spécifique.

**Version**: 2.0
**Date**: 2025-01-14
**Auteur**: Claude (Assistant IA)

---

## Table des matières

1. [Changements principaux](#changements-principaux)
2. [Architecture V2](#architecture-v2)
3. [Format de configuration YAML](#format-de-configuration-yaml)
4. [Stratégies de slicing](#stratégies-de-slicing)
5. [Utilisation](#utilisation)
6. [Migration des endpoints](#migration-des-endpoints)
7. [Troubleshooting](#troubleshooting)

---

## Changements principaux

### Ce qui a changé

| Aspect | V1 (Ancien) | V2 (Nouveau) | Impact |
|--------|-------------|--------------|--------|
| **Fichier source** | `sources.py` (600 lignes) | `hubeau_source.py` (786 lignes) | ✅ Aucun |
| **Primitives DLT** | Partielles | 100% natives | ✅ Meilleures performances |
| **Format config** | Custom non documenté | Standard DLT étendu | ✅ Plus maintenable |
| **Code** | Monolithique | Modulaire (fonctions pures) | ✅ Plus testable |
| **Documentation** | Limitée | Complète | ✅ Meilleure DX |

### Ce qui n'a PAS changé

- ✅ **Assets Dagster**: Aucune modification nécessaire
- ✅ **Stratégies de slicing**: Toutes conservées (global, dept, station_month_chunked, datetime)
- ✅ **MinIO + Parquet**: Configuration identique
- ✅ **Incremental loading**: Fonctionne toujours
- ✅ **Stations data**: Extraction et filtrage identiques

---

## Architecture V2

```
┌──────────────────────────────────────────────────────────┐
│              hubeau_source.py (786 lignes)                │
│                                                            │
│  ┌────────────────────────────────────────────────────┐  │
│  │ UTILITAIRES                                        │  │
│  │ - load_hubeau_config()                             │  │
│  │ - create_hubeau_session()                          │  │
│  │ - create_hubeau_paginator()                        │  │
│  │ - create_hubeau_client() ← RESTClient natif DLT   │  │
│  │ - extract_records()                                │  │
│  │ - get_primary_keys()                               │  │
│  └────────────────────────────────────────────────────┘  │
│                                                            │
│  ┌────────────────────────────────────────────────────┐  │
│  │ STRATÉGIES DE SLICING                              │  │
│  │ - slice_global()          → Requête unique         │  │
│  │ - slice_by_department()   → 101 départements       │  │
│  │ - slice_by_station_month() → Chunks stations+mois  │  │
│  │ - slice_by_datetime()     → Périodes temporelles   │  │
│  └────────────────────────────────────────────────────┘  │
│                                                            │
│  ┌────────────────────────────────────────────────────┐  │
│  │ FACTORIES DE RESOURCES DLT                         │  │
│  │ - create_reference_resource()     (stations, etc.)  │  │
│  │ - create_chroniques_resource()    (incremental)     │  │
│  │ - create_observations_resource()  (analyses)        │  │
│  │ - create_generic_resource()       (fallback)        │  │
│  └────────────────────────────────────────────────────┘  │
│                                                            │
│  ┌────────────────────────────────────────────────────┐  │
│  │ SOURCE PRINCIPALE                                   │  │
│  │ @dlt.source(name="hubeau")                          │  │
│  │ def hubeau_rest_source(config_path, ...)           │  │
│  │   ↓ Routing intelligent par type d'endpoint        │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## Format de configuration YAML

### Structure standard

```yaml
# Metadata de la source DLT
source:
  name: piezometry                    # Nom de la source
  max_table_nesting: 2                # Niveau d'imbrication DLT

# Configuration de la resource DLT
resource:
  name: piezometry_stations           # Nom de la table
  endpoint: /stations                 # Endpoint API (relatif à base_url)
  base_url: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes
  primary_key: ["code_station"]       # Clé primaire (array)
  write_disposition: replace          # merge|append|replace

  # Schéma des colonnes (optionnel)
  columns:
    code_station:
      data_type: text
      nullable: false
      primary_key: true

# Configuration d'extraction (EXTENSION HUBEAU)
extraction:
  # Type de slicing
  slicing_mode: dept                  # global|dept|station_month_chunked|datetime

  # Pour slicing_mode=dept
  param: code_departement             # Nom du paramètre
  values: ["01", "02", ...]           # Valeurs à itérer

  # Pour slicing_mode=station_month_chunked
  station_param: code_bss             # Paramètre stations
  chunk_size: 80                      # Stations par chunk
  start_param: date_debut_mesure      # Paramètre date début
  end_param: date_fin_mesure          # Paramètre date fin

  # Pagination Hub'Eau
  pagination:
    type: page_number                 # Type de paginator
    total_path: last_page             # Chemin JSON total pages
    page_size: 5000                   # Taille de page

  # Paramètres par défaut
  default_params:
    format: json

  # Extraction records
  records_path: $.data                # JSONPath données

# Configuration temporelle (pour incremental)
temporal_filter:
  date_field: timestamp_mesure        # Champ pour incremental
  start_param: date_debut_mesure      # Paramètre API début
  end_param: date_fin_mesure          # Paramètre API fin
  start_date: "2020-01-01"            # Date initiale

# Performance
performance:
  retry_times: 3
  retry_delay: 2.0
  timeout: 30

# Destinations (référence)
destinations:
  filesystem:
    dataset_name: bronze
    bucket_url: s3://bronze
    file_format: parquet
```

---

## Stratégies de slicing

### 1. `global` - Requête unique

**Utilisation**: Endpoints avec peu de données

```yaml
extraction:
  slicing_mode: global
  pagination:
    page_size: 5000
```

**Comportement**:
- Une seule requête paginée
- Paramètres: `format=json&size=5000`
- Métadonnée ajoutée: `_slice_mode: 'global'`

---

### 2. `dept` - Découpage par département

**Utilisation**: Contourner limites API, parallélisation

```yaml
extraction:
  slicing_mode: dept
  param: code_departement
  values:
    - "01"
    - "02"
    # ... 101 départements
  pagination:
    page_size: 5000
```

**Comportement**:
- Itère sur 101 départements français
- Une requête paginée par département
- Métadonnée ajoutée: `_slice_dept: '01'`

---

### 3. `station_month_chunked` - Chunks stations + mois

**Utilisation**: Chroniques avec limite URL, incremental loading

```yaml
extraction:
  slicing_mode: station_month_chunked
  station_param: code_bss
  chunk_size: 80                      # 80 stations par requête
  start_param: date_debut_mesure
  end_param: date_fin_mesure
  pagination:
    page_size: 5000

temporal_filter:
  date_field: timestamp_mesure
  start_date: "2020-01-01"
```

**Comportement**:
- Nécessite `stations_data: Dict[code, List[months]]`
- Chunking: 80 stations par requête (limite URL ~2083 chars)
- Itère sur les mois actifs par chunk
- Incremental: skip automatique mois déjà chargés
- Métadonnées: `_chunk_month: '2024-03'`, `_chunk_stations: 75`

---

### 4. `datetime` - Périodes temporelles

**Utilisation**: Endpoints avec découpage temporel annuel/mensuel

```yaml
extraction:
  slicing_mode: datetime
  period_days: 30                     # Période par slice
  start_param: annee
  end_param: annee
  pagination:
    page_size: 5000

temporal_filter:
  start_date: "2020-01-01"
  start_param: annee
  end_param: annee
```

**Comportement**:
- Découpe la période en chunks de N jours
- Itère de start_date à aujourd'hui-1
- Métadonnées: `_slice_start: '2024-01-01'`, `_slice_end: '2024-01-31'`

---

## Utilisation

### Import et appel

```python
from dlt_pipeline.hubeau_source import hubeau_rest_source

# Asset Dagster
@asset(group_name="hubeau_piezometry")
def piezometry_stations_reference(context: AssetExecutionContext):
    source = hubeau_rest_source(
        config_path="configs/hubeau/piezometry_stations.yml",
        stations_data=None,           # Optionnel
        partition_date=None           # Optionnel
    )

    pipeline = dlt.pipeline(
        pipeline_name="hubeau_pipeline",
        destination=dlt.destinations.filesystem(
            bucket_url="s3://bronze",
            credentials={...}
        ),
        dataset_name="bronze"
    )

    load_info = pipeline.run(source)
    return {"status": "success"}
```

### Avec stations_data (chroniques)

```python
@asset(
    group_name="hubeau_piezometry",
    deps=[piezometry_stations_reference]  # Dépendance
)
def piezometry_chroniques(context: AssetExecutionContext):
    # Extraire stations depuis MinIO
    stations_data = extract_station_codes_from_minio(
        bucket="bronze",
        key="piezometry/piezometry_stations/*.parquet"
    )
    # stations_data = {"06255X0037/F": ["2023-01", "2023-02", ...], ...}

    source = hubeau_rest_source(
        config_path="configs/hubeau/piezometry_chroniques.yml",
        stations_data=stations_data,  # ← Requis pour station_month_chunked
        partition_date=None
    )

    pipeline.run(source)
```

---

## Migration des endpoints

### Aucun changement nécessaire pour:

- ✅ **Assets Dagster**: Fonctionnent tel quel
- ✅ **Configs YAML**: Structure déjà compatible
- ✅ **MinIO credentials**: Identiques
- ✅ **Stations extraction**: Code inchangé

### Vérifications recommandées:

1. **Primary keys**: Vérifier que `primary_key` n'est pas vide
2. **Slicing mode**: Confirmer cohérence avec endpoint
3. **Temporal config**: Vérifier champs pour chroniques

---

## Troubleshooting

### Erreur: `Config must contain 'resource' section`

**Cause**: Fichier YAML mal formé

**Solution**:
```yaml
# Ajouter la section manquante
resource:
  name: my_resource
  endpoint: /my_endpoint
  base_url: https://...
```

---

### Erreur: `'NoneType' is not iterable` sur endpoint

**Cause**: `endpoint_name` est None (ancien bug résolu en V2)

**Solution**: Utiliser `hubeau_rest_source()` au lieu de l'ancienne source

---

### Chroniques ne chargent rien

**Cause**: `stations_data` non fourni pour `station_month_chunked`

**Solution**:
```python
# Extraire stations depuis référentiel
stations_data = extract_station_codes_from_minio(...)
source = hubeau_rest_source(..., stations_data=stations_data)
```

---

### Incremental loading ne fonctionne pas

**Cause**: `temporal_filter.date_field` incorrect

**Solution**:
```yaml
temporal_filter:
  date_field: timestamp_mesure  # ← Doit correspondre au champ dans les données
  start_date: "2020-01-01"
```

---

## Références

- **Fichier source**: `src/dlt_pipeline/hubeau_source.py`
- **Assets Dagster**: `src/hubeau_pipeline/assets/bronze/dlt_assets.py`
- **Configs**: `configs/hubeau/*.yml`
- **Tests**: À lancer manuellement après migration

---

## Support

Pour toute question ou problème:
1. Vérifier ce guide
2. Consulter les logs Dagster
3. Vérifier les fichiers Parquet dans MinIO
4. Tester avec un asset simple (stations) d'abord

---

**Version du guide**: 1.0
**Dernière mise à jour**: 2025-01-14
