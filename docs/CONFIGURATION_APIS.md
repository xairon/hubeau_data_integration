# Configuration des APIs Hub'Eau - Guide Complet

## 📋 Vue d'Ensemble

Chaque API Hub'Eau a son fichier de configuration YAML dans `configs/hubeau/`. Ces fichiers définissent :
- **Endpoints** et paramètres API
- **Stratégies de slicing** optimisées
- **Fallbacks** automatiques
- **Rate limiting** adaptatif

## 🗂️ Structure des Configurations

```
configs/hubeau/
├── temperature_chroniques.yml      # Température des cours d'eau
├── temperature_stations.yml       # Stations température
├── hydrometry_observations.yml    # Observations hydrométrie temps réel
├── hydrometry_stations.yml        # Stations hydrométrie
├── piezometry_chroniques.yml      # Chroniques piézométrie
├── piezometry_stations.yml        # Stations piézométrie
├── quality_rivers_analyses.yml    # Analyses qualité cours d'eau
├── quality_rivers_stations.yml    # Stations qualité cours d'eau
├── quality_groundwater_analyses.yml # Analyses qualité eaux souterraines
├── quality_groundwater_stations.yml # Stations qualité eaux souterraines
├── ecoulement_observations.yml    # Observations écoulement (ONDE)
├── ecoulement_stations.yml        # Stations écoulement
├── hydrobio_indices.yml          # Indices hydrobiologie
├── hydrobio_taxons.yml           # Taxons hydrobiologie
├── hydrobio_stations.yml         # Stations hydrobiologie
├── prelevements_chroniques.yml   # Chroniques prélèvements
└── prelevements_stations.yml     # Stations prélèvements
```

## 🌡️ Température des Cours d'Eau

### Configuration : `temperature_chroniques.yml`

```yaml
name: temperature_chroniques
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/temperature
path: /chronique
method: GET

# Paramètres optimisés
params_default:
  format: json
  size: 20000  # Pages de 20K records (optimisé)

# Clés de données
records_path: $.data
primary_keys: [code_station, date_mesure_temp]
replication_key: date_mesure_temp

# Pagination
pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"

# Stratégie optimisée : département × temps
slicer:
  mode: dept_datetime
  start_param: date_debut_mesure
  end_param: date_fin_mesure
  window_days: 30
  start_date: "{{ partition_date }}"
  end_date: "2024-12-31"  # Limite à l'année de partition
  dept_param: code_departement
  dept_chunk_size: 5  # 5 départements par requête
  dept_list: ["01", "02", "03", ..., "976"]  # 101 départements

# Fallback automatique
fallbacks:
  truncation_threshold: 20000
  split_chain: [station_month]

# Rate limiting respectueux
rate_limit:
  target_rps: 0.7
  max_concurrency: 1

timeout: 60
backoff_initial: 2.0
backoff_max: 120.0
```

**Spécificités :**
- **Volume** : ~200,000 observations/an
- **Stations** : ~760 total, ~50 actives
- **Optimisation** : 21 chunks × 12 mois = 252 requêtes (vs 9,120 ancien code)
- **Performance** : 38x moins de requêtes

### Configuration : `temperature_stations.yml`

```yaml
name: temperature_stations
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/temperature
path: /station
method: GET

params_default:
  format: json
  size: 1000

records_path: $.data
primary_keys: [code_station]

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 1000
  until_expr: "len($.data) == 0"

# Pas de slicing pour les stations (données de référence)
slicer:
  mode: global

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

## 💧 Hydrométrie

### Configuration : `hydrometry_observations.yml`

```yaml
name: hydrometry_observations
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v2/hydrometrie
path: /observations_tr
method: GET

params_default:
  format: json
  size: 20000

records_path: $.data
primary_keys: [code_station, date_obs, grandeur_hydro]
replication_key: date_obs

# Pagination par curseur (API v2)
pagination:
  type: cursor
  cursor_param: cursor
  cursor_path: $.next
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"

# Découpage quotidien (temps réel)
slicer:
  mode: datetime
  start_param: date_debut_obs
  end_param: date_fin_obs
  window_days: 1
  start_date: "{{ partition_date }}"
  end_offset_days: 0

fallbacks:
  truncation_threshold: 20000
  split_chain: [dept_datetime]

rate_limit:
  target_rps: 2.0
  max_concurrency: 1
```

**Spécificités :**
- **API v2** : Pagination par curseur
- **Temps réel** : 30 derniers jours uniquement
- **Volume** : ~50,000 observations/jour
- **Fréquence** : 15min à 1h selon la station

### Configuration : `hydrometry_stations.yml`

```yaml
name: hydrometry_stations
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/hydrometrie
path: /stations
method: GET

params_default:
  format: json
  size: 1000

records_path: $.data
primary_keys: [code_station]

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 1000
  until_expr: "len($.data) == 0"

slicer:
  mode: global

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

## 🏔️ Piézométrie

### Configuration : `piezometry_chroniques.yml`

```yaml
name: piezometry_chroniques
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes
path: /chroniques
method: GET

params_default:
  format: json
  size: 20000

records_path: $.data
primary_keys: [code_bss, date_mesure]  # Note: code_bss (pas code_station)
replication_key: date_mesure

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"

# Station par station (données historiques)
slicer:
  mode: station_month
  start_param: date_debut_mesure
  end_param: date_fin_mesure
  window_days: 30
  stations_source: dagster_asset  # Utilise les stations filtrées

fallbacks:
  truncation_threshold: 20000
  split_chain: [day]

rate_limit:
  target_rps: 1.5
  max_concurrency: 1
```

**Spécificités :**
- **Clé primaire** : `code_bss` (Bureau de Recherches Géologiques et Minières)
- **Volume** : ~30M records historiques
- **Stations** : ~20,000 stations
- **Stratégie** : Station par station pour éviter les limites API

### Configuration : `piezometry_stations.yml`

```yaml
name: piezometry_stations
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes
path: /stations
method: GET

params_default:
  format: json
  size: 1000

records_path: $.data
primary_keys: [code_bss]  # Note: code_bss

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 1000
  until_expr: "len($.data) == 0"

slicer:
  mode: global

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

## 🌊 Qualité des Cours d'Eau

### Configuration : `quality_rivers_analyses.yml`

```yaml
name: quality_rivers_analyses
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/qualite_rivieres
path: /analyses
method: GET

params_default:
  format: json
  size: 20000

records_path: $.data
primary_keys: [code_station, date_prelevement, code_parametre]
replication_key: date_prelevement

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"

# Découpage quotidien
slicer:
  mode: day
  start_param: date_debut_prelevement
  end_param: date_fin_prelevement
  window_days: 1
  start_date: "{{ partition_date }}"
  end_offset_days: 0

fallbacks:
  truncation_threshold: 20000
  split_chain: [station_month]

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

**Spécificités :**
- **Volume** : ~5M analyses
- **Paramètres** : ~200 paramètres mesurés
- **Fréquence** : Analyses ponctuelles
- **Stratégie** : Découpage quotidien pour granularité fine

### Configuration : `quality_rivers_stations.yml`

```yaml
name: quality_rivers_stations
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/qualite_rivieres
path: /station_pc
method: GET

params_default:
  format: json
  size: 1000

records_path: $.data
primary_keys: [code_station]

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 1000
  until_expr: "len($.data) == 0"

slicer:
  mode: global

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

## 🏔️ Qualité des Eaux Souterraines

### Configuration : `quality_groundwater_analyses.yml`

```yaml
name: quality_groundwater_analyses
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/qualite_nappes
path: /analyses
method: GET

params_default:
  format: json
  size: 20000

records_path: $.data
primary_keys: [code_bss, date_prelevement, code_parametre]  # Note: code_bss
replication_key: date_prelevement

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"

# Découpage quotidien
slicer:
  mode: day
  start_param: date_debut_prelevement
  end_param: date_fin_prelevement
  window_days: 1
  start_date: "{{ partition_date }}"
  end_offset_days: 0

fallbacks:
  truncation_threshold: 20000
  split_chain: [station_month]

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

**Spécificités :**
- **Volume** : ~2M analyses
- **Stations** : ~15,000 stations (code_bss)
- **Paramètres** : ~150 paramètres mesurés
- **Stratégie** : Identique à la qualité cours d'eau

### Configuration : `quality_groundwater_stations.yml`

```yaml
name: quality_groundwater_stations
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/qualite_nappes
path: /stations
method: GET

params_default:
  format: json
  size: 1000

records_path: $.data
primary_keys: [code_bss]  # Note: code_bss

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 1000
  until_expr: "len($.data) == 0"

slicer:
  mode: global

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

## 🌊 Écoulement (ONDE)

### Configuration : `ecoulement_observations.yml`

```yaml
name: ecoulement_observations
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/ecoulement
path: /observations
method: GET

params_default:
  format: json
  size: 20000

records_path: $.data
primary_keys: [code_station, date_observation_min, date_observation_max]
replication_key: date_observation_min

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"

# Découpage quotidien (temps réel)
slicer:
  mode: datetime
  start_param: date_observation_min
  end_param: date_observation_max
  window_days: 1
  start_date: "{{ partition_date }}"
  end_offset_days: 0

fallbacks:
  truncation_threshold: 20000
  split_chain: [dept_datetime]

rate_limit:
  target_rps: 1.5
  max_concurrency: 1
```

**Spécificités :**
- **ONDE** : Observatoire National Des Étiages
- **Volume** : ~15,000 observations/an
- **Stations** : ~3,500 stations
- **Saisonnalité** : Généralement mai-octobre
- **Campagnes** : Observations de terrain

### Configuration : `ecoulement_stations.yml`

```yaml
name: ecoulement_stations
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/ecoulement
path: /stations
method: GET

params_default:
  format: json
  size: 1000

records_path: $.data
primary_keys: [code_station]

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 1000
  until_expr: "len($.data) == 0"

slicer:
  mode: global

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

## 🐟 Hydrobiologie

### Configuration : `hydrobio_indices.yml`

```yaml
name: hydrobio_indices
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/indicateurs_services
path: /indices
method: GET

params_default:
  format: json
  size: 20000

records_path: $.data
primary_keys: [code_station_hydrobio, date_campagne, code_indicateur]
replication_key: date_campagne

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"

# Requête globale (peu de données)
slicer:
  mode: global

fallbacks:
  truncation_threshold: 20000
  split_chain: [station_month]

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

**Spécificités :**
- **Volume** : ~500K indices
- **Stations** : ~2,000 stations hydrobiologiques
- **Indicateurs** : ~50 indicateurs biologiques
- **Campagnes** : Analyses saisonnières

### Configuration : `hydrobio_taxons.yml`

```yaml
name: hydrobio_taxons
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/indicateurs_services
path: /taxons
method: GET

params_default:
  format: json
  size: 20000

records_path: $.data
primary_keys: [code_station_hydrobio, date_campagne, code_taxon]
replication_key: date_campagne

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"

# Requête globale (peu de données)
slicer:
  mode: global

fallbacks:
  truncation_threshold: 20000
  split_chain: [station_month]

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

**Spécificités :**
- **Volume** : ~1M taxons
- **Taxons** : ~5,000 espèces identifiées
- **Groupes** : Macroinvertébrés, diatomées, poissons
- **Stratégie** : Requête globale (données limitées)

### Configuration : `hydrobio_stations.yml`

```yaml
name: hydrobio_stations
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/indicateurs_services
path: /stations_hydrobio
method: GET

params_default:
  format: json
  size: 1000

records_path: $.data
primary_keys: [code_station_hydrobio]  # Note: code_station_hydrobio

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 1000
  until_expr: "len($.data) == 0"

slicer:
  mode: global

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

## 🧪 Prélèvements

### Configuration : `prelevements_chroniques.yml`

```yaml
name: prelevements_chroniques
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/prelevements
path: /chroniques
method: GET

params_default:
  format: json
  size: 20000

records_path: $.data
primary_keys: [code_point_prelevement, date_prelevement]  # Note: code_point_prelevement
replication_key: date_prelevement

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"

# Station par station (données historiques)
slicer:
  mode: station_month
  start_param: date_debut_prelevement
  end_param: date_fin_prelevement
  window_days: 30
  stations_source: dagster_asset

fallbacks:
  truncation_threshold: 20000
  split_chain: [day]

rate_limit:
  target_rps: 1.5
  max_concurrency: 1
```

**Spécificités :**
- **Volume** : ~20M prélèvements
- **Points** : ~50,000 points de prélèvement
- **Fréquence** : Prélèvements ponctuels
- **Stratégie** : Station par station pour éviter les limites

### Configuration : `prelevements_stations.yml`

```yaml
name: prelevements_stations
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/prelevements
path: /points_prelevement
method: GET

params_default:
  format: json
  size: 1000

records_path: $.data
primary_keys: [code_point_prelevement]  # Note: code_point_prelevement

pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 1000
  until_expr: "len($.data) == 0"

slicer:
  mode: global

rate_limit:
  target_rps: 1.0
  max_concurrency: 1
```

## 🔧 Paramètres Communs

### Rate Limiting par API

| API | target_rps | Raison |
|-----|------------|--------|
| **Température** | 0.7 | API très sensible |
| **Hydrométrie** | 2.0 | API robuste |
| **Piézométrie** | 1.5 | API modérée |
| **Qualité** | 1.0 | API standard |
| **Écoulement** | 1.5 | API modérée |
| **Hydrobiologie** | 1.0 | API standard |
| **Prélèvements** | 1.5 | API modérée |

### Timeouts et Retry

```yaml
timeout: 60                    # Timeout par requête
backoff_initial: 2.0          # Délai initial de retry
backoff_max: 120.0            # Délai maximum de retry
```

### Pagination

```yaml
# Page-based (APIs v1)
pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"

# Cursor-based (APIs v2)
pagination:
  type: cursor
  cursor_param: cursor
  cursor_path: $.next
  page_size_param: size
  page_size: 20000
  until_expr: "len($.data) == 0"
```

## 🎯 Optimisations par API

### Stratégies de Slicing

| API | Mode Principal | Fallback | Optimisation |
|-----|----------------|----------|--------------|
| **Température** | `dept_datetime` | `station_month` | 38x moins de requêtes |
| **Hydrométrie** | `datetime` | `dept_datetime` | Temps réel optimisé |
| **Piézométrie** | `station_month` | `day` | Évite les limites API |
| **Qualité** | `day` | `station_month` | Granularité fine |
| **Écoulement** | `datetime` | `dept_datetime` | Temps réel optimisé |
| **Hydrobiologie** | `global` | `station_month` | Données limitées |
| **Prélèvements** | `station_month` | `day` | Évite les limites API |

### Calculs de Performance

**Température (Optimisée) :**
- 101 départements ÷ 5 = 21 chunks
- 21 chunks × 12 mois = 252 requêtes
- vs ancien code : 760 stations × 12 mois = 9,120 requêtes
- **Gain :** 38x moins de requêtes

**Hydrométrie (Temps Réel) :**
- Fenêtre quotidienne
- ~30 requêtes par jour
- Optimisé pour données récentes

**Piézométrie (Historique) :**
- Station par station
- ~20,000 stations
- Fallback automatique si limite atteinte

## 🚨 Gestion des Erreurs

### Codes d'Erreur Courants

| Code | Erreur | Solution |
|------|--------|----------|
| **400** | Bad Request | Réduire `page_size` ou activer fallback |
| **429** | Too Many Requests | Réduire `target_rps` |
| **500** | Internal Server Error | Augmenter `timeout` |
| **Timeout** | Request Timeout | Augmenter `timeout` |

### Fallbacks Automatiques

```yaml
fallbacks:
  truncation_threshold: 20000
  split_chain: [station_month]
```

**Flux de Fallback :**
1. **Tentative** : Mode principal
2. **Si troncature** : Passage au mode de fallback
3. **Résultat** : Récupération complète garantie

## 🔍 Debugging

### Logs de Configuration

```bash
# Test d'une configuration
python -c "from pipelines.dlt.hubeau_generic import test_config; test_config('configs/hubeau/temperature_chroniques.yml')"
```

### Métriques Clés

- **Slices générés** : Nombre de découpages
- **Records par slice** : Volume de données
- **Durée des requêtes** : Performance API
- **Fallbacks déclenchés** : Détection des limites

### Test de Performance

```bash
# Test avec petite taille
sed -i 's/size: 20000/size: 10/' configs/hubeau/temperature_chroniques.yml
dagster job execute -j temperature_job
```

## 📚 Ressources

- **[Guide DLT Complet](GUIDE_DLT_COMPLET.md)** : Architecture et concepts
- **[Hub'Eau APIs](https://hubeau.eaufrance.fr/page/api)** : Documentation officielle
- **[Configuration Examples](../configs/hubeau/)** : Exemples concrets
- **[Architecture Moderne](ARCHITECTURE_MODERNE.md)** : Stack technique
