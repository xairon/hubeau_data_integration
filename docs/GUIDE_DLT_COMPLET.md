# Guide Complet DLT - Hub'Eau Data Integration

## 🎯 Qu'est-ce que DLT ?

**DLT (Data Load Tool)** est un framework Python moderne pour l'ingestion de données. Il automatise :
- **Pagination** : Gestion automatique des pages
- **Slicing** : Découpage intelligent des requêtes
- **Fallbacks** : Stratégies de récupération automatiques
- **Schema Evolution** : Adaptation automatique des schémas
- **Error Handling** : Gestion robuste des erreurs

## 🏗️ Architecture DLT dans notre Pipeline

```
Hub'Eau API → DLT Source → DLT Pipeline → MinIO (Bronze)
```

### Composants Principaux

1. **`hubeau_source`** : Source DLT personnalisée pour Hub'Eau
2. **`slicing.py`** : Logique de découpage des requêtes
3. **`http_client.py`** : Client HTTP avec retry et rate limiting
4. **Configuration YAML** : Définition des pipelines par API

## 📋 Structure des Fichiers de Configuration

### Format Général

```yaml
# configs/hubeau/[api_name].yml
name: api_name                    # Nom unique du pipeline
source: hubeau                    # Source DLT (hubeau_source)
dataset_name: hubeau              # Dataset de destination
base_url: https://hubeau.eaufrance.fr/api/v1/endpoint
path: /endpoint                   # Chemin API
method: GET                       # Méthode HTTP

# Paramètres par défaut
params_default:
  format: json
  size: 20000                     # Taille de page optimisée

# Extraction des données
records_path: $.data              # JSONPath vers les records
primary_keys: [key1, key2]        # Clés primaires
replication_key: date_field       # Clé de réplication

# Pagination
pagination:
  type: page                      # Type: page ou cursor
  page_param: page               # Paramètre de page
  page_size_param: size          # Paramètre de taille
  page_size: 20000               # Taille de page
  until_expr: "len($.data) == 0" # Condition d'arrêt

# Stratégie de découpage
slicer:
  mode: dept_datetime            # Mode de slicing
  start_param: date_debut        # Paramètre date début
  end_param: date_fin            # Paramètre date fin
  window_days: 30                # Fenêtre temporelle
  start_date: "{{ partition_date }}" # Date de début (template)
  end_date: "2024-12-31"         # Date de fin fixe
  dept_param: code_departement   # Paramètre département
  dept_chunk_size: 5             # Taille des chunks
  dept_list: ["01", "02", ...]   # Liste des départements

# Fallbacks automatiques
fallbacks:
  truncation_threshold: 20000     # Seuil de troncature
  split_chain: [station_month]   # Chaîne de fallback

# Rate limiting
rate_limit:
  target_rps: 0.7                # Requêtes par seconde
  max_concurrency: 1             # Concurrence maximale

# Timeouts et retry
timeout: 60
backoff_initial: 2.0
backoff_max: 120.0
```

## 🔧 Modes de Slicing DLT

### 1. **`global`** - Requête Globale
```yaml
slicer:
  mode: global
```
**Usage :** APIs avec peu de données (< 20K records)
**Exemple :** Hydrobiologie taxons

### 2. **`datetime`** - Découpage Temporel
```yaml
slicer:
  mode: datetime
  start_param: date_debut_obs
  end_param: date_fin_obs
  window_days: 1                 # Fenêtre quotidienne
  start_date: "{{ partition_date }}"
  end_offset_days: 0
```
**Usage :** Données temps réel (hydrométrie, écoulement)
**Avantage :** Récupération quotidienne optimisée

### 3. **`station_month`** - Station × Mois
```yaml
slicer:
  mode: station_month
  start_param: date_debut_mesure
  end_param: date_fin_mesure
  window_days: 30
  stations_source: dagster_asset  # Utilise les stations filtrées
```
**Usage :** Données historiques par station
**Avantage :** Évite les limites API par station

### 4. **`dept_datetime`** - Département × Temps (Optimisé)
```yaml
slicer:
  mode: dept_datetime
  start_param: date_debut_mesure
  end_param: date_fin_mesure
  window_days: 30
  dept_param: code_departement
  dept_chunk_size: 5             # 5 départements par requête
  dept_list: ["01", "02", "03", ...]
```
**Usage :** Données volumineuses (température)
**Avantage :** Équilibre performance/limites API

### 5. **`day`** - Découpage Quotidien
```yaml
slicer:
  mode: day
  start_param: date_debut_prelevement
  end_param: date_fin_prelevement
  window_days: 1
```
**Usage :** Données quotidiennes (qualité)
**Avantage :** Granularité fine

### 6. **`campaign`** - Par Campagne
```yaml
slicer:
  mode: campaign
  campaign_param: code_campagne
```
**Usage :** Données par campagne (hydrobiologie)
**Avantage :** Respecte la logique métier

## 🔄 Système de Fallbacks

### Principe
Si une requête dépasse le `truncation_threshold`, DLT passe automatiquement au mode de fallback suivant.

### Exemple Température
```yaml
fallbacks:
  truncation_threshold: 20000
  split_chain: [station_month]
```

**Flux :**
1. **Tentative** : `dept_datetime` (5 départements × 12 mois = 252 requêtes)
2. **Si troncature** : Fallback vers `station_month` (station par station × mois)
3. **Résultat** : Récupération complète même si limite API atteinte

### Chaînes de Fallback Recommandées

| API | Mode Principal | Fallback | Raison |
|-----|----------------|----------|--------|
| **Température** | `dept_datetime` | `station_month` | Optimisation vs garantie |
| **Hydrométrie** | `datetime` | `dept_datetime` | Temps réel vs historique |
| **Piézométrie** | `station_month` | `day` | Station vs temps |
| **Qualité** | `day` | `station_month` | Temps vs station |

## 📊 Optimisations Performance

### 1. **Pages de 20K Records**
```yaml
params_default:
  size: 20000
pagination:
  page_size: 20000
```
**Avantage :** 4x moins de pagination qu'avec 5K

### 2. **Filtrage Intelligent des Stations**
```python
# Dans dlt_assets.py
def _filter_active_stations_for_period(stations, partition_date, station_type):
    # Test API pour identifier les stations actives
    # Retourne seulement les stations avec des données
```

**Exemple Température :**
- **Stations totales** : 760
- **Stations actives 2024** : 1
- **Optimisation** : 99.9% de requêtes évitées

### 3. **Rate Limiting Adaptatif**
```yaml
rate_limit:
  target_rps: 0.7                # Température (API sensible)
  target_rps: 2.0                # Hydrométrie (API robuste)
  max_concurrency: 1             # Respect des limites Hub'Eau
```

## 🛠️ Configuration par API

### Température (Optimisée)
```yaml
# configs/hubeau/temperature_chroniques.yml
name: temperature_chroniques
base_url: https://hubeau.eaufrance.fr/api/v1/temperature
path: /chronique
params_default:
  size: 20000
primary_keys: [code_station, date_mesure_temp]
replication_key: date_mesure_temp
slicer:
  mode: dept_datetime
  window_days: 30
  dept_chunk_size: 5
  dept_list: ["01", "02", "03", ...]  # 101 départements
fallbacks:
  truncation_threshold: 20000
  split_chain: [station_month]
rate_limit:
  target_rps: 0.7
```

**Calcul des requêtes :**
- 101 départements ÷ 5 = ~21 chunks
- 21 chunks × 12 mois = ~252 requêtes
- vs ancien code : 760 stations × 12 mois = 9,120 requêtes
- **Optimisation :** 38x moins de requêtes

### Hydrométrie (Temps Réel)
```yaml
# configs/hubeau/hydrometry_observations.yml
name: hydrometry_observations
base_url: https://hubeau.eaufrance.fr/api/v2/hydrometrie
path: /observations_tr
params_default:
  size: 20000
primary_keys: [code_station, date_obs, grandeur_hydro]
replication_key: date_obs
pagination:
  type: cursor                    # Pagination par curseur
  cursor_param: cursor
  cursor_path: $.next
slicer:
  mode: datetime                  # Découpage quotidien
  window_days: 1
  start_date: "{{ partition_date }}"
  end_offset_days: 0
fallbacks:
  truncation_threshold: 20000
  split_chain: [dept_datetime]
rate_limit:
  target_rps: 2.0
```

**Spécificités :**
- **API v2** : Pagination par curseur
- **Temps réel** : Fenêtre quotidienne
- **Restriction** : 30 derniers jours uniquement

### Piézométrie (Historique)
```yaml
# configs/hubeau/piezometry_chroniques.yml
name: piezometry_chroniques
base_url: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes
path: /chroniques
params_default:
  size: 20000
primary_keys: [code_bss, date_mesure]
replication_key: date_mesure
slicer:
  mode: station_month             # Station par station
  window_days: 30
  stations_source: dagster_asset   # Utilise les stations filtrées
fallbacks:
  truncation_threshold: 20000
  split_chain: [day]             # Fallback vers découpage quotidien
rate_limit:
  target_rps: 1.5
```

**Spécificités :**
- **Clé primaire** : `code_bss` (pas `code_station`)
- **Mode** : `station_month` pour données historiques
- **Fallback** : `day` si limite atteinte

## 🔍 Debugging et Monitoring

### Logs DLT
```python
# Dans hubeau_generic.py
def dagster_print(*args, **kwargs):
    message = ' '.join(str(arg) for arg in args)
    context.log.info(f"DLT: {message}")
```

**Exemple de logs :**
```
DLT: 🚀 DLT: Démarrage ingestion temperature_chroniques - 252 slices à traiter
DLT: 📊 DLT: Configuration: https://hubeau.eaufrance.fr/api/v1/temperature/chronique
DLT: 📋 DLT: Slices générés (252):
DLT: 📦 DLT: 📦 Traitement slice 1/252: dept-01_02_03_04_05
DLT: 🌐 Requête 1: GET /chronique avec params: {'code_departement': '01,02,03,04,05', 'date_debut_mesure': '2024-01-01', 'date_fin_mesure': '2024-01-30'}
DLT: ✅ DLT: ✅ Requête 1 réussie: 5760 records en 2.48s
DLT: ✅ Slice 1/252 terminé: 5760 records en 1 requêtes
```

### Métriques Clés
- **Slices traités** : Progression (ex: 1/252)
- **Records par slice** : Volume de données
- **Durée des requêtes** : Performance API
- **Fallbacks déclenchés** : Détection des limites

### Test de Configuration
```python
# Test d'une configuration DLT
from pipelines.dlt.hubeau_generic import test_config
test_config('configs/hubeau/temperature_chroniques.yml')
```

## 🚨 Gestion des Erreurs

### Types d'Erreurs Courantes

1. **400 Bad Request** : Limite API atteinte
   - **Solution** : Réduire `page_size` ou activer fallback

2. **429 Too Many Requests** : Rate limit dépassé
   - **Solution** : Réduire `target_rps`

3. **Timeout** : Requête trop longue
   - **Solution** : Augmenter `timeout`

4. **DNS Resolution** : Problème réseau Docker
   - **Solution** : Vérifier la configuration réseau

### Configuration de Retry
```yaml
timeout: 60
backoff_initial: 2.0
backoff_max: 120.0
```

**Stratégie :**
- **Tentative 1** : Immédiate
- **Tentative 2** : Attendre 2s
- **Tentative 3** : Attendre 4s
- **Tentative 4** : Attendre 8s
- **Maximum** : 120s

## 🔧 Développement et Extension

### Ajouter une Nouvelle API

1. **Créer le fichier de config** :
```yaml
# configs/hubeau/nouvelle_api.yml
name: nouvelle_api
source: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1/nouvelle_api
path: /endpoint
# ... configuration complète
```

2. **Définir l'asset Dagster** :
```python
# Dans dlt_assets.py
@asset(group_name="hubeau_nouvelle_api", partitions_def=YEARLY_PARTITIONS, deps=[nouvelle_api_stations_reference])
def nouvelle_api_observations(context: AssetExecutionContext) -> Dict[str, Any]:
    partition_date = _get_partition_date_yearly(context)
    stations_data, _ = _setup_observation_asset(context, "nouvelle_api", partition_date)
    return ingest_dlt(context, "configs/hubeau/nouvelle_api.yml", stations_data=stations_data, partition_date=partition_date)
```

3. **Ajouter au job** :
```python
# Dans dlt_jobs.py
nouvelle_api_job = define_asset_job(
    name="nouvelle_api_job",
    selection=AssetSelection.assets(
        nouvelle_api_stations_reference,
        nouvelle_api_observations,
    ),
    description="Job pour la nouvelle API",
)
```

### Modifier une Configuration Existante

1. **Éditer le YAML** : Modifier les paramètres
2. **Tester** : `python pipelines/dlt/test_config.py`
3. **Déployer** : Commit et push
4. **Exécuter** : Via Dagster UI

### Debugging Avancé

```python
# Activer les logs détaillés
import logging
logging.getLogger("dlt").setLevel(logging.DEBUG)

# Test d'un slice spécifique
from pipelines.dlt.slicing import build_slices
slices = build_slices(config, stations_data=["station1", "station2"])
print(f"Slices générés: {len(slices)}")
```

## 📚 Ressources et Documentation

- **[DLT Documentation](https://dlthub.com/docs)** : Documentation officielle
- **[Hub'Eau APIs](https://hubeau.eaufrance.fr/page/api)** : Documentation des APIs
- **[Dagster Documentation](https://docs.dagster.io)** : Orchestration
- **[Configuration Examples](configs/hubeau/)** : Exemples concrets

## 🎯 Bonnes Pratiques

### 1. **Configuration**
- Toujours tester avec `size: 10` d'abord
- Utiliser des `window_days` appropriés (1 pour temps réel, 30 pour historique)
- Configurer des fallbacks robustes

### 2. **Performance**
- Optimiser `dept_chunk_size` selon les limites API
- Utiliser le filtrage des stations
- Monitorer les métriques de performance

### 3. **Maintenance**
- Surveiller les logs pour détecter les changements API
- Mettre à jour les configurations selon l'évolution des APIs
- Tester régulièrement les fallbacks

### 4. **Sécurité**
- Respecter les rate limits Hub'Eau
- Utiliser des timeouts appropriés
- Gérer les erreurs gracieusement
