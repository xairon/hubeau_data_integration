# Tutoriel DLT - Comprendre les Fichiers de Configuration

## 🎯 Introduction

Ce tutoriel explique comment comprendre et modifier les fichiers de configuration DLT pour les APIs Hub'Eau. Chaque API a son fichier YAML dans `configs/hubeau/` qui définit comment récupérer et traiter les données.

## 📁 Structure d'un Fichier de Configuration

### Exemple : `temperature_chroniques.yml`

```yaml
# === IDENTIFICATION ===
name: temperature_chroniques          # Nom unique du pipeline
source: hubeau                        # Source DLT (toujours "hubeau")
dataset_name: hubeau                  # Dataset de destination

# === ENDPOINT API ===
base_url: https://hubeau.eaufrance.fr/api/v1/temperature
path: /chronique                      # Chemin spécifique de l'API
method: GET                           # Méthode HTTP

# === PARAMÈTRES PAR DÉFAUT ===
params_default:
  format: json                        # Format de réponse
  size: 20000                         # Taille de page optimisée

# === EXTRACTION DES DONNÉES ===
records_path: $.data                  # JSONPath vers les records
primary_keys: [code_station, date_mesure_temp]  # Clés primaires
replication_key: date_mesure_temp     # Clé de réplication (pour incrémental)

# === PAGINATION ===
pagination:
  type: page                          # Type: "page" ou "cursor"
  page_param: page                    # Paramètre de page
  page_size_param: size               # Paramètre de taille
  page_size: 20000                    # Taille de page
  until_expr: "len($.data) == 0"     # Condition d'arrêt

# === STRATÉGIE DE DÉCOUPAGE ===
slicer:
  mode: dept_datetime                 # Mode de slicing
  start_param: date_debut_mesure      # Paramètre date début
  end_param: date_fin_mesure          # Paramètre date fin
  window_days: 30                     # Fenêtre temporelle
  start_date: "{{ partition_date }}"  # Date de début (template Dagster)
  end_date: "2024-12-31"              # Date de fin fixe
  dept_param: code_departement        # Paramètre département
  dept_chunk_size: 5                  # Taille des chunks
  dept_list: ["01", "02", "03", ...]  # Liste des départements

# === FALLBACKS AUTOMATIQUES ===
fallbacks:
  truncation_threshold: 20000         # Seuil de troncature
  split_chain: [station_month]       # Chaîne de fallback

# === RATE LIMITING ===
rate_limit:
  target_rps: 0.7                     # Requêtes par seconde
  max_concurrency: 1                  # Concurrence maximale

# === TIMEOUTS ET RETRY ===
timeout: 60                           # Timeout par requête
backoff_initial: 2.0                  # Délai initial de retry
backoff_max: 120.0                    # Délai maximum de retry
```

## 🔧 Comprendre Chaque Section

### 1. Identification
```yaml
name: temperature_chroniques          # Nom unique dans Dagster
source: hubeau                        # Source DLT personnalisée
dataset_name: hubeau                  # Dataset MinIO
```
**Rôle :** Identifie le pipeline dans Dagster et MinIO.

### 2. Endpoint API
```yaml
base_url: https://hubeau.eaufrance.fr/api/v1/temperature
path: /chronique
method: GET
```
**Rôle :** Définit l'URL complète de l'API Hub'Eau.

### 3. Paramètres par Défaut
```yaml
params_default:
  format: json
  size: 20000
```
**Rôle :** Paramètres envoyés à chaque requête API.

### 4. Extraction des Données
```yaml
records_path: $.data                  # JSONPath vers les records
primary_keys: [code_station, date_mesure_temp]
replication_key: date_mesure_temp
```
**Rôle :**
- `records_path` : Chemin vers les données dans la réponse JSON
- `primary_keys` : Clés pour identifier de manière unique chaque record
- `replication_key` : Clé pour l'ingestion incrémentale

### 5. Pagination
```yaml
pagination:
  type: page                          # "page" ou "cursor"
  page_param: page                    # Nom du paramètre de page
  page_size_param: size               # Nom du paramètre de taille
  page_size: 20000                    # Taille de page
  until_expr: "len($.data) == 0"      # Condition d'arrêt
```
**Rôle :** Gère la pagination automatique des requêtes API.

### 6. Stratégie de Découpage (Slicing)
```yaml
slicer:
  mode: dept_datetime                 # Mode de slicing
  start_param: date_debut_mesure      # Paramètre API pour date début
  end_param: date_fin_mesure          # Paramètre API pour date fin
  window_days: 30                     # Fenêtre temporelle
  start_date: "{{ partition_date }}"  # Template Dagster
  end_date: "2024-12-31"              # Date de fin fixe
  dept_param: code_departement        # Paramètre département
  dept_chunk_size: 5                  # Départements par requête
  dept_list: ["01", "02", "03", ...]  # Liste complète
```
**Rôle :** Divise les requêtes en petits morceaux pour éviter les limites API.

### 7. Fallbacks Automatiques
```yaml
fallbacks:
  truncation_threshold: 20000         # Si > 20K records
  split_chain: [station_month]       # Fallback vers station×mois
```
**Rôle :** Si une requête dépasse le seuil, passe automatiquement au mode de fallback.

### 8. Rate Limiting
```yaml
rate_limit:
  target_rps: 0.7                     # Requêtes par seconde
  max_concurrency: 1                  # Concurrence maximale
```
**Rôle :** Respecte les limites de l'API Hub'Eau.

## 🎯 Modes de Slicing Expliqués

### 1. `global` - Requête Globale
```yaml
slicer:
  mode: global
```
**Usage :** APIs avec peu de données (< 20K records)
**Exemple :** Hydrobiologie taxons

### 2. `datetime` - Découpage Temporel
```yaml
slicer:
  mode: datetime
  start_param: date_debut_obs
  end_param: date_fin_obs
  window_days: 1                      # Fenêtre quotidienne
  start_date: "{{ partition_date }}"
  end_offset_days: 0
```
**Usage :** Données temps réel (hydrométrie, écoulement)
**Avantage :** Récupération quotidienne optimisée

### 3. `station_month` - Station × Mois
```yaml
slicer:
  mode: station_month
  start_param: date_debut_mesure
  end_param: date_fin_mesure
  window_days: 30
  stations_source: dagster_asset      # Utilise les stations filtrées
```
**Usage :** Données historiques par station
**Avantage :** Évite les limites API par station

### 4. `dept_datetime` - Département × Temps (Optimisé)
```yaml
slicer:
  mode: dept_datetime
  start_param: date_debut_mesure
  end_param: date_fin_mesure
  window_days: 30
  dept_param: code_departement
  dept_chunk_size: 5                  # 5 départements par requête
  dept_list: ["01", "02", "03", ...]
```
**Usage :** Données volumineuses (température)
**Avantage :** Équilibre performance/limites API

### 5. `day` - Découpage Quotidien
```yaml
slicer:
  mode: day
  start_param: date_debut_prelevement
  end_param: date_fin_prelevement
  window_days: 1
```
**Usage :** Données quotidiennes (qualité)
**Avantage :** Granularité fine

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

## 🛠️ Comment Modifier une Configuration

### 1. Identifier le Problème
```bash
# Regarder les logs Dagster
docker-compose logs dagster_daemon | grep "temperature_chroniques"

# Chercher les erreurs
grep -i "error\|failed\|truncation" logs
```

### 2. Modifier le Fichier YAML
```bash
# Éditer la configuration
nano configs/hubeau/temperature_chroniques.yml

# Exemple : Réduire la taille de page si erreur 400
params_default:
  size: 10000  # Au lieu de 20000
```

### 3. Tester la Configuration
```bash
# Test rapide avec petite taille
sed -i 's/size: 20000/size: 10/' configs/hubeau/temperature_chroniques.yml

# Exécuter le job
dagster job execute -j temperature_job
```

### 4. Déployer
```bash
# Commit et push
git add configs/hubeau/temperature_chroniques.yml
git commit -m "fix: Réduction taille page température"
git push gitlab main
```

## 🚨 Gestion des Erreurs Courantes

### Erreur 400 Bad Request
**Cause :** Limite API atteinte
**Solution :**
```yaml
# Réduire la taille de page
params_default:
  size: 10000  # Au lieu de 20000

# Ou activer le fallback plus tôt
fallbacks:
  truncation_threshold: 10000  # Au lieu de 20000
```

### Erreur 429 Too Many Requests
**Cause :** Rate limit dépassé
**Solution :**
```yaml
# Réduire le rate limit
rate_limit:
  target_rps: 0.5  # Au lieu de 0.7
```

### Timeout
**Cause :** Requête trop longue
**Solution :**
```yaml
# Augmenter le timeout
timeout: 120  # Au lieu de 60
```

### Troncature Détectée
**Cause :** Trop de données dans une requête
**Solution :**
```yaml
# Réduire la fenêtre temporelle
slicer:
  window_days: 15  # Au lieu de 30

# Ou réduire la taille des chunks
slicer:
  dept_chunk_size: 3  # Au lieu de 5
```

## 📊 Optimisations Performance

### 1. Pages de 20K Records
```yaml
params_default:
  size: 20000
pagination:
  page_size: 20000
```
**Avantage :** 4x moins de pagination qu'avec 5K

### 2. Filtrage Intelligent des Stations
Le système filtre automatiquement les stations actives :
- **Test API** : Requête test pour identifier les stations avec données
- **Filtrage** : Seules les stations actives sont traitées
- **Optimisation** : Évite les requêtes inutiles (0 records)

### 3. Rate Limiting Adaptatif
```yaml
# Température (API sensible)
rate_limit:
  target_rps: 0.7

# Hydrométrie (API robuste)
rate_limit:
  target_rps: 2.0
```

## 🔍 Debugging et Monitoring

### Logs DLT
```bash
# Logs en temps réel
docker-compose logs -f dagster_daemon

# Logs spécifiques température
docker-compose logs dagster_daemon | grep temperature_chroniques
```

### Métriques Clés
- **Slices traités** : Progression des découpages
- **Records récupérés** : Volume de données par slice
- **Requêtes API** : Nombre et durée des appels
- **Fallbacks** : Détection des troncatures

### Test de Configuration
```python
# Test d'une configuration DLT
from pipelines.dlt.hubeau_generic import test_config
test_config('configs/hubeau/temperature_chroniques.yml')
```

## 📚 Ressources

- **[Schémas d'Architecture](ARCHITECTURE_SCHEMAS.md)** : Diagrammes Mermaid
- **[APIs Hub'Eau](APIS_HUBEAU_COMPLETE.md)** : Documentation des APIs
- **[Documentation Hub'Eau](https://hubeau.eaufrance.fr/page/api)** : Documentation officielle
- **[Configuration Examples](../configs/hubeau/)** : Exemples concrets
