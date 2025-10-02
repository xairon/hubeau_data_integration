# 🔄 Migration vers l'Architecture dlt

## 📋 Résumé de la Migration

Nous avons migré de l'architecture Python custom vers **dlt (Data Load Tool)** pour l'ingestion des APIs Hub'Eau.

### 🗂️ Organisation des Branches

- **`main`** : Nouvelle architecture avec dlt
- **`old`** : Ancienne architecture Python custom (sauvegardée)

---

## 🏗️ Nouvelle Architecture

### Structure des Fichiers

```
├── configs/hubeau/           # Configurations YAML par endpoint
│   └── hydrobio_taxons.yml
├── pipelines/dlt/            # Pipeline dlt générique
│   ├── hubeau_generic.py     # Pipeline principal
│   ├── http_client.py        # Client HTTP avec retry
│   ├── schema.py             # Validation des configs
│   ├── slicing.py            # Découpage temporel intelligent
│   └── state.py              # Gestion de l'état incrémental
├── dagster/assets/
│   └── dlt_assets.py         # Assets Dagster pour dlt
└── tests/                    # Tests de la nouvelle architecture
```

### 🎯 Avantages de dlt

- ✅ **Configurations YAML** : Plus de code Python custom
- ✅ **État incrémental** : Gestion automatique des bookmarks
- ✅ **Découpage intelligent** : Fallback automatique si troncature
- ✅ **Retry/Backoff** : Gestion robuste des erreurs HTTP
- ✅ **Format Parquet** : Stockage optimisé dans MinIO
- ✅ **Debug facile** : Tout en Python, pas de subprocess

---

## 🔧 Configuration d'un Endpoint

### Exemple : `configs/hubeau/hydrobio_taxons.yml`

```yaml
name: hydrobio_taxons
source: hubeau
dataset_name: hubeau
base_url: https://hubeau.eaufrance.fr/api/v1
path: /hydrobio/taxons
method: GET
params_default:
  format: json
  size: 500
records_path: $.data
primary_keys: [id_taxon, code_station, date_prelevement]
replication_key: date_prelevement
pagination:
  type: page
  page_param: page
  page_size_param: size
  page_size: 500
  until_expr: "len($.data) < 500"
slicer:
  mode: datetime
  start_param: date_debut_prelevement
  end_param: date_fin_prelevement
  window_days: 1
  start_date: 2023-01-01
  end_offset_days: 1
fallbacks:
  truncation_threshold: 20000
  split_chain: [day, station_month]
```

### 📝 Paramètres Clés

- **`replication_key`** : Champ temporel pour l'incrémental
- **`window_days`** : Taille de la fenêtre temporelle
- **`truncation_threshold`** : Seuil de troncature (ex: 20k)
- **`split_chain`** : Stratégies de fallback si troncature

---

## 🚀 Utilisation

### 1. Test de l'Architecture

```bash
# Tester la nouvelle architecture
python scripts/test_dlt_architecture.py
```

### 2. Lancement via Dagster

```python
# Dans Dagster UI, lancer le job
# dagster/jobs.py contient les jobs dlt
```

### 3. Ajout d'un Nouvel Endpoint

1. **Créer la config YAML** dans `configs/hubeau/`
2. **Ajouter l'asset** dans `dagster/assets/dlt_assets.py`
3. **Tester** avec le script de test
4. **Déployer** via Dagster

---

## 🔄 Migration des Endpoints Existants

### APIs à Migrer

| API | Status | Config File | Notes |
|-----|--------|-------------|-------|
| **Hydrobiologie** | ✅ Migré | `hydrobio_taxons.yml` | Testé |
| **Hydrométrie** | 🔄 À migrer | `hydrometry.yml` | En cours |
| **Piézométrie** | 🔄 À migrer | `piezometry.yml` | En cours |
| **Qualité Cours d'Eau** | 🔄 À migrer | `quality_rivers.yml` | En cours |
| **Qualité Nappes** | 🔄 À migrer | `quality_groundwater.yml` | En cours |
| **Écoulement** | 🔄 À migrer | `ecoulement.yml` | En cours |
| **Prélèvements** | 🔄 À migrer | `prelevements.yml` | En cours |
| **Température** | 🔄 À migrer | `temperature.yml` | En cours |

### 📋 Checklist de Migration

Pour chaque endpoint :

- [ ] **Analyser** l'ancienne config (`hubeau_configs.py`)
- [ ] **Créer** la config YAML (`configs/hubeau/`)
- [ ] **Tester** avec le script de test
- [ ] **Valider** les données ingérées
- [ ] **Comparer** avec l'ancienne version
- [ ] **Déployer** via Dagster
- [ ] **Documenter** les spécificités

---

## 🧪 Tests

### Tests Disponibles

```bash
# Tests unitaires
pytest tests/test_hubeau_generic_utils.py
pytest tests/test_slicing.py
pytest tests/test_http_retry.py

# Test end-to-end
pytest tests/test_end_to_end_small.py
```

### Tests à Ajouter

- [ ] Tests de configuration YAML
- [ ] Tests de fallback temporel
- [ ] Tests de gestion d'état
- [ ] Tests de performance

---

## 📊 Monitoring

### Métriques Dagster

- **Lignes ingérées** par endpoint
- **Durée d'exécution** par fenêtre
- **Taux d'erreur** HTTP (4xx/5xx)
- **Utilisation** des fallbacks

### Logs

- **dlt** : Logs détaillés de l'ingestion
- **Dagster** : Métadonnées et métriques
- **MinIO** : Fichiers Parquet générés

---

## 🔧 Dépannage

### Problèmes Courants

1. **Erreur de configuration YAML**
   ```bash
   # Valider la config
   python -c "import yaml; yaml.safe_load(open('configs/hubeau/endpoint.yml'))"
   ```

2. **Erreur de connexion MinIO**
   ```bash
   # Vérifier les variables d'environnement
   echo $MINIO_USER $MINIO_PASS $MINIO_ENDPOINT
   ```

3. **Troncature détectée**
   ```yaml
   # Ajuster la fenêtre ou les fallbacks
   slicer:
     window_days: 1  # Réduire la fenêtre
   fallbacks:
     split_chain: [day, station_month]  # Ajouter des fallbacks
   ```

### Debug

```python
# Activer les logs détaillés
import logging
logging.basicConfig(level=logging.DEBUG)

# Tester un endpoint spécifique
from pipelines.dlt.hubeau_generic import run_pipeline
result = run_pipeline(config, credentials, debug=True)
```

---

## 📚 Ressources

- **Documentation dlt** : https://dlthub.com/docs
- **Configuration YAML** : Voir `configs/hubeau/hydrobio_taxons.yml`
- **Tests** : Voir `tests/test_*.py`
- **Ancienne architecture** : Branche `old`

---

## 🎯 Prochaines Étapes

1. **Migrer** tous les endpoints Hub'Eau
2. **Optimiser** les configurations (fenêtres, fallbacks)
3. **Ajouter** des tests complets
4. **Documenter** les spécificités par API
5. **Supprimer** l'ancienne architecture (branche `old`)

---

*Migration réalisée le : Janvier 2025*
