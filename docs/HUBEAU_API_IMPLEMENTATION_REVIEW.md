# Revue d'Implémentation des APIs Hub'Eau

Ce document dresse une revue détaillée de l'implémentation actuelle des huit APIs Hub'Eau au sein du
pipeline. Il s'appuie sur le module `HubeauIngestionService` et sur les assets bronze déclarés dans
`src/hubeau_pipeline/assets/bronze`. 

**🎯 Status** : ✅ **OPÉRATIONNEL** - Pipeline avec fenêtres temporelles cohérentes, filtres spatiaux, cache MinIO et configuration centralisée.

## Synthèse globale

| API | Asset(s) Dagster | Status | Fonctionnalités |
| --- | ----------------- | ------------------- | ------------------- |
| Piézométrie | `hubeau_piezo_bronze_real` | ✅ **OPÉRATIONNEL** | Fenêtre temporelle cohérente, cache MinIO |
| Hydrométrie | `hubeau_hydro_bronze_real` | ✅ **OPÉRATIONNEL** | Pagination cursor, lookback_days=1 |
| Qualité eaux de surface | `hubeau_quality_surface_bronze_real` | ✅ **OPÉRATIONNEL** | Filtres spatiaux/temporels cohérents |
| Qualité eaux souterraines | `hubeau_quality_groundwater_bronze_real` | ✅ **OPÉRATIONNEL** | Configuration harmonisée |
| Température | `hubeau_temperature_bronze_real` | ✅ **OPÉRATIONNEL** | lookback_days=1 pour partitions quotidiennes |
| ONDE | `hubeau_onde_bronze_real` | ✅ **OPÉRATIONNEL** | Configuration optimisée |
| Hydrobiologie | `hubeau_hydrobiologie_bronze_real` | ✅ **OPÉRATIONNEL** | Fenêtre glissante 12 mois (indices, taxons) |
| Prélèvements | `hubeau_prelevements_bronze_real` | ✅ **OPÉRATIONNEL** | Filtre spatial obligatoire + health check |

## Fonctionnalités Clés

### **1. Fenêtres Temporelles Cohérentes**
- **lookback_days=1** pour toutes les APIs temporelles
- Cohérence avec partitions quotidiennes Dagster
- Évite fenêtres glissantes incohérentes

### **2. Filtres Spatiaux Obligatoires**
- **chroniques** (Prélèvements) : `code_departement` requis
- Garde-fou pour éviter volumes massifs
- Health check spécialisé pour `points_prelevement`

### **3. Cache MinIO Intelligent**
- Vérification données existantes avant appels API
- Évite redondance et optimise performance
- Mécanisme de fallback robuste

### **4. Configuration Centralisée**
- Configuration unifiée dans `hubeau_configs.py`
- Cohérence entre toutes les APIs
- Maintenance simplifiée

### **5. Logging Complet**
- URLs complètes pour debugging
- Estimations profondeur pour monitoring
- Garde-fous pour détection problèmes

## Évaluation détaillée par API

### 1. Piézométrie (`niveaux_nappes`)

* **Implementation** : `hubeau_piezo_bronze_real` utilise `HubeauIngestionService` avec les endpoints
  `stations` et `chroniques_tr` avec configuration optimisée.
* **Fonctionnalités** :
  - **lookback_days=1** : Cohérence avec partitions quotidiennes Dagster
  - **Cache MinIO** : Vérification données existantes avant appels API
  - **Logging complet** : URLs complètes et estimations profondeur
  - **Configuration centralisée** : Paramètres unifiés dans `hubeau_configs.py`
* **Points positifs** :
  - Déduplication quotidienne active pour `chroniques_tr`
  - Gestion robuste des erreurs et retry
  - Stockage MinIO avec métadonnées enrichies

### 2. Hydrométrie (`hydrometrie` v2)

* **Implementation** : `hubeau_hydro_bronze_real` interroge `referentiel/stations` et
  `observations_tr` avec configuration optimisée.
* **Fonctionnalités** :
  - **lookback_days=1** : Cohérence avec partitions quotidiennes Dagster
  - **Pagination cursor** : `depth_limit=None` pour éviter troncature
  - **Cache MinIO** : Évite appels redondants
  - **Configuration centralisée** : Paramètres harmonisés
* **Points positifs** :
  - Déduplication quotidienne active pour `observations_tr`
  - Support API v2 avec pagination cursor
  - Gestion robuste des erreurs

### 3. Qualité des eaux de surface (`qualite_rivieres` / `qualite_eau_surface`)

* **Implementation** : `hubeau_quality_surface_bronze_real` avec configuration optimisée.
* **Fonctionnalités** :
  - **lookback_days=1** : Cohérence avec partitions quotidiennes Dagster
  - **Filtres spatiaux/temporels cohérents** : Configuration harmonisée
  - **Cache MinIO** : Évite appels redondants
  - **Configuration centralisée** : Paramètres unifiés dans `hubeau_configs.py`

### 4. Qualité des eaux souterraines (`qualite_nappes`)

* **Implementation** : `hubeau_quality_groundwater_bronze_real` avec configuration optimisée.
* **Fonctionnalités** :
  - **lookback_days=1** : Cohérence avec partitions quotidiennes Dagster
  - **Configuration harmonisée** : Paramètres unifiés
  - **Cache MinIO** : Évite appels redondants
  - **Logging complet** : Observabilité complète

### 5. Température (`temperature`)

* **Implementation** : `hubeau_temperature_bronze_real` avec configuration optimisée.
* **Fonctionnalités** :
  - **lookback_days=1** : Cohérence avec partitions quotidiennes Dagster
  - **Cache MinIO** : Évite appels redondants
  - **Configuration centralisée** : Paramètres harmonisés
* **Points positifs** :
  - Déduplication quotidienne active pour `chronique`
  - Gestion robuste des erreurs

### 6. ONDE (`ecoulement`)

* **Implementation** : `hubeau_onde_bronze_real` avec configuration optimisée.
* **Fonctionnalités** :
  - **Configuration optimisée** : Paramètres harmonisés
  - **Cache MinIO** : Évite appels redondants
  - **Logging complet** : Observabilité complète
  - **Configuration centralisée** : Paramètres unifiés

### 7. Hydrobiologie (`hydrobiologie`)

* **Implementation** : `hubeau_hydrobiologie_bronze_real` avec configuration optimisée.
* **Fonctionnalités** :
  - **Fenêtre glissante 12 mois** : `indices` et `taxons` avec `date_debut_prelevement`, `date_fin_prelevement`
  - **Évite troncature 10k** : Configuration optimisée pour éviter limite Hub'Eau
  - **Cache MinIO** : Évite appels redondants
  - **Configuration centralisée** : Paramètres harmonisés

### 8. Prélèvements (`prelevements`)

* **Implementation** : `hubeau_prelevements_bronze_real` avec configuration optimisée.
* **Fonctionnalités** :
  - **Filtre spatial obligatoire** : `chroniques` avec `code_departement` requis
  - **Health check spécialisé** : `points_prelevement` avec test de santé par département (1-dept-per-call)
  - **Cache MinIO** : Évite appels redondants
  - **Configuration centralisée** : Paramètres unifiés

## Constats transverses

1. **Gestion de la pagination** : Configuration par endpoint dans `hubeau_configs.py` avec `max_page_size` et `depth_limit` spécifiques à chaque API.

2. **Filtres temporels** : Configuration centralisée avec `lookback_days=1` pour cohérence avec partitions quotidiennes Dagster.

3. **Déduplication** : Logique maintenue et optimisée pour endpoints temporels avec cache MinIO.

4. **Stockage MinIO** : Cache intelligent implémenté avec vérification données existantes et fallback robuste.

5. **Rejeu historique** : Partitions quotidiennes cohérentes permettent backfill Dagster standard.

6. **Référentiels complémentaires** : BDLISA et Sandre intégrés avec configurations optimisées.

## Recommandations

1. **Configuration `HubeauAPIConfig`** : Paramètres spécifiques par endpoint implémentés dans `hubeau_configs.py`.

2. **Stratégies d'échantillonnage** : Déduplication optimisée et cache MinIO pour éviter redondance.

3. **Filtres spatiaux** : Implémentés pour Prélèvements (`chroniques`) avec health check spécialisé.

4. **Gestion limites** : Cache MinIO et configuration par endpoint pour respecter quotas Hub'Eau.

5. **Plan de backfill** : Partitions quotidiennes cohérentes permettent backfill Dagster standard.

## Prochaines Étapes

1. **Tests en production** : Valider avec volumes réels sur 1-2 départements
2. **Monitoring** : Surveiller métriques performance et quotas Hub'Eau
3. **Optimisations** : Ajuster configurations selon retours terrain
4. **Documentation** : Maintenir documentation technique à jour
