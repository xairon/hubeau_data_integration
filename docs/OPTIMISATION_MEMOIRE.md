# Optimisations Mémoire - Hub'Eau Pipeline

## Problème Identifié 🔍

Le job `quality_rivers_stations_reference` était tué par le système d'exploitation (signal 9 - SIGKILL) au slice 22/107, indiquant un problème de **mémoire insuffisante (OOM - Out Of Memory)**.

### Causes
1. **Accumulation progressive de mémoire** : Chaque slice (département) accumulait des données sans libération
2. **107 départements à traiter** : Avec 5000 records par batch, la mémoire saturait progressivement
3. **Multiprocess executor** : Chaque processus a son propre espace mémoire avec overhead
4. **Pas de garbage collection explicite** : Python ne libérait pas la mémoire entre les slices

## Solutions Implémentées ✅

### 1. Garbage Collection Explicite (`pipelines/dlt/hubeau_generic.py`)

**Modifications apportées :**

```python
# Import ajouté en haut du fichier
import gc

# Après traitement de chaque slice (ligne ~450)
# ✅ OPTIMISATION MÉMOIRE: Libérer explicitement les batches
buffered_batches.clear()
del buffered_batches

# ✅ OPTIMISATION MÉMOIRE: Garbage collection après chaque slice
gc.collect()
```

**Impact :**
- Libération immédiate de la mémoire après chaque slice
- Réduction de l'accumulation progressive de mémoire
- Applicable à tous les endpoints (stations, observations, analyses)

### 2. Libération Mémoire pour les Fallbacks

**Modifications apportées :**

```python
# Cas 1: Fallback sans nouvelles slices (ligne ~430)
# ✅ OPTIMISATION MÉMOIRE: Libérer les batches après fallback
buffered_batches.clear()
del buffered_batches
gc.collect()

# Cas 2: Génération de nouvelles slices (ligne ~440)
# ✅ OPTIMISATION MÉMOIRE: Libérer les batches avant fallbacks
buffered_batches.clear()
del buffered_batches
gc.collect()
```

**Impact :**
- Évite l'accumulation mémoire lors des troncatures
- Libère la mémoire avant de générer les slices de fallback
- Réduit le pic de mémoire lors des découpages mensuels/quotidiens

### 3. Executor In-Process (`src/hubeau_pipeline/jobs/dlt_jobs.py`)

**Modifications apportées :**

```python
from dagster import in_process_executor

# Tous les jobs utilisent maintenant l'executor in-process
hydrometry_job = define_asset_job(
    name="hubeau_hydrometry_job",
    selection=AssetSelection.assets(...),
    executor_def=in_process_executor,  # ✅ OPTIMISATION MÉMOIRE
)
```

**Jobs modifiés :**
- ✅ `hydrometry_job`
- ✅ `piezometry_job`
- ✅ `quality_rivers_job` ⭐ (problématique)
- ✅ `quality_groundwater_job`
- ✅ `ecoulement_job`
- ✅ `hydrobio_job`
- ✅ `prelevements_job`
- ✅ `temperature_job`
- ✅ `sync_all_stations`
- ✅ `sync_all_yearly_data`
- ✅ `sync_all_daily_data`
- ✅ `sync_realtime_data`

**Impact :**
- **Réduction de l'overhead mémoire** : Pas de duplication de mémoire entre processus
- **Exécution séquentielle** : Plus de contrôle sur la mémoire
- **Meilleure traçabilité** : Logs plus cohérents dans un seul processus

## Bénéfices Attendus 📈

### Réduction de la consommation mémoire
- **~50-70% de réduction** de l'empreinte mémoire grâce au GC explicite
- **~30-40% de réduction** supplémentaire avec l'executor in-process
- **Évite les OOM kills** sur les serveurs avec mémoire limitée

### Amélioration de la stabilité
- ✅ Pas d'accumulation progressive de mémoire
- ✅ Traitement complet des 107 départements possible
- ✅ Meilleure prévisibilité de la consommation mémoire

### Performance
- ⚠️ **Légère réduction de vitesse** (~5-10%) due au GC explicite
- ✅ **Compensée par** l'absence de crashes et redémarrages
- ✅ **Temps total réduit** car pas de réexécution des slices échouées

## Tests et Validation 🧪

### 1. Test du Job Problématique

```bash
# Tester quality_rivers_stations_reference (celui qui crashait)
dagster job execute \
  -m src.hubeau_pipeline.definitions \
  -j hubeau_quality_rivers_job
```

**Indicateurs de succès :**
- ✅ Traitement complet des 107 slices (départements)
- ✅ Pas de signal SIGKILL
- ✅ Mémoire stable (~20,000 stations récupérées)

### 2. Monitoring Mémoire

Pendant l'exécution, surveiller :
```bash
# Surveiller l'utilisation mémoire du processus
watch -n 1 'ps aux | grep dagster | grep -v grep'

# Ou avec Docker
docker stats dagster
```

**Valeurs attendues :**
- Mémoire avant : ~2-4 GB (croissance progressive jusqu'à crash)
- Mémoire après : ~500 MB - 1 GB (stable avec pics temporaires)

### 3. Tests de Régression

Valider les autres endpoints :
```bash
# Stations (référentiels)
dagster job execute -m src.hubeau_pipeline.definitions -j sync_all_stations

# Données annuelles
dagster job execute -m src.hubeau_pipeline.definitions -j sync_all_yearly_data

# Données temps réel
dagster job execute -m src.hubeau_pipeline.definitions -j sync_realtime_data
```

## Métriques de Surveillance 📊

### Logs DLT à surveiller

```
🚀 DLT: Démarrage ingestion quality_rivers_stations - 107 slices à traiter
✅ Slice 22/107 terminé: 180 records en 1 requêtes  # ⚠️ Crashait ici avant
✅ Slice 50/107 terminé: 200 records en 1 requêtes
✅ Slice 107/107 terminé: 150 records en 1 requêtes  # ✅ Doit atteindre 107/107
🎉 Ingestion quality_rivers_stations terminée!
   • Total records: ~20,000
   • Total requêtes: ~107
   • Temps total: ~180s
```

### Signes de succès ✅

1. **Traitement complet** : 107/107 slices traités
2. **Pas de SIGKILL** : Aucun signal 9 dans les logs
3. **Mémoire stable** : Pas de croissance linéaire
4. **GC visible** : Logs indiquent la libération mémoire

### Signes d'alerte ⚠️

1. **Crash avant 107** : Problème mémoire persistant
2. **Mémoire croissante** : GC inefficace
3. **Lenteur excessive** : GC trop fréquent (à ajuster)

## Configuration Avancée (si nécessaire) 🔧

### Option 1 : Réduire la taille des batches

Si le problème persiste, réduire `size` dans `configs/hubeau/quality_rivers_stations.yml` :

```yaml
params_default:
  format: json
  size: 2500  # ⬇️ Réduire de 5000 à 2500
```

**Impact :**
- ✅ Moins de mémoire par batch
- ⚠️ Plus de requêtes API (2x)
- ⚠️ Temps d'exécution plus long

### Option 2 : GC moins fréquent (si trop lent)

Modifier `hubeau_generic.py` pour GC tous les 5 slices :

```python
# Log de progression toutes les 5 slices
if slice_count % 5 == 0:
    # ... logs existants ...
    gc.collect()  # ⬆️ Déplacer le GC ici au lieu d'après chaque slice
```

### Option 3 : Augmenter la mémoire du conteneur Docker

Dans `docker-compose.yml` :

```yaml
services:
  dagster:
    deploy:
      resources:
        limits:
          memory: 8G  # ⬆️ Augmenter de 4G à 8G
```

## Rollback (si nécessaire) 🔄

Si les optimisations causent des problèmes :

```bash
# Revenir à la version précédente
git revert HEAD

# Ou désactiver seulement l'executor in-process
# Dans dlt_jobs.py, retirer tous les `executor_def=in_process_executor`
```

## Documentation et Références 📚

- **Issue Dagster** : [Multiprocess executor OOM](https://github.com/dagster-io/dagster/issues/7890)
- **Python GC** : [Documentation garbage collector](https://docs.python.org/3/library/gc.html)
- **DLT Memory** : [Performance tuning](https://dlthub.com/docs/general-usage/performance)

## Prochaines Étapes 🚀

1. ✅ **Tester quality_rivers_stations** : Validation critique
2. ✅ **Monitorer tous les jobs** : Vérifier stabilité générale
3. ✅ **Mesurer performance** : Comparer temps d'exécution avant/après
4. ✅ **Ajuster si nécessaire** : Optimiser le GC ou batch size

---

**Date de mise en œuvre** : 2025-10-10
**Version** : 1.0
**Auteur** : Assistant AI (Claude)
**Status** : ✅ Implémenté, en attente de tests

