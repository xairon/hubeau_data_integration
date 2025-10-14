# 🔪 Stratégies de Slicing DLT pour Hub'Eau

## Problème : Limite API de 20k records

Hub'Eau impose une limite de **20,000 records maximum par requête**. Si un département ou une période contient plus de 20k records, les données sont **tronquées** (silencieusement !).

## 📊 Stratégies de slicing disponibles

### 1. **Global** (pas de slicing)
```yaml
extraction:
  slicing_mode: global
```

**Cas d'usage :** Endpoints avec peu de données (< 20k total)
- ✅ Simple, une seule requête
- ❌ Risque de truncation si > 20k records
- **Exemples :** Campagnes ONDE (quelques milliers), Sites hydrométriques

---

### 2. **Department** (slicing par département)
```yaml
extraction:
  slicing_mode: dept
  param: code_departement
  values: ["01", "02", ..., "976"]
```

**Cas d'usage :** Référentiels de stations (stations piézométrie, hydrométriques, etc.)
- ✅ Contourne la limite pour la plupart des départements
- ✅ 101 requêtes max (un par département)
- ⚠️ Risque si un département a > 20k stations (rare mais possible)
- **Exemples :** Stations piézométrie (23k total), Stations hydrométriques

**Détection automatique :** Le code log un WARNING si un département atteint 20k records

---

### 3. **Station + Month (chunked)** (slicing par station et mois)
```yaml
extraction:
  slicing_mode: station_month_chunked
  chunk_size: 80
  station_param: code_bss
  start_param: date_debut_mesure
  end_param: date_fin_mesure
```

**Cas d'usage :** Chroniques de mesures (très volumineuses)
- ✅ Slicing ultra-granulaire, aucun risque de truncation
- ✅ Support incremental loading
- ✅ Optimisé pour les données time-series
- ⚠️ Beaucoup de requêtes (3565 stations × 12 mois / 80 = ~535 requêtes)
- **Exemples :** Chroniques piézométrie, Mesures hydrométriques

**Optimisation :** Chunks de 80 stations par requête pour rester sous la limite d'URL

---

### 4. **DateTime** (slicing par période temporelle)
```yaml
extraction:
  slicing_mode: datetime
  period_days: 30
  start_param: date_debut
  end_param: date_fin
```

**Cas d'usage :** Observations avec filtrage temporel
- ✅ Adapté aux données avec forte densité temporelle
- ✅ Permet de paralléliser par période
- ⚠️ Nécessite que l'API supporte le filtrage temporel
- **Exemples :** Observations ONDE, Analyses qualité eau

---

## 🚨 Stratégies pour éviter la truncation

### Problème : Un département a > 20k stations

**Détection :**
```python
if dept_total_records >= 20000:
    logger.warning(f"⚠️ Department {dept} reached 20k limit - possible truncation!")
```

**Solution 1 : Slicing multi-dimensionnel département + préfixe**

Créer un slicing qui combine département + première lettre du code station :

```yaml
extraction:
  slicing_mode: dept_prefix  # Nouveau mode à implémenter
  dept_param: code_departement
  prefix_param: code_bss_prefix  # Filtrer par préfixe
  departments: ["01", "02", ...]
  prefixes: ["0", "1", "2", ..., "9", "A", "B", ...]
```

Cela donnerait : 101 départements × 36 préfixes = **3,636 requêtes max**
Mais chaque requête < 1k records en moyenne

**Solution 2 : Pagination stricte avec vérification**

Vérifier que `last_page` est cohérent :

```python
if page >= last_page and record_count == page_size:
    # On a reçu une page pleine sur la "dernière page" -> suspect !
    logger.warning(f"⚠️ Possible truncation: last page still returned {page_size} records")
```

**Solution 3 : Combiner département + critère temporel**

Pour les stations, utiliser `date_ouverture` en plus du département :

```yaml
extraction:
  slicing_mode: dept_temporal
  dept_param: code_departement
  temporal_param: date_ouverture
  temporal_ranges:
    - ["1900-01-01", "2000-12-31"]
    - ["2001-01-01", "2010-12-31"]
    - ["2011-01-01", "2024-12-31"]
```

---

## 📋 Bonnes pratiques DLT

### 1. **Toujours vérifier la pagination**

```python
# Vérifier que la pagination fonctionne vraiment
if page == 1:
    expected_total = page_data.get('count', 0)  # Total annoncé par l'API
    
if dept_total_records < expected_total:
    logger.warning(f"⚠️ Only got {dept_total_records}/{expected_total} records for {dept}")
```

### 2. **Logger les métriques de slicing**

```python
logger.info(f"📊 Slicing stats:")
logger.info(f"   • Total slices: {len(departments)}")
logger.info(f"   • Total records: {total_records_all}")
logger.info(f"   • Avg records/slice: {total_records_all / len(departments):.1f}")
logger.info(f"   • Max records/slice: {max_records_per_slice}")
```

### 3. **Utiliser les métadonnées DLT**

Ajouter des métadonnées pour tracker le slicing :

```python
record['_slice_dept'] = dept
record['_slice_page'] = page
record['_slice_timestamp'] = datetime.now().isoformat()
record['_slice_total_records'] = dept_total_records
```

### 4. **Incremental loading pour éviter la truncation**

Utiliser `dlt.sources.incremental()` pour charger seulement les nouvelles données :

```python
@dlt.resource(
    primary_key="code_station",
    write_disposition="merge"
)
def stations(updated_at=dlt.sources.incremental("date_maj")):
    # DLT charge seulement les stations mises à jour depuis la dernière run
    yield from api_call(updated_since=updated_at.last_value)
```

---

## 🔍 Diagnostic de truncation

### Comment détecter si on a perdu des données ?

1. **Comparer avec les métadonnées de l'API :**
   ```python
   api_total = response.json().get('count')  # Total annoncé
   records_fetched = len(all_records)
   
   if records_fetched < api_total:
       logger.error(f"❌ TRUNCATION: Got {records_fetched}/{api_total} records")
   ```

2. **Vérifier les logs pour les warnings de 20k :**
   ```bash
   grep "reached 20k" dagster_logs.txt
   ```

3. **Analyser la distribution par slice :**
   ```sql
   SELECT _slice_dept, COUNT(*) as records
   FROM stations
   GROUP BY _slice_dept
   HAVING COUNT(*) >= 20000
   ```

---

## 🎯 Recommandations par endpoint

| Endpoint | Slicing recommandé | Justification |
|----------|-------------------|---------------|
| **Stations piézométrie** | `dept` | 23k total, max ~500/dept |
| **Stations hydrométriques** | `dept` | ~10k total, pas de risque |
| **Chroniques piézométrie** | `station_month_chunked` | Millions de mesures |
| **Observations ONDE** | `datetime` (annuel) | ~50k campagnes/an |
| **Analyses qualité** | `station_month_chunked` | Très volumineuses |
| **Prélèvements** | `dept` + temporel | ~500k ouvrages |

---

## 🚀 Migration vers slicing plus fin

Si un endpoint commence à avoir > 20k records par slice :

1. **Ajouter des logs de détection** (déjà fait ✅)
2. **Créer une nouvelle config YAML** avec slicing plus fin
3. **Tester sur une partition** pour valider
4. **Migrer progressivement** les anciennes données

Exemple :
```yaml
# Avant : piezometry_stations.yml
extraction:
  slicing_mode: dept

# Après : piezometry_stations_v2.yml
extraction:
  slicing_mode: dept_prefix
  dept_param: code_departement
  prefix_param: code_bss
  prefix_length: 2  # Premiers 2 caractères du code BSS
```

