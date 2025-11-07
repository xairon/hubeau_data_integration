# Checklist d'intégration SANDRE & BD-LISA

## ✅ Vérifications effectuées

### 1. **Structure des fichiers**
- ✅ Sources DLT créées
  - `/src/hubeau_pipeline/sources/sandre_raw_source.py`
  - `/src/hubeau_pipeline/sources/bdlisa_raw_source.py`

- ✅ Assets Dagster créés
  - `/src/hubeau_pipeline/assets/bronze/sandre_raw_assets.py`
  - `/src/hubeau_pipeline/assets/bronze/bdlisa_raw_assets.py`

- ✅ Jobs créés
  - `/src/hubeau_pipeline/jobs/reference_jobs.py`

### 2. **Imports et dépendances**
- ✅ `shapely` et `geopandas` présents dans `pyproject.toml`
- ✅ Imports corrigés dans `reference_jobs.py` (suppression import double)
- ✅ Assets importés dans `/src/hubeau_pipeline/assets/__init__.py`
- ✅ Jobs importés dans `/src/hubeau_pipeline/jobs/__init__.py`
- ✅ `definitions.py` mis à jour (commentaire: 21 jobs)

### 3. **Patterns Dagster**
- ✅ Utilisation de `multi_asset` avec `AssetOut` pour grouper les assets liés
- ✅ Utilisation de `define_asset_job` avec `AssetSelection.groups()`
- ✅ Groupes cohérents entre assets et jobs:
  - `sandre_references`
  - `sandre_territories`
  - `sandre_hydro`
  - `sandre_organizations`
  - `bdlisa_spatial`

### 4. **Gestion d'erreurs**
- ✅ Try/except dans les sources avec logging
- ✅ Fallback dans BD-LISA si WFS échoue
- ✅ Initialisation `conn = None` avant try/except dans PostGIS
- ✅ Gestion des géométries NULL

### 5. **Schémas PostgreSQL**
- ✅ Schéma `sandre` pour les référentiels SANDRE
- ✅ Schéma `bdlisa` pour les données BD-LISA
- ✅ Extension PostGIS activée

### 6. **Performance**
- ✅ Utilisation d'Iterator (pas de listes) dans les sources
- ✅ Pagination dans SANDRE API
- ✅ Rate limiting (0.5s) pour ne pas surcharger les APIs
- ✅ Index créés sur les colonnes clés et géométries

## 🚀 Pour utiliser

### Dans Dagster UI (http://localhost:3000)
1. Aller dans "Jobs"
2. Lancer:
   - `sandre_full_load` - Charge tous les référentiels SANDRE
   - `bdlisa_spatial_load` - Charge BD-LISA avec PostGIS
   - `reference_data_full_load` - Charge tout

### Vérifier le chargement
```sql
-- Compter les données SANDRE
SELECT 'sandre.sandre_parametres' as table_name, COUNT(*) FROM sandre.sandre_parametres
UNION ALL SELECT 'sandre.sandre_communes', COUNT(*) FROM sandre.sandre_communes;

-- Vérifier BD-LISA
SELECT niveau, COUNT(*), COUNT(geom) as with_geom
FROM bdlisa.bdlisa_entites_nv3
GROUP BY niveau;
```

## ⚠️ Points d'attention

1. **WFS BD-LISA peut être lent** - Timeout de 60s configuré
2. **Volume de données** - cours_eau SANDRE = ~120k lignes
3. **PostGIS requis** - CREATE EXTENSION postgis;
4. **Rate limiting** - 0.5s entre requêtes API

## 📊 Métriques attendues

### SANDRE (14 tables)
- parametres: ~6,300 lignes
- communes: ~35,000 lignes
- intervenants: ~50,000 lignes
- cours_eau: ~120,000 lignes
- Total: ~250,000 lignes

### BD-LISA (3 tables)
- Niveau 1: ~200 polygones
- Niveau 2: ~500 polygones
- Niveau 3: ~4,000 polygones
- Total: ~4,700 polygones avec géométries

## 🔧 Debugging

Si erreur d'import:
```bash
# Tester dans le conteneur Docker
docker exec -it brgm-dagster-webserver bash
python3 -c "from hubeau_pipeline.sources.sandre_raw_source import sandre_raw"
```

Si géométries manquantes:
```sql
-- Vérifier PostGIS
SELECT PostGIS_Version();

-- Reprocesser les géométries
UPDATE bdlisa.bdlisa_entites_nv3
SET geom = ST_GeomFromText(geometry_wkt, 2154)
WHERE geometry_wkt IS NOT NULL AND geom IS NULL;
```