# 🧹 Nettoyage des Buckets MinIO

## Problème identifié

Le système créait **deux buckets** dans MinIO :
1. **`hubeau-bronze`** - avec des fichiers **parquet** ✅
2. **`bronze`** - avec des fichiers **jsonl** ❌

Cette duplication venait d'une configuration incohérente entre :
- Les fichiers YAML de configuration (qui utilisaient `s3://hubeau-bronze`)
- Le code par défaut (qui utilisait `s3://bronze` comme fallback)

## Solutions appliquées

### 1. Uniformisation de la configuration

Tous les buckets ont été standardisés sur **`s3://bronze`** avec le format **parquet** :

```yaml
# Dans tous les fichiers configs/hubeau/*.yml
destinations:
  filesystem:
    bucket_url: s3://bronze        # ✅ Standardisé
    file_format: parquet            # ✅ Format optimisé
    layout: '{table_name}/year={YYYY}/{load_id}.parquet'
```

### 2. Mise à jour du code

- **`src/dlt_pipeline/destinations.py`** : bucket par défaut changé de `s3://hubeau-bronze` → `s3://bronze`
- **`src/hubeau_pipeline/assets/bronze/dlt_assets.py`** : logs DLT améliorés pour debug

### 3. Script de nettoyage MinIO

Un script a été créé pour nettoyer les anciens buckets : `scripts/cleanup_minio_buckets.py`

## Comment nettoyer les buckets existants

### Option 1 : Via l'interface MinIO (manuel)

1. Ouvrir MinIO Console : http://localhost:9001
2. Connexion avec `admin` / `BrgmMinio2024!`
3. Supprimer manuellement le bucket `hubeau-bronze` (ou tout autre bucket indésirable)
4. Garder uniquement le bucket **`bronze`**

### Option 2 : Via le script automatique (recommandé)

```bash
# Depuis le répertoire du projet
python scripts/cleanup_minio_buckets.py
```

Ou depuis Docker :

```bash
docker exec dagster python scripts/cleanup_minio_buckets.py
```

Le script va :
1. ✅ Lister tous les buckets existants
2. 📊 Afficher les statistiques (nombre de fichiers parquet/jsonl)
3. ⚠️ Demander confirmation avant suppression
4. 🗑️ Supprimer les buckets en double
5. 📦 Créer le bucket `bronze` s'il n'existe pas
6. ✅ Vérifier la configuration finale

## Vérification post-nettoyage

Après le nettoyage, vérifier que :

1. **Un seul bucket** existe : `bronze`
2. **Tous les fichiers** sont au format **parquet** (pas de jsonl)
3. **La structure** respecte le layout : `{table_name}/year={YYYY}/{load_id}.parquet`

Exemple de structure attendue :
```
bronze/
├── piezometry_api/
│   ├── piezometry_stations/
│   │   └── 1760453954.2435954.parquet
│   └── piezometry_chroniques/
│       ├── year=2024/
│       │   └── 1760454120.5678901.parquet
│       └── year=2023/
│           └── 1760454130.1234567.parquet
└── hydrometry_api/
    └── hydrometry_stations/
        └── 1760454140.9876543.parquet
```

## Prévention future

Pour éviter la recréation de buckets en double :

1. ✅ **Toujours utiliser les fichiers YAML** de configuration (jamais de hardcode)
2. ✅ **Vérifier que `bucket_url: s3://bronze`** est présent dans tous les YAML
3. ✅ **Utiliser `file_format: parquet`** (plus efficace que jsonl)
4. ✅ **Tester avec une partition** avant de lancer en masse

## Correction des logs DLT invisibles

Le problème des logs DLT invisibles a été corrigé dans `dlt_assets.py` :

```python
# ✅ Capture des logs hubeau_source.py
hubeau_source_logger = logging.getLogger('src.dlt_pipeline.hubeau_source')
hubeau_source_logger.setLevel(logging.DEBUG)
hubeau_source_logger.addHandler(dagster_handler)
```

Maintenant, les logs montreront :
- 📡 Requêtes HTTP vers Hub'Eau API
- 📊 Nombre de records extraits par page
- 🏛️ Départements traités
- ✅ Statut des réponses API

## Relancer les pipelines

Après le nettoyage, relancer les assets Dagster pour vérifier que tout fonctionne :

```bash
# Via l'interface Dagster UI
1. Ouvrir http://localhost:3000
2. Aller dans "Assets"
3. Sélectionner "piezometry_stations_reference"
4. Cliquer "Materialize"
5. Observer les logs détaillés
```

Les nouveaux logs devraient afficher :
```
📡 HTTP GET https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations dept=01 page=1
✅ Response: status=200, content-length=45000 bytes
📊 Extracted 150 records from page 1
✅ Department 01 completed: 150 records extracted
```

## Support

Si le problème persiste après le nettoyage :

1. Vérifier les variables d'environnement MinIO
2. Vérifier la connectivité MinIO (port 9000)
3. Consulter les logs Dagster complets
4. Vérifier les credentials dans `.env`

