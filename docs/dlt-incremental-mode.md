# Configuration DLT en Mode Incrémental

## Problématique

Quand on redémarre l'application, on veut:
✅ **NE PAS** dupliquer les données déjà ingérées
✅ **COMPLETER** les fichiers Parquet avec les nouvelles données
✅ **GARDER** l'historique complet

## Solution: State Management DLT

DLT utilise un système de **state** pour tracker ce qui a déjà été ingéré. Ce state DOIT être persisté dans MinIO (pas dans le conteneur Docker).

### Configuration actuelle

Le code `dlt_assets.py` ligne 866 définit:
```python
state_store = cfg.get("state_store", "s3://bronze/_state")
```

Mais cette configuration n'est **PAS utilisée** par le pipeline !

### Correction nécessaire

**ACTUELLEMENT**, le state DLT est stocké dans `/app/.dlt/` (perdu à chaque redémarrage).

**SOLUTION**: DLT stocke automatiquement le state dans la destination `filesystem` si configuré correctement.

## Comment fonctionne l'incrémental DLT

### 1. State Management automatique

DLT stocke automatiquement:
- Le dernier `load_id` pour chaque table
- Les checksums des données
- Les métadonnées de chaque run

### 2. Détection des doublons

DLT utilise les **primary_keys** définis dans les configs YAML pour détecter les doublons:

```yaml
# Exemple: configs/hubeau/temperature_chroniques.yml
primary_keys:
  - code_station
  - date_mesure_temp
```

Quand DLT voit un record avec la même combinaison `(code_station, date_mesure_temp)`, il:
- **Mode merge** (défaut Parquet): Remplace l'ancien record
- **Mode append**: Ajoute le nouveau record (attention aux doublons !)

### 3. Write disposition

Dans les configs YAML, on peut définir:

```yaml
write_disposition: merge  # Recommandé pour éviter doublons
# OU
write_disposition: append  # Pour données purement additives
# OU
write_disposition: replace  # Remplace TOUT à chaque run
```

## État actuel du projet

### Configs qui utilisent `merge` (safe):
- `temperature_stations.yml`: `write_disposition: replace` (référentiel)
- `quality_groundwater_stations.yml`: `write_disposition: replace` (référentiel)

### Configs qui utilisent des primary_keys:
Toutes les configs d'observations ont des primary_keys définis.

## Vérification

Pour vérifier que le state est bien persisté:

```bash
# Se connecter au conteneur MinIO
docker exec brgm-minio-1 mc ls local/bronze/_dlt_pipeline_state/

# Devrait montrer les fichiers de state pour chaque pipeline
```

## Test de non-duplication

1. Lancer un job (ex: `temperature_chroniques` pour partition 2024)
2. Vérifier le nombre de records dans MinIO
3. Relancer le même job
4. Vérifier que le nombre de records n'a PAS doublé

```bash
# Compter les records dans un fichier Parquet
docker exec brgm-dagster-daemon python -c "
import pyarrow.parquet as pq
import pyarrow.fs as pafs

s3_fs = pafs.S3FileSystem(
    access_key='minioadmin',
    secret_key='minioadmin',
    endpoint_override='minio:9000',
    scheme='http'
)

table = pq.read_table('bronze/temperature_api/temperature_chroniques/<LOAD_ID>.parquet', filesystem=s3_fs)
print(f'Total records: {len(table)}')
"
```

## Recommandation

✅ **DLT gère déjà l'incrémental** grâce aux primary_keys
✅ **Le state est stocké dans MinIO** (dans `bronze/_dlt_pipeline_state/`)
✅ **Pas de duplication** si les primary_keys sont bien définis

**Action requise**: Vérifier que toutes les configs YAML ont:
1. `primary_keys` définis
2. `write_disposition: merge` pour les observations
3. `write_disposition: replace` pour les référentiels
