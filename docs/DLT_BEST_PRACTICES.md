# DLT Best Practices - Hub'Eau Pipeline

## Meilleures pratiques pour éviter les fuites mémoire et optimiser les performances

### 1. Gestion des pipelines DLT

#### ✅ BON : Créer un nouveau pipeline pour chaque tâche majeure

```python
# Pour les boucles de traitement par chunks
for chunk_index, record in enumerate(data_source()):
    # Créer un nouveau pipeline à CHAQUE itération
    pipeline = create_dlt_pipeline("my_pipeline", context=context)

    load_info = pipeline.run(resource)

    # Nettoyage explicite
    del pipeline
    del load_info
    gc.collect()
```

**Pourquoi ?**
- DLT maintient un état interne (connexions DB, cache, métadonnées)
- Réutiliser un pipeline accumule la mémoire progressivement
- Recréer le pipeline libère les ressources à chaque itération

#### ❌ MAUVAIS : Réutiliser le même pipeline dans une boucle

```python
# NE PAS FAIRE : Création unique
pipeline = create_dlt_pipeline("my_pipeline", context=context)

for chunk_index, record in enumerate(data_source()):
    # Réutilisation du même pipeline -> FUITE MÉMOIRE
    load_info = pipeline.run(resource)
```

---

### 2. Utilisation des générateurs

#### ✅ BON : Yield par pages/batches

```python
@dlt.resource(name="my_resource", write_disposition="append")
def my_data_source(config):
    """Générateur qui yield progressivement"""
    for page_num in range(1, total_pages + 1):
        records = fetch_page(page_num)
        if records:
            yield records  # DLT streame en batches
```

**Pourquoi ?**
- DLT traite les données en streaming
- Pas d'accumulation mémoire
- Les données sont écrites en PostgreSQL au fur et à mesure

#### ❌ MAUVAIS : Charger tout en mémoire

```python
@dlt.resource(name="my_resource", write_disposition="append")
def my_data_source(config):
    all_records = []  # ❌ Accumulation en RAM
    for page_num in range(1, total_pages + 1):
        records = fetch_page(page_num)
        all_records.extend(records)

    yield all_records  # Trop tard, la RAM est saturée
```

---

### 3. Gestion des ressources DLT

#### ✅ BON : Définir les ressources HORS de la boucle (quand possible)

```python
@dlt.resource(
    name="my_resource",
    write_disposition="append",
    primary_key="id"
)
def my_data_source(config):
    """Définie UNE FOIS, instanciée UNE FOIS"""
    for record in fetch_data():
        yield record

# Utilisation
pipeline = create_dlt_pipeline("my_pipeline", context=context)
pipeline.run(my_data_source(config))
```

#### ⚠️ ACCEPTABLE (cas spécial) : Redéfinir dans une boucle avec nouveau pipeline

```python
# Cas ERA5 : nécessaire pour chunk-by-chunk storage
for chunk_index, record in enumerate(data_source()):
    # Nouveau pipeline à chaque fois
    pipeline = create_dlt_pipeline("my_pipeline", context=context)

    # Ressource redéfinie (closure sur 'record')
    @dlt.resource(name="my_resource", write_disposition="append")
    def single_chunk():
        yield record

    pipeline.run(single_chunk)

    # Nettoyage
    del pipeline
    gc.collect()
```

**Note** : Ce pattern est acceptable **SEULEMENT** si le pipeline est recréé à chaque itération.

---

### 4. Configuration DLT pour la performance

#### Contrôle de la mémoire

```python
# Dans dlt_batching.py ou config
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=postgres(...),
    dataset_name="staging",
    progress="log",
    # Optionnel: limiter la mémoire
    # config={
    #     "data_writer.buffer_max_items": 5000,  # Défaut: 5000
    #     "data_writer.file_max_items": 10000    # Défaut: 10000
    # }
)
```

#### Parallelisation

```python
@dlt.resource(
    parallelized=False,  # ✅ Dagster gère le parallélisme
    write_disposition="append"
)
def my_resource(config):
    """Sequential fetching, Dagster handles asset-level parallelism"""
    for page in fetch_pages():
        yield page
```

**Note** : Dans notre architecture, Dagster `multiprocess_executor` gère le parallélisme au niveau des assets. DLT `parallelized=True` est désactivé pour éviter la contention.

---

### 5. Nettoyage et garbage collection

#### ✅ BON : Nettoyage explicite après traitement lourd

```python
import gc

for chunk in large_data_source():
    pipeline = create_dlt_pipeline("my_pipeline", context=context)

    load_info = pipeline.run(resource)

    # Nettoyage explicite
    del pipeline
    del load_info
    gc.collect()  # Force garbage collection
```

**Quand utiliser ?**
- Traitement de gros volumes (> 100 MB par chunk)
- Boucles avec plus de 10 itérations
- Utilisation de ressources externes (connexions DB multiples)

---

### 6. Gestion des connexions PostgreSQL

#### ✅ BON : Utiliser un pool de connexions

```python
from dlt.destinations import postgres

destination = postgres(
    credentials={
        "database": os.getenv("PG_DB"),
        "username": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": int(os.getenv("PG_PORT", "5432")),
    }
)
```

DLT gère automatiquement le pool de connexions. Pas besoin de gérer manuellement.

#### ⚠️ Limite PostgreSQL

```yaml
# docker-compose.yml
postgres:
  environment:
    POSTGRES_MAX_CONNECTIONS: 200  # Défaut: 100
```

Augmenter si nécessaire pour supporter des charges concurrentes élevées.

---

### 7. Monitoring et debugging

#### Logs essentiels

```python
context.log.info(f"🔧 Creating fresh DLT pipeline...")
pipeline = create_dlt_pipeline("my_pipeline", context=context)

context.log.info(f"💾 Storing chunk {chunk_id}...")
load_info = pipeline.run(resource)

context.log.info(f"✅ Chunk stored successfully!")
context.log.info(f"🧹 Memory cleaned (garbage collection)")
```

#### Docker stats en temps réel

```bash
docker stats brgm-dlt-worker
```

Surveiller `MEM USAGE` pendant l'exécution. Une augmentation linéaire = fuite mémoire.

---

### 8. Checklist avant déploiement

- [ ] Les pipelines DLT sont-ils recréés à chaque itération dans les boucles ?
- [ ] Les générateurs utilisent-ils `yield` progressivement ?
- [ ] Le garbage collection est-il forcé après les traitements lourds ?
- [ ] Les logs permettent-ils de tracer la progression et la consommation mémoire ?
- [ ] Les tests de charge ont-ils été effectués sur des datasets volumineux ?
- [ ] La configuration PostgreSQL est-elle adaptée (max_connections, shared_buffers) ?

---

## Exemples d'architecture

### Architecture 1 : Asset simple (stations, replace mode)

```python
@asset(...)
def my_stations_raw(context):
    config = load_config("my_stations.yml")

    # Pipeline créé UNE FOIS
    pipeline = create_dlt_pipeline("my_stations", context=context)

    # Exécution UNIQUE
    metrics = run_dlt_resource(
        pipeline=pipeline,
        resource=hubeau_stations(config, dagster_context=context),
        table_name="my_stations_raw",
    )

    return metrics
```

**Verdict** : ✅ Optimal, pas de fuite mémoire

---

### Architecture 2 : Asset avec partitions (chroniques, year mode)

```python
@asset(partitions_def=MODE_PARTITIONS, ...)
def my_chroniques_raw(context):
    config = load_config("my_chroniques.yml")

    # Pipeline créé UNE FOIS par partition
    pipeline = create_dlt_pipeline("my_chroniques", context=context)

    if context.has_partition_key:
        year = context.partition_key

        # Delete existing data for idempotence
        delete_year_data("my_chroniques_raw", year, "date_field")

        # Exécution UNIQUE pour cette partition
        metrics = run_dlt_resource(
            pipeline=pipeline,
            resource=hubeau_chroniques_year(config, year=year, dagster_context=context),
            table_name="my_chroniques_raw",
        )

    return metrics
```

**Verdict** : ✅ Optimal, chaque partition = nouveau pipeline

---

### Architecture 3 : Asset avec boucle manuelle (ERA5, chunk-by-chunk)

```python
@asset(...)
def era5_france_meteo_raw(context):
    config = load_config("era5_france_meteo.yml")

    total_files = 0

    # BOUCLE avec création de pipeline à chaque itération
    for chunk_index, record in enumerate(era5_france_meteo(config, dagster_context=context), start=1):

        # ✅ NOUVEAU pipeline à chaque chunk
        pipeline = create_dlt_pipeline("era5_france_meteo", context=context)

        @dlt.resource(name="era5_netcdf_files", write_disposition="append")
        def single_chunk():
            yield record

        load_info = pipeline.run(single_chunk, table_name="era5_france_meteo_raw")

        total_files += 1

        # ✅ Nettoyage explicite
        del pipeline
        del load_info
        gc.collect()

    return {"rows_loaded": total_files, "status": "success"}
```

**Verdict** : ✅ Optimal avec cleanup explicite

---

## Références

- [DLT Documentation - Performance](https://dlthub.com/docs/reference/performance)
- [DLT Documentation - Build Advanced Pipeline](https://dlthub.com/docs/tutorial/load-data-from-an-api)
- [Moving Data with Python and dlt (DataCamp)](https://www.datacamp.com/tutorial/python-dlt)

---

**Dernière mise à jour** : 2025-01-04
**Auteur** : Hub'Eau Pipeline Team (avec aide de Claude Code)
