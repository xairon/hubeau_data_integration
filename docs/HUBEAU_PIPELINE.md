# Pipeline Hub'Eau - Fonctionnement Technique

## Architecture Dagster

### Assets
**Définition :** Entités de données avec lineage automatique
**Exécution :** Matérialisation déclarative
**Monitoring :** UI intégrée avec métriques

#### Assets Bronze
```python
@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
    group_name="bronze"
)
def hubeau_hydrometry_bronze(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingestion hydrométrie Hub'Eau vers MinIO"""
    partition_date = context.partition_key
    service = HubeauIngestionService(minio_resource=context.resources.minio)
    config = get_hydrometry_config()
    return service.ingest_api_data(config, partition_date)
```

### Jobs
**Rôle :** Orchestration des assets
**Exécution :** Multi-process avec parallélisation
**Monitoring :** Logs structurés et métriques

#### Jobs d'Ingestion
```python
@job(
    name="hubeau_bronze_ingestion_job",
    resource_defs={"minio": minio_resource},
    executor_def=multiprocess_executor.configured({"max_concurrent": 3})
)
def bronze_ingestion_job():
    """Job d'ingestion complète Hub'Eau"""
    return [
        hubeau_hydrometry_bronze(),
        hubeau_piezometry_bronze(),
        hubeau_quality_bronze(),
        hubeau_temperature_bronze(),
        hubeau_onde_bronze(),
        hubeau_hydrobiology_bronze(),
        hubeau_prelevements_bronze()
    ]
```

### Schedules
**Rôle :** Planification automatique
**Fréquence :** Quotidienne pour APIs temps réel
**Gestion :** Retry automatique sur échecs

```python
@schedule(
    job=bronze_ingestion_job,
    cron_schedule="0 2 * * *",  # 2h00 quotidien
    name="daily_hubeau_ingestion"
)
def daily_ingestion_schedule(context: ScheduleExecutionContext):
    """Planification quotidienne ingestion Hub'Eau"""
    return RunRequest(
        partition_key=context.scheduled_execution_time.strftime("%Y-%m-%d")
    )
```

## Client Hub'Eau

### Configuration
**Rate Limiting :** 0.5s entre requêtes
**Concurrence :** Sémaphore global (10 requêtes max)
**Retry :** 3 tentatives avec backoff exponentiel
**Timeout :** 60s par requête

### Gestion d'Erreurs
```python
async def _make_request(self, endpoint: str, params: Dict[str, Any]) -> HubeauApiResponse:
    """Requête HTTP avec retry automatique"""
    async with GLOBAL_HUBEAU_SEMAPHORE:
        await asyncio.sleep(self.config.rate_limit_delay)
        
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.config.max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException))
        ):
            with attempt:
                await asyncio.sleep(random.random() * 0.5)  # Jitter
                response = await self.client.get(url, params=params)
                response.raise_for_status()
                return HubeauApiResponse(**response.json())
```

### Stratégies d'Ingestion

#### Par Codes d'Entités
```python
async def get_observations(self, endpoint_name: str, entity_codes: List[str], date_partition: str):
    """Récupération par codes d'entités avec chunking"""
    # Chunking adaptatif selon API
    if api_name == "hydrobiology":
        MAX_CODES_PER_REQUEST = 25
    elif api_name == "onde":
        MAX_CODES_PER_REQUEST = 3  # API très sensible
    else:
        MAX_CODES_PER_REQUEST = 50
    
    # Parallélisation avec asyncio.gather
    entity_chunks = [entity_codes[i:i + MAX_CODES_PER_REQUEST] 
                     for i in range(0, len(entity_codes), MAX_CODES_PER_REQUEST)]
    
    tasks = [self._execute_chunk(chunk) for chunk in entity_chunks]
    results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]
```

#### Par Départements
```python
async def get_stations(self, endpoint_name: str = "stations"):
    """Récupération par départements avec chunking adaptatif"""
    all_departments = self._get_french_departments()
    
    # Chunking selon API
    if endpoint_config.path == "referentiel/points_prelevement":
        chunk_size = 1  # API sensible
    elif depth_limit is not None and depth_limit <= 10000:
        chunk_size = 1  # APIs avec limite 10k
    else:
        chunk_size = 5  # APIs standard
    
    dept_chunks = [all_departments[i:i + chunk_size] 
                   for i in range(0, len(all_departments), chunk_size)]
    
    for dept_chunk in dept_chunks:
        params = {
            "format": "json",
            "size": endpoint_config.page_size,
            spatial_param_key: ",".join(dept_chunk)
        }
        chunk_data = await self._fetch_all_pages(endpoint_config, params)
        all_data.extend(chunk_data)
```

## Service d'Ingestion

### HubeauIngestionService
**Rôle :** Orchestration complète d'une API
**Flux :** Stations → Observations → Sauvegarde MinIO
**Métriques :** Observabilité complète

```python
async def ingest_api_data(self, config: HubeauApiConfig, date_partition: str):
    """Ingestion complète d'une API Hub'Eau"""
    results = {}
    total_records = 0
    
    async with HubeauClient(config) as client:
        # 1. Récupérer stations
        stations_endpoint = self._get_stations_endpoint(config)
        if stations_endpoint:
            stations = await client.get_stations(stations_endpoint)
            results[stations_endpoint] = {
                'records_count': len(stations),
                'type': 'stations',
                'data': stations
            }
        
        # 2. Récupérer observations
        observations_endpoints = self._get_observations_endpoints(config)
        for endpoint_name in observations_endpoints:
            station_codes = self._extract_station_codes(stations, config.name)
            observations = await client.get_observations(
                endpoint_name, station_codes, date_partition, config.name
            )
            results[endpoint_name] = {
                'records_count': len(observations),
                'type': 'observations',
                'data': observations
            }
    
    # 3. Sauvegarder dans MinIO
    self._save_to_minio(config.name, date_partition, results)
    return results
```

## Monitoring et Observabilité

### Métriques d'Ingestion
```python
class IngestionMetrics(BaseModel):
    """Métriques d'observabilité"""
    departements_traites: int = 0
    stations_total: int = 0
    chunks_total: int = 0
    chunks_ok: int = 0
    chunks_vides: int = 0
    chunks_echoues: int = 0
    erreurs_http_500: int = 0
    erreurs_timeout: int = 0
```

### Logging Structuré
```python
# Logs avec contexte
self.logger.info(f"🌍 Récupération stations {endpoint_name} pour TOUT LE TERRITOIRE FRANÇAIS")
self.logger.info(f"📊 Découpage spatial: {len(all_departments)} départements en {len(dept_chunks)} groupes")
self.logger.info(f"✅ Groupe {i+1}: {len(chunk_data)} stations (total: {len(all_data)})")
```

## Exécution et Debugging

### Commandes Dagster
```bash
# Exécution manuelle
dagster job execute -j hubeau_hydrometry_bronze_job

# Backfill
dagster asset materialize -a hubeau_hydrometry_bronze --partition 2024-09-01

# Monitoring
dagster asset list -m hubeau_pipeline
dagster run list --limit 10
```

### Debugging
- **Logs :** `docker-compose logs -f dagster_webserver`
- **UI :** http://localhost:8080 pour monitoring visuel
- **Métriques :** Dashboard intégré avec KPIs
- **Erreurs :** Stack traces détaillées avec contexte
