# Operations Runbook

## Common Scenarios & Solutions

---

## 🔴 Pipeline Failures

### Hub'Eau API Returns 503 (Service Unavailable)
**Symptoms**: Job fails with `HTTPError 503`
**Cause**: Hub'Eau API is temporarily overloaded or under maintenance
**Solution**:
1. Check [Hub'Eau Status](https://hubeau.eaufrance.fr/) for announcements
2. Wait 15-30 minutes and retry
3. If persistent, check Twitter/X for @eabordeaux announcements

### CDS API Timeout (ERA5)
**Symptoms**: `TimeoutError` or `ConnectionError` during ERA5 download
**Cause**: Large request or CDS server overloaded
**Solution**:
1. Retry the job (has built-in retry with exponential backoff)
2. If 2-year chunk fails, consider running smaller date ranges manually
3. Check [CDS Status](https://cds.climate.copernicus.eu/)

---

## 🟠 Data Issues

### Gap in ERA5 Data
**Symptoms**: Missing dates in `bronze.era5_france_timeseries`
**Diagnosis**:
```sql
SELECT date_trunc('month', time), COUNT(*) 
FROM bronze.era5_france_timeseries 
GROUP BY 1 ORDER BY 1;
```
**Solution**:
1. Identify missing period
2. Manually trigger historical partition: `dagster job launch --job era5_historical_load --partition 2006_2007`

### Duplicates in Bronze Tables
**Symptoms**: More rows than expected after daily load
**Cause**: Overlap window (7 days) without dedup
**Solution**: Run dbt to deduplicate into Silver:
```bash
docker exec brgm-dlt-worker dbt run --select stg_piezo_chroniques stg_hydrometry_obs_elab
```

---

## 🟡 Infrastructure Issues

### Container Won't Start
**Diagnosis**:
```powershell
docker-compose logs dagster_webserver
docker-compose logs dlt_worker
```
**Common fixes**:
- Port conflict: Change port in `docker-compose.yml`
- Volume permissions: `docker-compose down -v` and restart
- Image issue: `docker-compose build --no-cache`

### Database Connection Refused
**Diagnosis**:
```powershell
docker exec brgm-postgres pg_isready
```
**Solution**:
1. Check if container is running: `docker ps`
2. Check logs: `docker logs brgm-postgres`
3. Restart: `docker-compose restart postgres`

### Disk Space Full
**Symptoms**: Write errors, container crashes
**Diagnosis**:
```powershell
docker system df
```
**Solution**:
```powershell
docker system prune -a --volumes  # WARNING: Removes unused data!
```

---

## 🟢 Routine Operations

### Full Reload (Clean Slate)
```powershell
docker-compose down -v
docker-compose up -d
# Wait for healthy, then in Dagster UI:
# 1. Run all_stations_bronze
# 2. Run all_chroniques_bronze (partitioned - will take hours)
# 3. Run era5_historical_load (partitioned - will take days)
# 4. Run dbt_silver_gold_pipeline
```

### Check Data Freshness
```sql
SELECT 
  'piezometry' as domain,
  MAX(date_mesure) as latest_date,
  NOW() - MAX(date_mesure) as lag
FROM bronze.piezometry_chroniques_raw
UNION ALL
SELECT 
  'hydrometry',
  MAX(date_obs_elab),
  NOW() - MAX(date_obs_elab)
FROM bronze.hydrometry_obs_elab_raw
UNION ALL
SELECT 
  'era5',
  MAX(time),
  NOW() - MAX(time)
FROM bronze.era5_france_timeseries;
```

### Verify dbt Models
```bash
docker exec brgm-dlt-worker dbt test
```
