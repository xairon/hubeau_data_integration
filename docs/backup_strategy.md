# Backup & Recovery Strategy

## PostgreSQL / TimescaleDB

### Automated Daily Backup (Recommended)
Add this to a cron job or scheduled task:

```bash
# Daily backup at 3 AM
0 3 * * * docker exec brgm-postgres pg_dumpall -c -U postgres | gzip > /backups/hubeau_$(date +\%Y\%m\%d).sql.gz

# Keep only last 7 days
find /backups -name "hubeau_*.sql.gz" -mtime +7 -delete
```

### Manual Backup
```powershell
# Full database dump
docker exec -t brgm-postgres pg_dumpall -c -U postgres > backup_full.sql

# Single schema (faster)
docker exec -t brgm-postgres pg_dump -U postgres -n bronze postgres > backup_bronze.sql
docker exec -t brgm-postgres pg_dump -U postgres -n silver postgres > backup_silver.sql
```

### Restore
```powershell
# Full restore (destructive!)
Get-Content backup_full.sql | docker exec -i brgm-postgres psql -U postgres

# Schema restore
Get-Content backup_bronze.sql | docker exec -i brgm-postgres psql -U postgres
```

## Docker Volumes

### Volume Backup
```powershell
# Stop containers first
docker-compose stop

# Backup volume
docker run --rm -v brgm_postgres_data:/data -v ${PWD}:/backup alpine tar czf /backup/postgres_data.tar.gz /data

# Restart
docker-compose up -d
```

### Volume Restore
```powershell
docker-compose down -v
docker run --rm -v brgm_postgres_data:/data -v ${PWD}:/backup alpine tar xzf /backup/postgres_data.tar.gz -C /
docker-compose up -d
```

## Recovery Time Objectives

| Scenario | RTO | RPO | Method |
|----------|-----|-----|--------|
| Container crash | 1 min | 0 | Auto-restart |
| Volume corruption | 30 min | 1 day | Restore from backup |
| Full rebuild | 4-8 hrs | N/A | Re-run all pipelines |
