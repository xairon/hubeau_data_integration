# Guide de Déploiement Local Hub'Eau Pipeline

## Prérequis

- **Docker Desktop** installé et lancé
- **8GB de RAM minimum** disponible pour Docker
- **20GB d'espace disque** libre
- **Ports disponibles** : 3001 (Dagster), 5433 (PostgreSQL)

## Configuration Docker Desktop

1. **Allouer plus de ressources** (Paramètres → Resources) :
   - CPUs : 4+ cores
   - Memory : 8GB minimum (12GB recommandé)
   - Disk image size : 50GB+

## Démarrage rapide

### 1. Lancer l'environnement

```bash
# Windows
.\start_local.bat

# Linux/Mac
docker-compose -f docker-compose.local.yml up -d
```

### 2. Accéder aux services

- **Dagster UI** : http://localhost:3001
- **PostgreSQL** : `localhost:5433` (user: `hubeau_user`, pass: `hubeau_password_local`)

### 3. Vérifier le statut

```bash
docker-compose -f docker-compose.local.yml ps
```

## Architecture locale

```
┌─────────────────────────────────────────────────────────────┐
│                     Machine Locale                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│  │  PostgreSQL  │    │   Dagster    │    │  DLT Worker  │ │
│  │     4GB      │    │   Daemon     │    │     8GB      │ │
│  │   Port 5433  │    │     4GB      │    │   4 CPUs     │ │
│  └──────────────┘    └──────────────┘    └──────────────┘ │
│                                                             │
│  ┌──────────────┐                                          │
│  │   Dagster    │                                          │
│  │  Webserver   │                                          │
│  │     2GB      │                                          │
│  │  Port 3001   │                                          │
│  └──────────────┘                                          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Commandes utiles

### Logs en temps réel
```bash
# Tous les services
docker-compose -f docker-compose.local.yml logs -f

# Service spécifique
docker-compose -f docker-compose.local.yml logs -f dagster_webserver
docker-compose -f docker-compose.local.yml logs -f dlt_worker
```

### Redémarrer un service
```bash
docker-compose -f docker-compose.local.yml restart dlt_worker
```

### Accès shell dans un container
```bash
docker exec -it hubeau_dlt_worker_local bash
docker exec -it hubeau_postgres_local psql -U hubeau_user -d hubeau
```

### Reset complet
```bash
# Windows
.\stop_local.bat
# Puis répondre Y pour supprimer les volumes

# Linux/Mac
docker-compose -f docker-compose.local.yml down -v
```

## Lancer un job Dagster

1. Ouvrir http://localhost:3001
2. Aller dans "Assets"
3. Sélectionner les assets à matérialiser
4. Cliquer "Materialize"

### Via CLI
```bash
docker exec hubeau_dagster_daemon_local dagster asset materialize \
  -m hubeau_pipeline \
  --select "hydrobio_stations_csv"
```

## Configuration avancée

### Modifier les ressources

Éditer `docker-compose.local.yml` :

```yaml
deploy:
  resources:
    limits:
      memory: 12g  # Augmenter la RAM
      cpus: '6'    # Plus de CPUs
```

### Variables d'environnement

Éditer `.env.local` pour ajuster :
- `MAX_WORKERS` : Nombre de workers parallèles
- `BATCH_SIZE` : Taille des batches d'insertion
- `CHUNK_SIZE` : Taille des chunks CSV

## Monitoring

### Utilisation mémoire
```bash
docker stats
```

### Performances PostgreSQL
```sql
-- Dans PostgreSQL
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'hubeau'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Santé des services
```bash
# Check health status
docker-compose -f docker-compose.local.yml ps --format "table {{.Name}}\t{{.Status}}"
```

## Troubleshooting

### Service ne démarre pas
```bash
# Voir les logs
docker-compose -f docker-compose.local.yml logs [service_name]

# Reconstruire l'image
docker-compose -f docker-compose.local.yml build --no-cache [service_name]
```

### Out of Memory
1. Augmenter la RAM Docker Desktop
2. Réduire `MAX_WORKERS` dans `.env.local`
3. Diminuer `BATCH_SIZE` et `CHUNK_SIZE`

### Port déjà utilisé
Modifier les ports dans `docker-compose.local.yml` :
```yaml
ports:
  - "3002:3000"  # Changer 3001 en 3002
```

### Reset base de données
```sql
docker exec hubeau_postgres_local psql -U hubeau_user -d hubeau -c "DROP SCHEMA hubeau CASCADE;"
docker exec hubeau_postgres_local psql -U hubeau_user -d hubeau -c "CREATE SCHEMA hubeau;"
```

## Différences avec production

| Aspect | Local | Production |
|--------|-------|------------|
| RAM Worker | 8GB | 4.5GB |
| RAM PostgreSQL | 4GB | 2.5GB |
| Workers parallèles | 4 | 2 |
| Batch size | 5000 | 1000 |
| Port Dagster | 3001 | 3000 |
| Port PostgreSQL | 5433 | 5432 |

## Support

Pour toute question ou problème :
1. Vérifier les logs : `docker-compose -f docker-compose.local.yml logs`
2. Consulter la documentation Dagster : https://docs.dagster.io
3. Voir les issues GitHub du projet