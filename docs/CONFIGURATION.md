# Configuration des Variables d'Environnement

## Vue d'ensemble

Ce guide détaille la configuration des variables d'environnement pour le projet Hub'Eau Data Pipeline.

## Variables d'environnement

### PostgreSQL - Base de données principale

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `PG_HOST` | Hôte PostgreSQL | `postgres` | ✅ |
| `PG_PORT` | Port PostgreSQL | `5432` | ✅ |
| `PG_DB` | Nom de la base | `postgres` | ✅ |
| `PG_USER` | Utilisateur | `postgres` | ✅ |
| `PG_PASSWORD` | Mot de passe | - | ✅ |
| `HUBEAU_SCHEMA` | Schema Hub'Eau | `hubeau` | ❌ |

### Dagster - Orchestration

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `DAGSTER_PG_HOST` | Hôte PostgreSQL Dagster | `dagster_postgres` | ✅ |
| `DAGSTER_PG_PORT` | Port PostgreSQL Dagster | `5432` | ✅ |
| `DAGSTER_PG_DB` | Base Dagster | `dagster` | ✅ |
| `DAGSTER_PG_USER` | Utilisateur Dagster | `postgres` | ✅ |
| `DAGSTER_PG_PASSWORD` | Mot de passe Dagster | - | ✅ |
| `DAGSTER_HOME` | Répertoire Dagster | `/app/dagster_home` | ❌ |

### DLT - Ingestion

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `DESTINATION__POSTGRES__CREDENTIALS__HOST` | Hôte destination DLT | `${PG_HOST}` | ✅ |
| `DESTINATION__POSTGRES__CREDENTIALS__PORT` | Port destination DLT | `${PG_PORT}` | ✅ |
| `DESTINATION__POSTGRES__CREDENTIALS__DATABASE` | Base destination DLT | `${PG_DB}` | ✅ |
| `DESTINATION__POSTGRES__CREDENTIALS__USERNAME` | User destination DLT | `${PG_USER}` | ✅ |
| `DESTINATION__POSTGRES__CREDENTIALS__PASSWORD` | Pass destination DLT | `${PG_PASSWORD}` | ✅ |

### Monitoring (Optionnel)

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `GRAFANA_PASSWORD` | Mot de passe admin Grafana | `admin` | ❌ |
| `PROMETHEUS_PORT` | Port Prometheus | `9090` | ❌ |

### Backfill (Optionnel)

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `FORCE_INITIAL_BACKFILL` | Force le backfill sur nouvelle installation | `false` | ❌ |

## Fichiers de configuration

### Développement local - `.env`

Créer un fichier `.env` à la racine du projet :

```bash
# Dagster Orchestrator Database
DAGSTER_PG_HOST=dagster_postgres
DAGSTER_PG_PORT=5432
DAGSTER_PG_DB=dagster
DAGSTER_PG_USER=postgres
DAGSTER_PG_PASSWORD=BrgmDagster2024!

# Data Storage - PostgreSQL
PG_HOST=postgres
PG_PORT=5432
PG_DB=postgres
PG_USER=postgres
PG_PASSWORD=BrgmPostgres2024!

# Hub'Eau Schema
HUBEAU_SCHEMA=hubeau

# Monitoring (optionnel)
GRAFANA_PASSWORD=admin
```

### Production - GitLab CI/CD Variables

Dans GitLab, aller dans **Settings → CI/CD → Variables** et définir :

| Variable | Type | Options |
|----------|------|---------|
| `PG_PASSWORD` | Variable | Protected ✅, Masked ✅ |
| `DAGSTER_PG_PASSWORD` | Variable | Protected ✅, Masked ✅ |

Les autres variables utilisent les valeurs par défaut définies dans `docker-compose.production.yml`.

## Scénarios de déploiement

### 1. Développement local

**Configuration** : Tous les services via Docker Compose

```bash
# 1. Copier le template
cp .env.example .env

# 2. Éditer .env avec vos passwords

# 3. Démarrer les services
docker-compose up -d
```

**Services démarrés** :
- PostgreSQL données (port 5432)
- PostgreSQL Dagster (interne)
- Dagster Webserver (port 8080)
- Dagster Daemon
- DLT Worker
- Adminer (port 8081)

### 2. Production VPS

**Configuration** : Déploiement automatique via GitLab CI/CD

Variables GitLab CI/CD requises :
- `PG_PASSWORD` : Password PostgreSQL données
- `DAGSTER_PG_PASSWORD` : Password PostgreSQL Dagster

**Déploiement** :
```bash
# Push sur main déclenche le déploiement
git push origin main
```

### 3. Production avec base externe

Si vous utilisez une base PostgreSQL externe (AWS RDS, etc.) :

```bash
# Dans GitLab CI/CD Variables
PG_HOST=database.example.com
PG_PORT=5432
PG_DB=hubeau_prod
PG_USER=hubeau_user
PG_PASSWORD=<strong-password>
```

Supprimer le service `postgres` de `docker-compose.production.yml`.

## Sécurité

### Bonnes pratiques

1. **Passwords forts** :
   - Minimum 16 caractères
   - Caractères spéciaux, chiffres, majuscules
   - Différents pour chaque service

2. **Stockage sécurisé** :
   - Local : `.env` (gitignored)
   - Production : GitLab CI/CD Variables (Protected + Masked)
   - Jamais dans le code ou les commits

3. **Rotation régulière** :
   - Changer les passwords tous les 3-6 mois
   - Mettre à jour dans GitLab Variables
   - Redéployer l'application

### Vérification des variables

```bash
# Vérifier dans un container
docker exec brgm-dlt-worker env | grep PG_

# Tester connexion PostgreSQL
docker exec brgm-postgres psql -U postgres -c "SELECT version();"

# Vérifier les logs
docker logs brgm-dlt-worker --tail 50
```

## Troubleshooting

### Erreur : "PG_PASSWORD not set"

**Solution** : Vérifier que `.env` existe et contient `PG_PASSWORD`

### Erreur : "could not translate host name"

**Solution** : Vérifier que le nom d'hôte dans la variable correspond au nom du service Docker

### Erreur : "password authentication failed"

**Solution** :
1. Vérifier le password dans `.env` ou GitLab Variables
2. Si changé, supprimer le volume PostgreSQL et recréer

## Migration depuis ancienne configuration

Si vous aviez une configuration avec MinIO/PostGIS séparés :

1. **Supprimer les anciennes variables** :
   - `MINIO_*`
   - `POSTGIS_*` (sauf si vous utilisez PostGIS séparé)

2. **Utiliser uniquement** :
   - `PG_*` pour PostgreSQL données
   - `DAGSTER_PG_*` pour PostgreSQL Dagster

3. **Nettoyer docker-compose** :
   - Supprimer services MinIO
   - Supprimer PostGIS si non utilisé

## Exemples de configuration

### Minimal (dev local)

```bash
# Seulement les passwords obligatoires
PG_PASSWORD=dev123456
DAGSTER_PG_PASSWORD=dev123456
```

### Complet (production)

```bash
# PostgreSQL
PG_HOST=postgres
PG_PORT=5432
PG_DB=hubeau_prod
PG_USER=hubeau_admin
PG_PASSWORD=xK9#mP2$vL5@nQ8!

# Dagster
DAGSTER_PG_HOST=dagster_postgres
DAGSTER_PG_PORT=5432
DAGSTER_PG_DB=dagster
DAGSTER_PG_USER=dagster_admin
DAGSTER_PG_PASSWORD=yR7&tW4*hN9%sF3@

# Options
HUBEAU_SCHEMA=hubeau
FORCE_INITIAL_BACKFILL=false
```

## Ressources

- [.env.example](../.env.example) - Template avec toutes les variables
- [docker-compose.yml](../docker-compose.yml) - Configuration locale
- [docker-compose.production.yml](../docker-compose.production.yml) - Configuration production
- [GITLAB_CI_VARIABLES_SETUP.md](GITLAB_CI_VARIABLES_SETUP.md) - Guide GitLab