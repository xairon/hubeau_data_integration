# Environment Configuration Guide

Ce guide explique comment configurer les variables d'environnement pour différents scénarios de déploiement.

## 📋 Table des matières

- [Variables disponibles](#variables-disponibles)
- [Scénarios de déploiement](#scénarios-de-déploiement)
- [Exemples de configuration](#exemples-de-configuration)

## Variables disponibles

### 🔹 Dagster (Orchestration)

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `DAGSTER_PG_HOST` | Hostname PostgreSQL Dagster | `dagster_postgres` | ✅ |
| `DAGSTER_PG_PORT` | Port PostgreSQL Dagster | `5432` | ✅ |
| `DAGSTER_PG_DB` | Base de données Dagster | `dagster` | ✅ |
| `DAGSTER_PG_USER` | Utilisateur PostgreSQL | `postgres` | ✅ |
| `DAGSTER_PG_PASSWORD` | Mot de passe PostgreSQL | - | ✅ |

### 🔹 PostgreSQL (Données relationnelles)

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `PG_HOST` | Hostname PostgreSQL | `postgres` | ✅ |
| `PG_PORT` | Port PostgreSQL | `5432` | ✅ |
| `PG_DB` | Base de données | `postgres` | ✅ |
| `PG_USER` | Utilisateur PostgreSQL | `postgres` | ✅ |
| `PG_PASSWORD` | Mot de passe PostgreSQL | - | ✅ |

### 🔹 PostGIS (Données géospatiales)

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `POSTGIS_HOST` | Hostname PostGIS | `postgis` | ✅ |
| `POSTGIS_PORT` | Port PostGIS | `5432` | ✅ |
| `POSTGIS_DB` | Base de données PostGIS | `postgres` | ✅ |
| `POSTGIS_USER` | Utilisateur PostGIS | `postgres` | ✅ |
| `POSTGIS_PASSWORD` | Mot de passe PostGIS | - | ✅ |

### 🔹 MinIO / S3 (Object Storage)

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `MINIO_ENDPOINT` | URL du endpoint S3 | `http://minio:9000` | ✅ |
| `MINIO_USER` | Access Key ID | `admin` | ✅ |
| `MINIO_PASS` | Secret Access Key | - | ✅ |
| `MINIO_REGION` | Région S3 | `us-east-1` | ✅ |
| `MINIO_BRONZE_BUCKET` | Bucket Bronze layer | `bronze` | ✅ |

### 🔹 Monitoring (Optionnel)

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `GRAFANA_PASSWORD` | Mot de passe admin Grafana | `admin` | ❌ |
| `PROMETHEUS_PORT` | Port Prometheus | `9090` | ❌ |

## Scénarios de déploiement

### 1️⃣ Développement local (Docker Compose)

**Configuration** : Tous les services en local via Docker Compose

```bash
# Dagster
DAGSTER_PG_HOST=dagster_postgres
DAGSTER_PG_PORT=5432
DAGSTER_PG_PASSWORD=BrgmDagster2024!

# Postgres
PG_HOST=postgres
PG_PORT=5432
PG_PASSWORD=BrgmPostgres2024!

# PostGIS
POSTGIS_HOST=postgis
POSTGIS_PORT=5432
POSTGIS_PASSWORD=BrgmPostgres2024!

# MinIO
MINIO_ENDPOINT=http://minio:9000
MINIO_USER=admin
MINIO_PASS=BrgmMinio2024!
```

**Commandes** :
```bash
cp .env.template .env
# Éditer .env avec vos passwords
docker-compose up -d
```

---

### 2️⃣ Production VPS (Docker Compose)

**Configuration** : Identique au dev local mais avec des passwords sécurisés

```bash
# Même configuration que dev local mais avec :
DAGSTER_PG_PASSWORD=<strong-password-1>
PG_PASSWORD=<strong-password-2>
POSTGIS_PASSWORD=<strong-password-2>
MINIO_PASS=<strong-password-3>
```

**Déploiement** : Via GitLab CI/CD (variables configurées dans GitLab)

---

### 3️⃣ Production Hybride (MinIO externe + Databases locales)

**Scénario** : Utiliser AWS S3 ou Scaleway Object Storage au lieu de MinIO local

```bash
# Dagster - Local
DAGSTER_PG_HOST=dagster_postgres
DAGSTER_PG_PORT=5432
DAGSTER_PG_PASSWORD=<password>

# Postgres - Local
PG_HOST=postgres
PG_PORT=5432
PG_PASSWORD=<password>

# PostGIS - Local
POSTGIS_HOST=postgis
POSTGIS_PORT=5432
POSTGIS_PASSWORD=<password>

# AWS S3 - Externe
MINIO_ENDPOINT=https://s3.eu-west-3.amazonaws.com
MINIO_USER=AKIAIOSFODNN7EXAMPLE
MINIO_PASS=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
MINIO_REGION=eu-west-3
MINIO_BRONZE_BUCKET=hubeau-bronze-prod
```

**Modifications docker-compose** :
```yaml
# Supprimer le service minio de docker-compose.yml
# Garder les autres services
```

---

### 4️⃣ Production Cloud-Native (Tout externe)

**Scénario** : Bases de données managées (AWS RDS, Scaleway DB) + S3

```bash
# Dagster - AWS RDS PostgreSQL
DAGSTER_PG_HOST=dagster-prod.xxxx.eu-west-3.rds.amazonaws.com
DAGSTER_PG_PORT=5432
DAGSTER_PG_DB=dagster
DAGSTER_PG_USER=dagster_admin
DAGSTER_PG_PASSWORD=<rds-password>

# Postgres - AWS RDS
PG_HOST=hubeau-postgres.xxxx.eu-west-3.rds.amazonaws.com
PG_PORT=5432
PG_DB=hubeau_data
PG_USER=hubeau_admin
PG_PASSWORD=<rds-password>

# PostGIS - AWS RDS with PostGIS extension
POSTGIS_HOST=hubeau-postgis.xxxx.eu-west-3.rds.amazonaws.com
POSTGIS_PORT=5432
POSTGIS_DB=hubeau_geo
POSTGIS_USER=hubeau_admin
POSTGIS_PASSWORD=<rds-password>

# AWS S3
MINIO_ENDPOINT=https://s3.eu-west-3.amazonaws.com
MINIO_USER=AKIAIOSFODNN7EXAMPLE
MINIO_PASS=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
MINIO_REGION=eu-west-3
MINIO_BRONZE_BUCKET=hubeau-bronze-prod
```

**Déploiement** :
- Dagster sur Kubernetes ou ECS
- Aucun service de base de données dans docker-compose
- Seulement les workers DLT

---

### 5️⃣ Environnement de Staging

**Scénario** : Tester avec des services externes avant la production

```bash
# Databases - Instances dédiées de staging
PG_HOST=postgres-staging.internal
PG_PASSWORD=<staging-password>

POSTGIS_HOST=postgis-staging.internal
POSTGIS_PASSWORD=<staging-password>

# Object Storage - Bucket dédié
MINIO_ENDPOINT=https://s3.eu-west-3.amazonaws.com
MINIO_BRONZE_BUCKET=hubeau-bronze-staging  # ← Bucket séparé
MINIO_USER=<aws-key>
MINIO_PASS=<aws-secret>
```

---

## Exemples de configuration

### Exemple 1: Scaleway Object Storage

```bash
MINIO_ENDPOINT=https://s3.fr-par.scw.cloud
MINIO_USER=<scaleway-access-key>
MINIO_PASS=<scaleway-secret-key>
MINIO_REGION=fr-par
MINIO_BRONZE_BUCKET=hubeau-bronze
```

### Exemple 2: DigitalOcean Spaces

```bash
MINIO_ENDPOINT=https://fra1.digitaloceanspaces.com
MINIO_USER=<do-access-key>
MINIO_PASS=<do-secret-key>
MINIO_REGION=fra1
MINIO_BRONZE_BUCKET=hubeau-bronze
```

### Exemple 3: MinIO local accessible depuis l'extérieur

```bash
# Accès depuis le host Docker
MINIO_ENDPOINT=http://localhost:9000

# Accès depuis un autre serveur
MINIO_ENDPOINT=http://192.168.1.100:9000
```

### Exemple 4: Plusieurs environnements PostgreSQL

```bash
# Dev
PG_HOST=localhost
PG_PORT=5432

# Staging
PG_HOST=postgres-staging.company.com
PG_PORT=5432

# Production
PG_HOST=postgres-prod.company.com
PG_PORT=5432
```

---

## 🔐 Bonnes pratiques de sécurité

### Passwords

- **Minimum 16 caractères**
- Mélange de majuscules, minuscules, chiffres, symboles
- Différents pour chaque service
- Stockés dans un password manager

### Secrets Management

**Local** :
```bash
# .env - gitignored
cp .env.template .env
# Éditer .env
```

**GitLab CI/CD** :
```
Settings > CI/CD > Variables
- Protected: ✅
- Masked: ✅ (sauf MINIO_USER si = "admin")
```

**Production** (alternatives):
- AWS Secrets Manager
- HashiCorp Vault
- Kubernetes Secrets

### Rotation des credentials

Changer les passwords régulièrement :
1. Générer nouveau password
2. Mettre à jour dans GitLab CI/CD
3. Redéployer l'application
4. Vérifier que tout fonctionne
5. Supprimer l'ancien password

---

## 🔍 Debugging

### Vérifier les variables d'environnement

```bash
# Dans un container Docker
docker exec dlt_worker env | grep MINIO
docker exec dagster_webserver env | grep DAGSTER_PG
```

### Tester la connexion MinIO

```bash
# Avec awscli
aws --endpoint-url=$MINIO_ENDPOINT \
    s3 ls s3://$MINIO_BRONZE_BUCKET

# Avec curl
curl -I $MINIO_ENDPOINT/minio/health/live
```

### Tester la connexion PostgreSQL

```bash
# Avec psql
PGPASSWORD=$PG_PASSWORD psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -c "SELECT version();"
```

---

## 📚 Ressources

- [.env.template](../.env.template) - Template avec toutes les variables
- [GITLAB_CI_SETUP.md](../GITLAB_CI_SETUP.md) - Configuration GitLab CI/CD
- [docker-compose.yml](../docker-compose.yml) - Configuration Docker locale
- [docker-compose.production.yml](../docker-compose.production.yml) - Configuration production
