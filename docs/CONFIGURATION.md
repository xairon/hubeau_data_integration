# Configuration

Guide de configuration des variables d'environnement et du déploiement.

## Variables d'Environnement

### PostgreSQL - Base de données principale

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `PG_HOST` | Hôte PostgreSQL | `postgres` | ✅ |
| `PG_PORT` | Port PostgreSQL | `5432` | ✅ |
| `PG_DB` | Nom de la base | `postgres` | ✅ |
| `PG_USER` | Utilisateur | `postgres` | ✅ |
| `PG_PASSWORD` | Mot de passe | `REDACTED` | ✅ |
| `POSTGRES_EXTENSIONS` | Extensions à activer | `postgis,timescaledb` | (Géré par init.sql) |

### Dagster - Orchestration

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `DAGSTER_PG_HOST` | Hôte PostgreSQL Dagster | `dagster_postgres` | ✅ |
| `DAGSTER_PG_PORT` | Port PostgreSQL Dagster | `5432` | ✅ |
| `DAGSTER_PG_DB` | Base Dagster | `dagster` | ✅ |
| `DAGSTER_PG_USER` | Utilisateur Dagster | `postgres` | ✅ |
| `DAGSTER_PG_PASSWORD` | Mot de passe Dagster | `REDACTED` | ✅ |
| `DAGSTER_HOME` | Répertoire Dagster | `/app/dagster_home` | ❌ |

### Superset - Visualisation

| Variable | Description | Valeur par défaut | Obligatoire |
|----------|-------------|-------------------|-------------|
| `SUPERSET_SECRET_KEY` | Clé secrète Flask | `your-secret-key...` | ⚠️ CRITIQUE en Prod |
| `SUPERSET_SQLALCHEMY_DATABASE_URI` | Base métadonnées Superset | `postgresql://...` | ✅ |
| `REDIS_HOST` | Hôte Redis (Cache) | `redis` | ✅ |

### DLT - Ingestion

Les variables DLT sont automatiquement dérivées des variables PostgreSQL :

| Variable | Source |
|----------|--------|
| `DESTINATION__POSTGRES__CREDENTIALS__HOST` | `PG_HOST` |
| `DESTINATION__POSTGRES__CREDENTIALS__PORT` | `PG_PORT` |
| `DESTINATION__POSTGRES__CREDENTIALS__DATABASE` | `PG_DB` |
| `DESTINATION__POSTGRES__CREDENTIALS__USERNAME` | `PG_USER` |
| `DESTINATION__POSTGRES__CREDENTIALS__PASSWORD` | `PG_PASSWORD` |

## Configuration Locale

### Fichier `.env`

Créer un fichier `.env` à la racine du projet :

```bash
# PostgreSQL - Données
PG_HOST=postgres
PG_PORT=5432
PG_DB=postgres
PG_USER=postgres
PG_PASSWORD=VotreMotDePasseSecurise123!

# PostgreSQL - Dagster
DAGSTER_PG_HOST=dagster_postgres
DAGSTER_PG_PORT=5432
DAGSTER_PG_DB=dagster
DAGSTER_PG_USER=postgres
DAGSTER_PG_PASSWORD=VotreMotDePasseDagster123!
```

**Note** : Le fichier `.env` est dans `.gitignore` et ne sera pas commité.

### Démarrage

```bash
# Avec .env personnalisé
docker compose up -d --build

# Sans .env (utilise les valeurs par défaut)
docker compose up -d --build
```

## Configuration Production

### GitLab CI/CD Variables

Dans GitLab, aller dans **Settings → CI/CD → Variables** et définir :

| Variable | Type | Options |
|----------|------|---------|
| `PG_PASSWORD` | Variable | Protected ✅, Masked ✅ |
| `DAGSTER_PG_PASSWORD` | Variable | Protected ✅, Masked ✅ |

Les autres variables utilisent les valeurs par défaut ou sont définies dans le `docker-compose.yml` de production.

### Base de Données Externe

Si vous utilisez une base PostgreSQL externe (AWS RDS, etc.) :

```bash
# Dans GitLab CI/CD Variables
PG_HOST=database.example.com
PG_PORT=5432
PG_DB=hubeau_prod
PG_USER=hubeau_user
PG_PASSWORD=<strong-password>
```

**Important** : Supprimer le service `postgres` de `docker-compose.yml` si vous utilisez une base externe.

## Vérification

### Vérifier les Variables

```bash
# Dans un container
docker exec brgm-dlt-worker env | grep PG_

# Tester connexion PostgreSQL
docker exec brgm-postgres psql -U postgres -c "SELECT version();"
```

### Vérifier les Logs

```bash
# Logs worker
docker compose logs -f dlt_worker

# Logs webserver
docker compose logs -f dagster_webserver

# Logs PostgreSQL
docker compose logs -f postgres
```

## Sécurité

### Bonnes Pratiques

1. **Mots de passe forts** :
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

### Utilisateur Readonly

Pour créer un utilisateur en lecture seule :

```bash
# Exécuter le script
bash scripts/create_readonly_user.sh

# Ou manuellement via psql
docker exec -it brgm-postgres psql -U postgres -d postgres
```

## Dépannage

### Erreur : "PG_PASSWORD not set"

**Solution** : Vérifier que `.env` existe et contient `PG_PASSWORD`, ou utiliser les valeurs par défaut.

### Erreur : "could not translate host name"

**Solution** : Vérifier que le nom d'hôte correspond au nom du service Docker (ex: `postgres` pour le service local).

### Erreur : "password authentication failed"

**Solution** :
1. Vérifier le password dans `.env` ou GitLab Variables
2. Si changé, supprimer le volume PostgreSQL et recréer :
   ```bash
   docker compose down -v
   docker compose up -d
   ```

### Message : "Database directory appears to contain a database"

✅ **Ce message est NORMAL** : PostgreSQL détecte que la base existe déjà et skip l'initialisation. C'est le comportement attendu.

## Exemples

### Configuration Minimale (Dev)

```bash
# .env
PG_PASSWORD=dev123456
DAGSTER_PG_PASSWORD=dev123456
```

### Configuration Complète (Production)

```bash
# .env ou GitLab Variables
PG_HOST=postgres
PG_PORT=5432
PG_DB=hubeau_prod
PG_USER=hubeau_admin
PG_PASSWORD=xK9#mP2$vL5@nQ8!

DAGSTER_PG_HOST=dagster_postgres
DAGSTER_PG_PORT=5432
DAGSTER_PG_DB=dagster
DAGSTER_PG_USER=dagster_admin
DAGSTER_PG_PASSWORD=yR7&tW4*hN9%sF3@
```
