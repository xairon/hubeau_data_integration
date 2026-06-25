# Configuration

Variables d'environnement et paramétrage du pipeline. Les valeurs se définissent dans un
fichier `.env` à la racine (modèle : `.env.example`), lu par `docker-compose.yml`.

## Variables d'environnement

### PostgreSQL — base de données

| Variable | Description | Défaut | Obligatoire |
|----------|-------------|--------|-------------|
| `PG_PASSWORD` | Mot de passe PostgreSQL | — | Oui |
| `PG_HOST` | Hôte PostgreSQL | `postgres` | Oui |
| `PG_PORT` | Port (interne) | `5432` | Oui |
| `PG_DB` | Nom de la base | `postgres` | Oui |
| `PG_USER` | Utilisateur | `postgres` | Oui |

Les extensions (`postgis`, `timescaledb`) sont activées par `docker/postgres/init.sql`.

### Dagster — orchestration

| Variable | Description | Défaut | Obligatoire |
|----------|-------------|--------|-------------|
| `DAGSTER_PG_PASSWORD` | Mot de passe de la base métadonnées Dagster | — | Oui |
| `DAGSTER_ENABLE_SCHEDULES` | Active les schedules (ingestion) | `false` | Non |
| `DAGSTER_ENABLE_SENSORS` | Active les sensors (chaîne dbt) | `false` | Non |

En production, passer `DAGSTER_ENABLE_SCHEDULES` et `DAGSTER_ENABLE_SENSORS` à `true`.

### ERA5 — Copernicus

| Variable | Description | Défaut | Obligatoire |
|----------|-------------|--------|-------------|
| `COPERNICUS_API_KEY` | Clé API Copernicus CDS (ingestion ERA5) | — | Oui (pour ERA5) |
| `ERA5_AVAILABILITY_LAG_DAYS` | Jours retirés à « aujourd'hui » pour la date de fin ERA5 | `5` | Non |

Copernicus publie ERA5-Land avec quelques jours de latence : le job ERA5 ne charge que
jusqu'à `(aujourd'hui − ERA5_AVAILABILITY_LAG_DAYS)`.

### DLT — ingestion

Les variables DLT sont dérivées automatiquement des variables PostgreSQL
(`DESTINATION__POSTGRES__CREDENTIALS__*` ← `PG_*`). Rien à configurer manuellement.

### Bootstrap — chargement initial contrôlé

| Variable | Description | Défaut |
|----------|-------------|--------|
| `BOOTSTRAP_PARTITIONS` | Allowlist de partitions à charger (`job:partition`) | (vide = tout) |
| `BOOTSTRAP_FORCE_RERUN` | Ignore l'état de complétion et relance | `false` |
| `BOOTSTRAP_CONTINUE_ON_ERROR` | Continue après erreur (best-effort) | `false` |

Exemple — limiter le bootstrap à la piézométrie 2020 et ERA5 1990-1991 :

```bash
BOOTSTRAP_PARTITIONS=chroniques:piezometry:2020,era5:1990-1991
```

## Fichier `.env`

```bash
cp .env.example .env
# Éditer .env : mots de passe et clé Copernicus
docker compose up -d --build
```

Le `.env` est dans `.gitignore` et n'est jamais committé.

## Retraitement dbt (`--vars`)

Paramètres passés à un `dbt run` ciblé pour rejouer une fenêtre de données.

```bash
# Piézométrie depuis une date
dbt run --select stg_piezo_chroniques --vars '{"piezometry_reprocess_from_date": "2020-01-01"}'

# Hydrométrie depuis une date
dbt run --select stg_hydrometry_obs_elab --vars '{"hydrometry_reprocess_from_date": "2020-01-01"}'

# ERA5 depuis un timestamp
dbt run --select stg_era5_timeseries --vars '{"era5_reprocess_from_timestamp": "2020-01-01 00:00:00"}'

# Recalcul complet du mapping station ↔ ERA5
dbt run --select int_station_era5_mapping+ --vars '{"recompute_station_era5_mapping": true}'
```

## Production

### Secrets CI/CD (GitLab)

Définir dans **Settings → CI/CD → Variables** (Protected + Masked) :
`PG_PASSWORD`, `DAGSTER_PG_PASSWORD`, `COPERNICUS_API_KEY`.

### Base de données externe

Pour utiliser une base PostgreSQL managée plutôt que le conteneur local : renseigner
`PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD` vers la base cible et retirer le
service `postgres` de `docker-compose.yml`. La base doit fournir PostGIS et TimescaleDB.

### Utilisateur en lecture seule

```bash
bash scripts/create_readonly_user.sh
```

## Bonnes pratiques de sécurité

- Mots de passe forts (≥ 16 caractères), distincts par service, jamais en clair dans le code.
- Stockage : `.env` (local, gitignored) ou gestionnaire de secrets (production).
- Rotation tous les 90 jours.
- TLS sur les connexions exposées ; privilégier des accès en lecture seule.
