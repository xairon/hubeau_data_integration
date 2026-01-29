# Hub'Eau Data Pipeline

Pipeline d'ingestion et de transformation de données hydrologiques françaises, orchestré par Dagster.

## 🚀 Démarrage Rapide

### Installation

```bash
# 1. Cloner le projet
git clone <repository-url>
cd brgm

# 2. (Optionnel) Créer un fichier .env pour personnaliser les mots de passe
cp .env.example .env
# Éditer .env avec vos mots de passe

# 3. Démarrer la stack
docker compose up -d --build

# 4. Attendre que tous les services soient healthy (30-60 secondes)
docker compose ps
```

### Accès aux Interfaces

| Service | URL | Description |
|---------|-----|-------------|
| **Dagster UI** | http://localhost:49500 | Orchestration et monitoring des pipelines |
| **Adminer** | http://localhost:49501 | Interface web PostgreSQL (léger) |
| **CloudBeaver** | http://localhost:49503 | Interface SQL Universelle (avancé) **(Voir .env pour identifiants)** |
| **Apache Superset** | http://localhost:49504 | Business Intelligence & Dashboards **(Voir .env pour identifiants)** |

**Identifiants Adminer** :
- Système : PostgreSQL
- Serveur : `postgres`
- Utilisateur : `postgres`
- Utilisateur : `postgres`
- Mot de passe : **(Défini dans `.env`)**
- Base de données : `postgres`

## 📊 Architecture & Vue d'Ensemble

## 📊 Architecture & Vue d'Ensemble

Le projet suit une architecture **Medallion** modernisée avec **TimescaleDB** pour la performance temporelle.

```
┌─────────────┐    ┌─────────────┐
│ Sources API │    │   Datalake  │
│ (Hub'Eau)   │───▶│   (Bronze)  │
└─────────────┘    └──────┬──────┘
                          │ (dbt + TimescaleDB)
                          ▼
                   ┌─────────────┐
                   │  Warehouse  │
                   │   (Gold)    │
                   └──────┬──────┘
                          │ (BI)
                          ▼
                   ┌─────────────┐
                   │  Superset   │
                   │ (Analytics) │
                   └─────────────┘
```

## ✨ Configuration Superset

- **Objectif à terme** : exploiter l’ensemble des données dans Superset (dashboards, cartes, calques : BDLISA, stations, chroniques, météo). Voir [docs/SUPERSET.md](docs/SUPERSET.md).
- 1. Crée le compte admin (Identifiants définis via variables d'environnement)
- 2. Connecte-toi (Identifiants définis dans `.env`)
- 3. La connexion **"Hub'Eau Data Warehouse"** est déjà là (tables gold + calques carto importés au démarrage).

## ✨ Fonctionnalités Clés

- **Ingestion Automatique** : Pipelines DLT résilients avec gestion de la pagination et des retries.
- **Performance TimeSeries** : Utilisation de **TimescaleDB** (Hypertables + Compression 90%) pour les chroniques.
- **Automation** : Sensors Dagster pour déclencher les transformations dbt dès l'arrivée des données.
- **Zero-Touch BI** : Stack de visualisation (Superset, CloudBeaver) pré-configurée et connectée.

---

##  Tables Principales

### Bronze (Raw Data)
Données brutes, partitionnées par année. Dédupliquées automatiquement.

### Silver (Clean Data)
Données nettoyées, typées, avec index spataux.

### Gold (Analytics Marts)
Tables optimisées pour le reporting et l'analyse.

| Table | Description | Granularité |
|-------|-------------|-------------|
| **`hubeau_daily_chroniques`** | **Fact Table Principale** (Piézo + Météo) | Jour |
| `fct_monthly_chroniques` | Agrégats mensuels et variations | Mois |
| `fct_yearly_stats` | Bilans annuels, hydrologie, classifications | Année |
| `agg_station_trends` | Analyse des tendances saisonnières (pentes) | Saison |
| `dim_piezo_stations` | Station master avec KPIs et alertes | Station |

## 🔧 Commandes Utiles

### Docker

```bash
# Vérifier l'état des services
docker compose ps

# Voir les logs
docker compose logs -f dlt_worker
docker compose logs -f dagster_webserver

# Redémarrer un service
docker compose restart dlt_worker

# Rebuild complet
docker compose down
docker compose build --no-cache
docker compose up -d

# Arrêter tout
docker compose down

# Supprimer les volumes (⚠️ supprime les données)
docker compose down -v
```

### PostgreSQL

```bash
# Se connecter à PostgreSQL
docker exec -it brgm-postgres psql -U postgres -d postgres

# Vérifier les tables
\dt bronze.*
\dt silver.*
\dt gold.*

# Compter les lignes
SELECT schemaname, tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY n_live_tup DESC;
```

## 📚 Documentation

- [Architecture détaillée](docs/ARCHITECTURE.md) - Architecture complète du système
- [Configuration](docs/CONFIGURATION.md) - Variables d'environnement et configuration
- [Schéma BDD](docs/SCHEMA_BDD.md) - Structure des tables PostgreSQL
- [Stockage ERA5](docs/ERA5_DATA_STORAGE.md) - Détails sur le stockage des données ERA5

## 🛠️ Technologies

| Composant | Version | Rôle |
|-----------|---------|------|
| Dagster | 1.11.14 | Orchestration |
| DLT | 0.4.12 | Ingestion |
| dbt | 1.7.0 | Transformation |
| PostgreSQL | 16 | Base de données |
| PostGIS | 3.4 | Extension géospatiale |
| CloudBeaver | 24.3 | Administration BDD |
| Superset | 4.0 | Business Intelligence |
| Redis | 7 | Cache (Superset) |

## 🐛 Dépannage

### Les services ne démarrent pas

```bash
# Vérifier les logs
docker compose logs

# Vérifier les ports (doivent être libres)
netstat -an | grep 49500
netstat -an | grep 49501
netstat -an | grep 49502
```

### Erreur "Module not found"

```bash
# Rebuild le worker
docker compose build --no-cache dlt_worker
docker compose up -d dlt_worker
```

### Erreur de connexion PostgreSQL

```bash
# Vérifier que PostgreSQL est healthy
docker compose ps postgres

# Vérifier les variables d'environnement
docker exec brgm-dlt-worker env | grep PG_
```

## 📝 Licence

MIT
