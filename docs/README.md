# Documentation Hub'Eau Pipeline

## 📖 Guide de Démarrage

- **[README principal](../README.md)** - Installation et utilisation rapide
- **[Configuration](CONFIGURATION.md)** - Variables d'environnement et setup

## 🏗️ Architecture & Design

- **[Architecture](ARCHITECTURE.md)** - Architecture détaillée du pipeline
- **[Schéma BDD](SCHEMA_BDD.md)** - Structure des tables PostgreSQL
- **[Stockage ERA5](ERA5_DATA_STORAGE.md)** - Détails sur le stockage ERA5
- **[Intégration BDLISA](BDLISA_INTEGRATION.md)** - Référentiels, TME et nomenclatures

## 🔧 Utilisation

### Workflow Standard

1. **Ingestion** : Lancer les jobs DLT pour charger les données brutes dans `bronze`
2. **Transformation** : Lancer le job dbt pour créer `silver` et `gold`
3. **Analyse** : Utiliser `gold.hubeau_daily_chroniques` pour vos analyses

Pour les procédures d’exploitation : voir [runbook.md](runbook.md).

### Exemples de Requêtes

Voir [SCHEMA_BDD.md](SCHEMA_BDD.md#requêtes-courantes) pour des exemples de requêtes SQL.

## 🚀 Déploiement

### Local
```bash
docker compose up -d --build
```

### Production
Voir [CONFIGURATION.md](CONFIGURATION.md#production) pour les détails de déploiement en production.

## 📊 Interfaces

- **Dagster UI** : http://localhost:49500 - Orchestration et monitoring
- **Adminer** : http://localhost:49501 - Interface PostgreSQL

## 🔗 Ressources Externes

- [Hub'Eau](https://hubeau.eaufrance.fr) - APIs officielles
- [Dagster Docs](https://docs.dagster.io) - Documentation Dagster
- [DLT Docs](https://dlthub.com/docs) - Documentation DLT
- [dbt Docs](https://docs.getdbt.com) - Documentation dbt
