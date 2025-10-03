# Hub'Eau Data Integration - Description du Projet

## 🎯 Objectif

Ce projet vise à intégrer les données du BRGM (Bureau de Recherches Géologiques et Minières) disponibles via les APIs Hub'Eau dans un entrepôt de données moderne et performant.

## 📊 Données Intégrées

Le projet intègre **8 APIs Hub'Eau** couvrant l'ensemble du domaine de l'eau en France :

- **Hydrométrie** : Niveaux et débits des cours d'eau
- **Piézométrie** : Niveaux des nappes souterraines  
- **Qualité des cours d'eau** : Analyses physico-chimiques
- **Qualité des eaux souterraines** : Analyses des nappes
- **Température** : Température des cours d'eau
- **Écoulement** : Observations d'écoulement (ONDE)
- **Hydrobiologie** : Indices et taxons biologiques
- **Prélèvements** : Chroniques de prélèvements

## 🏗️ Architecture Technique

### Entrepôt de Données Medallion
- **Bronze Layer** : Données brutes Hub'Eau (MinIO)
- **Silver Layer** : Données transformées (TimescaleDB, PostGIS, Neo4j)
- **Gold Layer** : Données analytiques et services

### Stack Technologique
- **Orchestration** : Dagster (pipelines, monitoring, partitions)
- **Data Loading** : DLT (Data Load Tool) avec slicing intelligent
- **Stockage** : MinIO (S3-compatible)
- **Time Series** : TimescaleDB (données temporelles)
- **Géospatial** : PostGIS (données spatiales)
- **Graph** : Neo4j (relations entre entités)

## 🎯 Objectifs Métier

### 1. Wrapper Pérenne
Développer une solution robuste et maintenable pour l'intégration continue des données Hub'Eau, avec :
- Gestion automatique des partitions temporelles
- Fallbacks intelligents en cas de limites API
- Monitoring et alertes intégrés

### 2. Entrepôt de Données
Construire un entrepôt de données structuré permettant :
- Requêtes analytiques performantes
- Visualisations et dashboards
- Services de données pour applications

### 3. Performance et Fiabilité
- Gestion optimisée des volumes de données
- Filtrage intelligent des stations actives
- Respect des contraintes techniques des APIs

## 📈 Partitions et Fréquence

### Données Temps Réel (Daily)
- **Hydrométrie** : Observations des 30 derniers jours
- **Écoulement** : Observations saisonnières

### Données Historiques (Yearly)
- **Piézométrie** : Chroniques historiques
- **Qualité** : Analyses par année
- **Température** : Chroniques annuelles
- **Hydrobiologie** : Indices et taxons
- **Prélèvements** : Chroniques de prélèvements

## 🔧 Fonctionnalités Clés

### Ingestion Intelligente
- **Découpage adaptatif** : Gestion automatique des volumes de données
- **Fallbacks automatiques** : Stratégies de récupération en cas de limite
- **Rate limiting** : Respect des contraintes API Hub'Eau
- **Gestion d'erreurs** : Retry intelligent avec backoff exponentiel

### Monitoring et Observabilité
- **UI Dagster** : Interface de monitoring des pipelines
- **Logs détaillés** : Suivi des requêtes et performances
- **Métriques** : Volumes de données, durée d'exécution, erreurs
- **Alertes** : Détection automatique des problèmes

### Scalabilité
- **Architecture Docker** : Déploiement containerisé
- **Bases spécialisées** : TimescaleDB, PostGIS, Neo4j selon le type de données
- **Partitioning automatique** : Gestion des volumes importants

## 🎓 Contexte Académique

Ce projet est développé dans le cadre académique de l'**Université de Tours** et vise à :
- Contribuer à la recherche en sciences de l'eau
- Fournir un outil réutilisable pour la communauté scientifique
- Démonstrer les bonnes pratiques en ingénierie des données

## 📚 Documentation

- **[Tutoriel DLT](docs/TUTORIEL_DLT.md)** : Guide complet des configurations
- **[APIs Hub'Eau](docs/APIS_HUBEAU_COMPLETE.md)** : Documentation des sources
- **[Architecture Moderne](docs/ARCHITECTURE_MODERNE.md)** : Choix techniques

## 🚀 Déploiement

Le projet est déployé via **Docker Compose** avec :
- Services orchestrés (Dagster, MinIO, TimescaleDB, PostGIS, Neo4j)
- Configuration centralisée
- Monitoring intégré
- Documentation complète

## 📄 Licence

Projet développé dans le cadre académique de l'Université de Tours.
