# Documentation des APIs Hub'Eau Intégrées

## Vue d'Ensemble

Ce document décrit les 8 APIs Hub'Eau intégrées dans notre pipeline. Les informations sont basées sur les configurations réelles du projet.

## APIs Intégrées

### 1. Hydrométrie
- **Base URL** : `https://hubeau.eaufrance.fr/api/v2/hydrometrie`
- **Endpoints** : `/stations`, `/observations_tr`
- **Partitioning** : Daily
- **Rate Limit** : 2.0 RPS

### 2. Piézométrie
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/niveaux_nappes`
- **Endpoints** : `/stations`, `/chroniques`
- **Partitioning** : Yearly
- **Rate Limit** : 1.5 RPS
- **Clé primaire** : `code_bss`

### 3. Qualité Cours d'Eau
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_rivieres`
- **Endpoints** : `/station_pc`, `/analyses`
- **Partitioning** : Yearly
- **Rate Limit** : 1.5 RPS

### 4. Qualité Eaux Souterraines
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_nappes`
- **Endpoints** : `/stations`, `/analyses`
- **Partitioning** : Yearly
- **Rate Limit** : 1.5 RPS
- **Clé primaire** : `code_bss`

### 5. Température
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/temperature`
- **Endpoints** : `/station`, `/chronique`
- **Partitioning** : Yearly
- **Rate Limit** : 0.7 RPS

### 6. Écoulement (ONDE)
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/ecoulement`
- **Endpoints** : `/stations`, `/observations`
- **Partitioning** : Daily
- **Rate Limit** : 1.5 RPS

### 7. Hydrobiologie
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/indicateurs_services`
- **Endpoints** : `/stations_hydrobio`, `/indices`, `/taxons`
- **Partitioning** : Yearly
- **Rate Limit** : 1.7 RPS (indices), 0.6 RPS (taxons)
- **Clé primaire** : `code_station_hydrobio`

### 8. Prélèvements
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/prelevements`
- **Endpoints** : `/points_prelevement`, `/chroniques`
- **Partitioning** : Yearly
- **Rate Limit** : 1.0 RPS
- **Clé primaire** : `code_point_prelevement`

## Configuration Commune

### Paramètres Généraux
- **Format** : JSON
- **Page Size** : 20,000 records (optimisé)
- **Timeout** : 60 secondes
- **Concurrence** : 1 requête simultanée par API
- **Truncation Threshold** : 20,000 records

### Pagination
- **Type** : Page-based (APIs v1) ou Cursor-based (APIs v2)
- **Condition d'arrêt** : `len($.data) == 0`

### Retry Strategy
- **Backoff initial** : 2.0 secondes
- **Backoff maximum** : 120.0 secondes

## Documentation Officielle

Pour plus de détails sur chaque API, consultez la documentation officielle Hub'Eau :
- [Documentation générale](https://hubeau.eaufrance.fr/page/api)
- [Hydrométrie](https://hubeau.eaufrance.fr/page/api-hydrometrie)
- [Piézométrie](https://hubeau.eaufrance.fr/page/api-niveaux-nappes)
- [Qualité](https://hubeau.eaufrance.fr/page/api-qualite-eau)
- [Température](https://hubeau.eaufrance.fr/page/api-temperature)
- [Écoulement](https://hubeau.eaufrance.fr/page/api-ecoulement)
- [Hydrobiologie](https://hubeau.eaufrance.fr/page/api-indicateurs-services)
- [Prélèvements](https://hubeau.eaufrance.fr/page/api-prelevements)