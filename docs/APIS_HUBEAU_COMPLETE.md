# Documentation des APIs Hub'Eau Intégrées

## Vue d'Ensemble

Ce document décrit les 8 APIs Hub'Eau intégrées dans notre pipeline. Les informations sont basées sur les configurations réelles du projet et les données observées.

## APIs Intégrées

### 1. Hydrométrie

#### Informations Générales
- **Base URL** : `https://hubeau.eaufrance.fr/api/v2/hydrometrie`
- **Version** : v2
- **Partitioning** : Daily
- **Rate Limit** : 2.0 RPS
- **Pagination** : Cursor-based

#### Endpoints Intégrés

##### `/stations`
- **Description** : Référentiel des stations hydrométriques
- **Méthode** : GET
- **Clé primaire** : `code_station`
- **Format** : JSON
- **Page Size** : 1,000 records
- **Slicing** : Global (pas de slicing)

##### `/observations_tr`
- **Description** : Observations hydrométriques temps réel
- **Méthode** : GET
- **Clés primaires** : `[code_station, date_obs, grandeur_hydro]`
- **Clé de réplication** : `date_obs`
- **Format** : JSON
- **Page Size** : 20,000 records
- **Pagination** : Cursor-based (`cursor` param, `$.next` path)
- **Slicing** : DateTime (fenêtre quotidienne)
- **Paramètres temporels** : `date_debut_obs`, `date_fin_obs`
- **Restriction** : 30 derniers jours uniquement

#### Attributs Clés
- `code_station` : Identifiant de la station
- `date_obs` : Date/heure d'observation
- `grandeur_hydro` : Type de grandeur (Q=débit, H=hauteur, N=niveau)
- `resultat_obs` : Valeur observée
- `code_qualification` : Qualité de la mesure

### 2. Piézométrie

#### Informations Générales
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/niveaux_nappes`
- **Version** : v1
- **Partitioning** : Yearly
- **Rate Limit** : 1.5 RPS
- **Pagination** : Page-based
- **Clé primaire** : `code_bss` (Bureau de Recherches Géologiques et Minières)

#### Endpoints Intégrés

##### `/stations`
- **Description** : Référentiel des stations piézométriques
- **Méthode** : GET
- **Clé primaire** : `code_bss`
- **Format** : JSON
- **Page Size** : 1,000 records
- **Slicing** : Global

##### `/chroniques`
- **Description** : Chroniques de niveaux de nappes
- **Méthode** : GET
- **Clés primaires** : `[code_bss, date_mesure]`
- **Clé de réplication** : `date_mesure`
- **Format** : JSON
- **Page Size** : 20,000 records
- **Pagination** : Page-based (`page` param)
- **Slicing** : Station Month (station par station × mois)
- **Paramètres temporels** : `date_debut_mesure`, `date_fin_mesure`
- **Fallback** : Day (découpage quotidien)

#### Attributs Clés
- `code_bss` : Identifiant de la station (BRGM)
- `date_mesure` : Date de mesure
- `niveau_nappe_eau` : Niveau de la nappe d'eau
- `profondeur_nappe` : Profondeur de la nappe
- `code_qualification` : Qualité de la mesure

### 3. Qualité Cours d'Eau

#### Informations Générales
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_rivieres`
- **Version** : v1
- **Partitioning** : Yearly
- **Rate Limit** : 1.5 RPS
- **Pagination** : Page-based

#### Endpoints Intégrés

##### `/station_pc`
- **Description** : Référentiel des stations de qualité cours d'eau
- **Méthode** : GET
- **Clé primaire** : `code_station`
- **Format** : JSON
- **Page Size** : 1,000 records
- **Slicing** : Global

##### `/analyses`
- **Description** : Analyses physico-chimiques des cours d'eau
- **Méthode** : GET
- **Clés primaires** : `[code_station, date_prelevement, code_parametre]`
- **Clé de réplication** : `date_prelevement`
- **Format** : JSON
- **Page Size** : 20,000 records
- **Pagination** : Page-based (`page` param)
- **Slicing** : Day (découpage quotidien)
- **Paramètres temporels** : `date_debut_prelevement`, `date_fin_prelevement`
- **Fallback** : Station Month

#### Attributs Clés
- `code_station` : Identifiant de la station
- `date_prelevement` : Date de prélèvement
- `code_parametre` : Code du paramètre analysé
- `resultat` : Valeur de l'analyse
- `code_qualification` : Qualité de l'analyse
- `libelle_parametre` : Libellé du paramètre

### 4. Qualité Eaux Souterraines

#### Informations Générales
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_nappes`
- **Version** : v1
- **Partitioning** : Yearly
- **Rate Limit** : 1.5 RPS
- **Pagination** : Page-based
- **Clé primaire** : `code_bss`

#### Endpoints Intégrés

##### `/stations`
- **Description** : Référentiel des stations de qualité eaux souterraines
- **Méthode** : GET
- **Clé primaire** : `code_bss`
- **Format** : JSON
- **Page Size** : 1,000 records
- **Slicing** : Global

##### `/analyses`
- **Description** : Analyses physico-chimiques des eaux souterraines
- **Méthode** : GET
- **Clés primaires** : `[code_bss, date_prelevement, code_parametre]`
- **Clé de réplication** : `date_prelevement`
- **Format** : JSON
- **Page Size** : 20,000 records
- **Pagination** : Page-based (`page` param)
- **Slicing** : Day (découpage quotidien)
- **Paramètres temporels** : `date_debut_prelevement`, `date_fin_prelevement`
- **Fallback** : Station Month

#### Attributs Clés
- `code_bss` : Identifiant de la station (BRGM)
- `date_prelevement` : Date de prélèvement
- `code_parametre` : Code du paramètre analysé
- `resultat` : Valeur de l'analyse
- `code_qualification` : Qualité de l'analyse
- `libelle_parametre` : Libellé du paramètre

### 5. Température

#### Informations Générales
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/temperature`
- **Version** : v1
- **Partitioning** : Yearly
- **Rate Limit** : 0.7 RPS (API très sensible)
- **Pagination** : Page-based

#### Endpoints Intégrés

##### `/station`
- **Description** : Référentiel des stations de température
- **Méthode** : GET
- **Clé primaire** : `code_station`
- **Format** : JSON
- **Page Size** : 1,000 records
- **Slicing** : Global

##### `/chronique`
- **Description** : Chroniques de température des cours d'eau
- **Méthode** : GET
- **Clés primaires** : `[code_station, date_mesure_temp]`
- **Clé de réplication** : `date_mesure_temp`
- **Format** : JSON
- **Page Size** : 20,000 records
- **Pagination** : Page-based (`page` param)
- **Slicing** : Dept DateTime (département × temps optimisé)
- **Paramètres temporels** : `date_debut_mesure`, `date_fin_mesure`
- **Paramètres spatiaux** : `code_departement`
- **Chunk Size** : 5 départements par requête
- **Fallback** : Station Month

#### Attributs Clés
- `code_station` : Identifiant de la station
- `date_mesure_temp` : Date de mesure de température
- `resultat` : Valeur de température
- `code_qualification` : Qualité de la mesure
- `libelle_station` : Libellé de la station

### 6. Écoulement (ONDE)

#### Informations Générales
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/ecoulement`
- **Version** : v1
- **Partitioning** : Daily
- **Rate Limit** : 1.5 RPS
- **Pagination** : Page-based
- **Description** : Observatoire National Des Étiages

#### Endpoints Intégrés

##### `/stations`
- **Description** : Référentiel des stations d'écoulement
- **Méthode** : GET
- **Clé primaire** : `code_station`
- **Format** : JSON
- **Page Size** : 1,000 records
- **Slicing** : Global

##### `/observations`
- **Description** : Observations d'écoulement des cours d'eau
- **Méthode** : GET
- **Clés primaires** : `[code_station, date_observation_min, date_observation_max]`
- **Clé de réplication** : `date_observation_min`
- **Format** : JSON
- **Page Size** : 20,000 records
- **Pagination** : Page-based (`page` param)
- **Slicing** : DateTime (fenêtre quotidienne)
- **Paramètres temporels** : `date_observation_min`, `date_observation_max`
- **Fallback** : Dept DateTime

#### Attributs Clés
- `code_station` : Identifiant de la station
- `date_observation_min` : Date de début d'observation
- `date_observation_max` : Date de fin d'observation
- `resultat_obs` : Valeur observée
- `code_qualification` : Qualité de la mesure

### 7. Hydrobiologie

#### Informations Générales
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/indicateurs_services`
- **Version** : v1
- **Partitioning** : Yearly
- **Rate Limit** : 1.7 RPS (indices), 0.6 RPS (taxons)
- **Pagination** : Page-based
- **Clé primaire** : `code_station_hydrobio`

#### Endpoints Intégrés

##### `/stations_hydrobio`
- **Description** : Référentiel des stations hydrobiologiques
- **Méthode** : GET
- **Clé primaire** : `code_station_hydrobio`
- **Format** : JSON
- **Page Size** : 1,000 records
- **Slicing** : Global

##### `/indices`
- **Description** : Indices biologiques (IBGN, IBD, etc.)
- **Méthode** : GET
- **Clés primaires** : `[code_station_hydrobio, date_campagne, code_indicateur]`
- **Clé de réplication** : `date_campagne`
- **Format** : JSON
- **Page Size** : 20,000 records
- **Pagination** : Page-based (`page` param)
- **Slicing** : Global (peu de données)
- **Fallback** : Station Month

##### `/taxons`
- **Description** : Taxons biologiques identifiés
- **Méthode** : GET
- **Clés primaires** : `[code_station_hydrobio, date_campagne, code_taxon]`
- **Clé de réplication** : `date_campagne`
- **Format** : JSON
- **Page Size** : 20,000 records
- **Pagination** : Page-based (`page` param)
- **Slicing** : Global (peu de données)
- **Fallback** : Station Month

#### Attributs Clés
- `code_station_hydrobio` : Identifiant de la station hydrobiologique
- `date_campagne` : Date de campagne d'échantillonnage
- `code_indicateur` : Code de l'indicateur biologique
- `code_taxon` : Code du taxon identifié
- `resultat` : Valeur de l'indice ou abondance du taxon
- `libelle_taxon` : Libellé du taxon

### 8. Prélèvements

#### Informations Générales
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/prelevements`
- **Version** : v1
- **Partitioning** : Yearly
- **Rate Limit** : 1.0 RPS
- **Pagination** : Page-based
- **Clé primaire** : `code_point_prelevement`

#### Endpoints Intégrés

##### `/points_prelevement`
- **Description** : Référentiel des points de prélèvement
- **Méthode** : GET
- **Clé primaire** : `code_point_prelevement`
- **Format** : JSON
- **Page Size** : 1,000 records
- **Slicing** : Global

##### `/chroniques`
- **Description** : Chroniques de prélèvements
- **Méthode** : GET
- **Clés primaires** : `[code_point_prelevement, date_prelevement]`
- **Clé de réplication** : `date_prelevement`
- **Format** : JSON
- **Page Size** : 20,000 records
- **Pagination** : Page-based (`page` param)
- **Slicing** : Station Month (station par station × mois)
- **Paramètres temporels** : `date_debut_prelevement`, `date_fin_prelevement`
- **Fallback** : Day (découpage quotidien)

#### Attributs Clés
- `code_point_prelevement` : Identifiant du point de prélèvement
- `date_prelevement` : Date de prélèvement
- `resultat` : Valeur du prélèvement
- `code_qualification` : Qualité du prélèvement
- `libelle_point_prelevement` : Libellé du point

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

### Temporalité d'Intégration

#### Données Temps Réel (Daily Partitions)
- **Hydrométrie** : Observations des 30 derniers jours
- **Écoulement** : Observations saisonnières (mai-octobre)

#### Données Historiques (Yearly Partitions)
- **Piézométrie** : Chroniques historiques complètes
- **Qualité Cours d'Eau** : Analyses par année
- **Qualité Eaux Souterraines** : Analyses par année
- **Température** : Chroniques annuelles complètes
- **Hydrobiologie** : Indices et taxons par campagne
- **Prélèvements** : Chroniques de prélèvements

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