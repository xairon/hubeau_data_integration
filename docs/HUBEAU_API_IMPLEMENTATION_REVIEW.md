# Revue d'Implémentation des APIs Hub'Eau

Ce document dresse une revue détaillée de l'implémentation actuelle des huit APIs Hub'Eau au sein du
pipeline. Il s'appuie sur le module `HubeauIngestionService` et sur les assets bronze déclarés dans
`src/hubeau_pipeline/assets/bronze`. L'objectif est de vérifier la conformité des intégrations par
rapport aux spécifications publiques Hub'Eau et de mettre en évidence les chantiers nécessaires pour
assurer l'ingestion quotidienne (une observation par jour maximum) et le stockage dans MinIO.

## Synthèse globale

| API | Asset(s) Dagster | Couverture actuelle | Limites principales |
| --- | ----------------- | ------------------- | ------------------- |
| Piézométrie | `hubeau_piezo_bronze_real` | Stations + chroniques temps réel | Déduplication partielle, fenêtre glissante 365 jours uniquement |
| Hydrométrie | `hubeau_hydro_bronze_real` | Référentiel stations + observations temps réel | Filtrage 7 jours, pas d'agrégation quotidienne, volume massif |
| Qualité eaux de surface | `hubeau_quality_surface_bronze_real` (nouvelle version) | Stations + analyses | Paramètres datés, désynchronisation v1/v2, déduplication absente |
| Qualité eaux souterraines | `hubeau_quality_groundwater_bronze_real` | Stations + analyses | Filtres temporels génériques, déduplication manquante |
| Température | `hubeau_temperature_bronze_real` | Station + chronique | Déduplication sur `chronique` OK mais fenêtre 365 jours |
| ONDE | `hubeau_onde_bronze_real` | Stations + campagnes + observations | Paramètres `date_*_campagne` non conformes, pas de déduplication |
| Hydrobiologie | `hubeau_hydrobiologie_bronze_real` | Stations + indices + opérations | Paramètres temporels approximatifs, pas de gestion de pagination spécifique |
| Prélèvements | `hubeau_prelevements_bronze_real` | Points + chroniques | Pas de filtrage spatial, risque de volumes importants |

## Évaluation détaillée par API

### 1. Piézométrie (`niveaux_nappes`)

* **Implementation** : `hubeau_piezo_bronze_real` utilise `HubeauIngestionService` avec les endpoints
  `stations` et `chroniques_tr` et impose une taille de page de 20 000 éléments.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L222-L250】
* **Points positifs** : la méthode `_deduplicate_observations` assure qu'une seule observation par
  jour est retenue pour l'endpoint `chroniques_tr`.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L147-L188】
* **Limites** :
  - La fenêtre temporelle est bloquée à 365 jours glissants par rapport à la partition Dagster, ce qui
    empêche une ingestion historique complète sans relancer les partitions passées.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L205-L240】
  - L'API officielle distingue `chroniques` (temps différé) et `chroniques_tr` (temps réel). Le code
    n'expose pas `chroniques`, ce qui prive des séries consolidées et des valeurs validées.
  - Aucun paramètre spatial ou par station n'est prévu, ce qui peut conduire à des extractions
    massives difficiles à stocker quotidiennement dans MinIO.

### 2. Hydrométrie (`hydrometrie` v2)

* **Implementation** : `hubeau_hydro_bronze_real` interroge `referentiel/stations` et
  `observations_tr` avec une fenêtre temporelle de 7 jours, taille de page 20 000.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L252-L286】
* **Points positifs** : la déduplication quotidienne s'applique à `observations_tr`.
* **Limites** :
  - L'API v2 propose également les endpoints `observations` (temps différé) et `stations`. Leur absence
    empêche d'accéder aux données consolidées et aux métadonnées complètes.
  - La fenêtre fixe de 7 jours ne garantit pas l'ingestion rétroactive : il faudrait rejouer chaque
    partition journalière passée pour reconstruire l'historique.
  - L'API recommande l'utilisation de filtres `code_entite` ou `code_departement`. Aucun filtrage
    spatial n'est prévu.

### 3. Qualité des eaux de surface (`qualite_rivieres` / `qualite_eau_surface`)

* **Implementation** : `hubeau_quality_surface_bronze_real` pointe vers
  `https://hubeau.eaufrance.fr/api/v1/qualite_eau_surface` avec les endpoints `stations` et
  `analyses`.【F:src/hubeau_pipeline/assets/bronze/hubeau_complete_apis.py†L24-L56】
* **Limites majeures** :
  - La documentation officielle a migré vers une version v2 (`/api/v2/qualite_rivieres`) avec des
    schémas différents et l'obligation d'indiquer au moins un filtre spatial ou un identifiant de
    station. L'implémentation actuelle n'intègre pas ces évolutions.
  - Aucun mécanisme de déduplication n'est appliqué aux analyses, ce qui contrevient à l'objectif « une
    observation par jour maximum ».
  - Les paramètres de date (`date_debut_prelevement`, `date_fin_prelevement`) doivent être fournis
    directement dans la requête : la méthode `ingest_hubeau_api` les ajoute mais ne tient pas compte des
    contraintes de l'API v2 (formats `datetime` ISO complets, limites de taille 500 au lieu de 5 000).

### 4. Qualité des eaux souterraines (`qualite_nappes`)

* **Implementation** : `hubeau_quality_groundwater_bronze_real` interroge `stations` et `analyses`.
* **Limites** :
  - Les paramètres temporels ajoutés dans `ingest_hubeau_api` sont globaux et ne respectent pas la
    distinction `date_debut_prelevement` / `date_fin_prelevement` propre à l'endpoint `analyses`. Les
    stations ne devraient pas recevoir ces filtres.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L313-L342】
  - Pas de déduplication sur les analyses, ni de filtrage sur la nature d'analyse (`code_parametre`).
  - L'API renvoie un volume important (> 1 Go) sans filtre : il faut prévoir des boucles par station ou
    par département.

### 5. Température (`temperature`)

* **Implementation** : `hubeau_temperature_bronze_real` consomme `station` et `chronique` avec une
  taille de page 10 000 et des paramètres de date `date_debut_mesure_temp` / `date_fin_mesure_temp`
  calculés automatiquement.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L344-L380】
* **Points positifs** : la déduplication quotidienne est active pour `chronique`.
* **Limites** :
  - La documentation impose un filtre sur `code_station` ou `code_departement`; l'implémentation
    interroge l'ensemble du territoire, ce qui excède les limites de débit Hub'Eau.
  - La fenêtre temporelle d'un an reste glissante et ne permet pas d'historiser sans rejouer les
    partitions.

### 6. ONDE (`ecoulement`)

* **Implementation** : `hubeau_onde_bronze_real` appelle `stations`, `campagnes` et `observations`.
* **Limites** :
  - L'API exige des paramètres `date_debut_campagne` et `date_fin_campagne` en datetime ou saison : le
    code les ajoute automatiquement mais sans tenir compte des spécificités de l'endpoint `observations`
    (qui attend `date_debut_obs` / `date_fin_obs`).【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L289-L341】
  - Pas de déduplication sur `observations`, alors que des observations multiples peuvent exister par
    jour et par station.
  - Les campagnes ONDE sont saisonnières : il est nécessaire de boucler sur les périodes actives et de
    gérer l'absence de données hors saison.

### 7. Hydrobiologie (`hydrobiologie`)

* **Implementation** : `hubeau_hydrobiologie_bronze_real` cible `stations`, `indices` et
  `operationPrelevement`.
* **Limites** :
  - La pagination Hub'Eau pour ces endpoints est limitée à 200 par page. Le service `paginate_api_call`
    force `size=20000`, ce qui n'est pas accepté par l'API et provoque un fallback implicite sans
    gestion de l'avertissement.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L120-L184】
  - L'API attend des paramètres tels que `code_commune`, `code_masse_eau` ou `periode`. Aucun n'est
    configuré.
  - Les observations biologiques nécessitent une agrégation par campagne; la logique « une observation
    par jour » n'est pas adaptée et n'est pas implémentée.

### 8. Prélèvements (`prelevements`)

* **Implementation** : `hubeau_prelevements_bronze_real` consomme `points_prelevement` et `chroniques`.
* **Limites** :
  - Les paramètres `date_debut` / `date_fin` sont injectés pour tous les endpoints, y compris les
    référentiels, ce qui n'est pas supporté. Les points de prélèvement doivent être récupérés sans
    filtre temporel.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L305-L334】
  - Aucun filtrage par usage, masse d'eau ou SIRET n'est prévu. Les volumes peuvent devenir ingérables
    et dépasser les quotas Hub'Eau.
  - Les chroniques de prélèvements sont annuelles ou mensuelles : une déduplication quotidienne n'a pas
    de sens. Le besoin réel est d'agréger par période de déclaration.

## Constats transverses

1. **Gestion de la pagination** : la taille de page est injectée de manière uniforme (`size=20000`)
   alors que plusieurs APIs imposent des plafonds inférieurs (hydrobiologie, qualité). Il faut lire la
   valeur maximale par API dans la documentation puis l'appliquer dynamiquement.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L120-L184】
2. **Filtres temporels** : les paramètres sont appliqués globalement à tous les endpoints; les
   référentiels (`stations`, `points_prelevement`, `referentiel/*`) ne doivent pas recevoir de filtres
   temporels ou de pagination agressive. Une configuration par endpoint est nécessaire.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L305-L342】
3. **Déduplication** : seule trois endpoints bénéficient d'une déduplication quotidienne.
   Il faut étendre cette logique ou définir des stratégies adaptées (par prélèvement, par campagne).
4. **Stockage MinIO** : le service stocke un fichier JSON par endpoint et par jour. Pour des APIs comme
   Hydrométrie ou Qualité, cela risque de dépasser 5 Go/jour. Il faudrait compresser (gzip/parquet) ou
   partitionner davantage dans MinIO.【F:src/hubeau_pipeline/assets/bronze/hubeau_real_ingestion.py†L189-L204】
5. **Rejeu historique** : la stratégie actuelle ne permet pas de reconstruire l'historique complet sans
   replanifier toutes les partitions depuis la date de début. Une commande de backfill Dagster et un
   paramétrage de période initiale par API sont requis.
6. **Référentiels complémentaires** : les API Hub'Eau fournissent des endpoints de référentiels
   (paramètres, métadonnées). Ils ne sont pas couverts et doivent être ajoutés pour enrichir les couches
   Silver/Gold.

## Recommandations prioritaires

1. **Revoir la configuration `HubeauAPIConfig`** pour permettre des paramètres spécifiques par
   endpoint (filtres temporels, spatial, taille de page).
2. **Mettre en place des stratégies d'échantillonnage** adaptées à chaque API (déduplication par station
   et jour pour les séries continues, agrégation par campagne pour ONDE et Hydrobiologie, agrégation par
   période déclarative pour Prélèvements).
3. **Ajouter des filtres spatiaux** (département, bassin, station) pour rester sous les limites Hub'Eau
   et faciliter le rechargement historique.
4. **Gérer les limites de taille et de quota** (compression, chunking, monitoring des erreurs 429) pour
   garantir la robustesse des assets Dagster.
5. **Documenter un plan de backfill** et d'initialisation de MinIO afin de garantir que les données
   historiques seront intégrées avant la mise en production.

## Étapes suivantes proposées

1. Cartographier précisément les endpoints et paramètres attendus pour chaque API en s'appuyant sur les
   spécifications Hub'Eau (OpenAPI/Swagger).
2. Refactorer `HubeauIngestionService` pour accepter une configuration par endpoint (paramètres, champs
   de date/station, stratégie de déduplication, taille de page).
3. Mettre en place des tests d'intégration simulant les réponses Hub'Eau (fixtures JSON) pour vérifier la
   bonne déduplication et le stockage MinIO.
4. Ajouter des assets Dagster dédiés aux référentiels et à l'orchestration des backfills.

Cette revue doit servir de base pour planifier le refactoring et assurer une implémentation fidèle aux
huit APIs Hub'Eau.
