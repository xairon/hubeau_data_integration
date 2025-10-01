# Sources de Données Hub'Eau

## APIs Intégrées

### 1. Hydrométrie
**Base URL :** `https://hubeau.eaufrance.fr/api/v1/hydrometrie`
**Volume :** ~50M records
**Partitioning :** Daily

#### Endpoints
- **`referentiel_stations`** : Stations hydrométriques (~15K stations)
- **`observations_tr`** : Observations temps réel (~30M records)
- **`obs_elab`** : Observations élaborées (~20M records)

#### Paramètres Temporels
```json
{
  "date_debut_obs": "YYYY-MM-DD",
  "date_fin_obs": "YYYY-MM-DD"
}
```

#### Champs Clés
- `code_entite` : Identifiant station
- `date_obs` : Date observation
- `resultat` : Valeur mesurée
- `code_qualification` : Qualité mesure

### 2. Piézométrie
**Base URL :** `https://hubeau.eaufrance.fr/api/v1/niveaux_nappes`
**Volume :** ~30M records
**Partitioning :** Daily

#### Endpoints
- **`stations`** : Stations piézométriques (~5K stations)
- **`chroniques_tr`** : Chroniques temps réel (~20M records)
- **`chroniques`** : Chroniques historiques (~10M records)

#### Champs Clés
- `code_bss` : Identifiant station
- `date_mesure` : Date mesure
- `niveau_nappe_eau` : Niveau piézométrique

### 3. Qualité Cours d'Eau
**Base URL :** `https://hubeau.eaufrance.fr/api/v1/qualite_rivieres`
**Volume :** ~5M records
**Partitioning :** Annual

#### Endpoints
- **`station_pc`** : Stations de prélèvement (~3K stations)
- **`analyse_pc`** : Analyses physico-chimiques (~5M analyses)

#### Champs Clés
- `code_station` : Identifiant station
- `date_prelevement` : Date prélèvement
- `code_parametre` : Code paramètre analysé
- `resultat` : Valeur analyse

### 4. Qualité Eaux Souterraines
**Base URL :** `https://hubeau.eaufrance.fr/api/v1/qualite_nappes`
**Volume :** ~2M records
**Partitioning :** Annual

#### Endpoints
- **`stations`** : Stations de surveillance (~2K stations)
- **`analyses`** : Analyses physico-chimiques (~2M analyses)

#### Champs Clés
- `code_bss` : Identifiant station
- `date_prelevement` : Date prélèvement
- `code_parametre` : Code paramètre
- `resultat` : Valeur analyse

### 5. Température
**Base URL :** `https://hubeau.eaufrance.fr/api/v1/temperature`
**Volume :** ~10M records
**Partitioning :** Daily

#### Endpoints
- **`station`** : Stations thermiques (~1K stations)
- **`chronique`** : Chroniques température (~10M mesures)

#### Champs Clés
- `code_station` : Identifiant station
- `date_mesure` : Date mesure
- `temperature_eau` : Température eau

### 6. ONDE
**Base URL :** `https://hubeau.eaufrance.fr/api/v1/onde`
**Volume :** ~500K records
**Partitioning :** Annual

#### Endpoints
- **`stations`** : Stations ONDE (~3K stations)
- **`observations`** : Observations d'étiage (~500K observations)

#### Champs Clés
- `code_station` : Identifiant station
- `date_obs` : Date observation
- `libelle_categorie_ecoulement` : État écoulement

### 7. Hydrobiologie
**Base URL :** `https://hubeau.eaufrance.fr/api/v1/hydrobio`
**Volume :** ~1M records
**Partitioning :** Annual

#### Endpoints
- **`stations_hydrobio`** : Stations hydrobiologiques (~2K stations)
- **`indices`** : Indices biologiques (~500K indices)
- **`taxons`** : Taxons observés (~500K observations)

#### Champs Clés
- `code_station_hydrobio` : Identifiant station
- `date_campagne` : Date campagne
- `code_indice` : Code indice biologique
- `resultat` : Valeur indice

### 8. Prélèvements
**Base URL :** `https://hubeau.eaufrance.fr/api/v1/prelevements`
**Volume :** ~20M records
**Partitioning :** Annual

#### Endpoints
- **`points_prelevement`** : Points de prélèvement (~50K points)
- **`chroniques`** : Chroniques de prélèvement (~20M mesures)

#### Champs Clés
- `code_ouvrage` : Identifiant ouvrage
- `annee` : Année prélèvement
- `volume_preleve` : Volume prélevé

## Contraintes Techniques

### Rate Limiting
- **Délai :** 0.5s entre requêtes
- **Concurrence :** Max 10 requêtes simultanées (toutes APIs)
- **Timeout :** 60s par requête
- **Retry :** 3 tentatives avec backoff exponentiel

### Pagination
- **Taille page :** 1000 records
- **Support curseur :** APIs récentes (hydrométrie, piézométrie)
- **Pagination classique :** APIs legacy (qualité, hydrobiologie)

### Limites de Volume
- **Hydrobiologie :** 10K records max par requête
- **Qualité eaux :** 10K records max par requête
- **Autres APIs :** Pas de limite explicite

## Stratégies d'Ingestion

### Partitioning Temporel
- **Daily :** APIs temps réel (hydrométrie, piézométrie, température)
- **Annual :** APIs de campagnes (qualité, hydrobiologie, ONDE)

### Chunking Spatial
- **1 département :** APIs sensibles (hydrobiologie, prélèvements)
- **5 départements :** APIs standard (hydrométrie, piézométrie)
- **Parallélisation :** 4-15 requêtes simultanées selon API
