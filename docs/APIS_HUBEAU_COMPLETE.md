# 📊 Documentation Complète des APIs Hub'Eau Intégrées

## 📋 Table des Matières

1. [Hydrométrie](#hydrométrie)
2. [Piézométrie](#piézométrie)
3. [Qualité des Cours d'Eau](#qualité-des-cours-deau)
4. [Qualité des Nappes](#qualité-des-nappes)
5. [Hydrobiologie](#hydrobiologie)
6. [Écoulement des Cours d'Eau](#écoulement-des-cours-deau)
7. [Prélèvements en Eau](#prélèvements-en-eau)
8. [Température des Cours d'Eau](#température-des-cours-deau)

---

## 🌊 Hydrométrie

### 📝 Description
API pour les niveaux et débits des cours d'eau en temps réel et historiques.

### 🔗 Informations Générales
- **Nom officiel** : Hydrométrie
- **Base URL** : `https://hubeau.eaufrance.fr/api/v2/hydrometrie`
- **Version** : v2
- **Documentation** : https://hubeau.eaufrance.fr/page/api-hydrometrie
- **Type de données** : Temps réel + Historiques
- **Fréquence de mise à jour** : Temps réel (données mises à jour toutes les heures)

### ⚡ Rate Limiting & Performance
- **Max retries** : 5
- **Rate limit delay** : 0.5 secondes
- **Limite de profondeur** : Aucune limite spécifique
- **Page size** : 1000-5000 records par page
- **Concurrence** : 10 requêtes simultanées max

### 📅 Temporalité
- **Partitions** : Non partitionné (30 derniers jours automatique)
- **Données disponibles** : Depuis 2000 jusqu'à maintenant
- **Fréquence** : Données horaires, quotidiennes, mensuelles selon les stations
- **Période de récupération** : 30 derniers jours par défaut

### 🎯 Endpoints Intégrés

#### 1. `referentiel_sites`
- **Path** : `/referentiel/sites`
- **Description** : Sites hydrométriques (lieux de mesure)
- **Page size** : 5000
- **Max pages** : 10
- **Cache** : 30 jours
- **Filtrage spatial** : Par département
- **Attributs principaux** :
  - `code_site` : Code unique du site
  - `libelle_site` : Nom du site
  - `coordonnee_x`, `coordonnee_y` : Coordonnées géographiques
  - `code_departement` : Code département
  - `code_commune` : Code commune
  - `type_site` : Type de site hydrométrique

#### 2. `referentiel_stations`
- **Path** : `/referentiel/stations`
- **Description** : Stations hydrométriques (points de mesure)
- **Page size** : 5000
- **Max pages** : 10
- **Cache** : 30 jours
- **Filtrage spatial** : Par département
- **Attributs principaux** :
  - `code_station` : Code unique de la station
  - `libelle_station` : Nom de la station
  - `code_site` : Code du site parent
  - `coordonnee_x`, `coordonnee_y` : Coordonnées géographiques
  - `en_service` : Station en service (true/false)
  - `date_mise_en_service` : Date de mise en service

#### 3. `observations_tr`
- **Path** : `/observations_tr`
- **Description** : Observations hydrométriques temps réel
- **Page size** : 1000
- **Max pages** : 50
- **Cache** : 1 heure
- **Filtrage spatial** : Par département
- **Filtrage temporel** : 30 derniers jours
- **Attributs principaux** :
  - `code_station` : Code de la station
  - `date_obs` : Date et heure d'observation
  - `niveau_eau` : Niveau d'eau (cm)
  - `debit` : Débit (m³/s)
  - `qualification` : Qualification de la mesure
  - `type_grandeur` : Type de grandeur mesurée

#### 4. `obs_elab`
- **Path** : `/obs_elab`
- **Description** : Observations hydrométriques élaborées (calculées)
- **Page size** : 1000
- **Max pages** : 50
- **Cache** : 1 heure
- **Filtrage spatial** : Par département
- **Filtrage temporel** : 30 derniers jours
- **Attributs principaux** :
  - `code_station` : Code de la station
  - `date_obs` : Date et heure d'observation
  - `valeur` : Valeur calculée
  - `type_elab` : Type d'élaboration
  - `qualification` : Qualification de la mesure

### 🔧 Configuration Technique
```python
HubeauApiConfig(
    name="hydrometry",
    base_url="https://hubeau.eaufrance.fr/api/v2/hydrometrie",
    version="v2",
    max_retries=5,
    rate_limit_delay=0.5,
    endpoints={
        "referentiel_sites": HubeauEndpointConfig(
            path="referentiel/sites",
            page_size=5000,
            max_pages=10,
            cache_duration=30,
            requires_spatial_filter=True,
            spatial_params={"dept": "code_departement"},
            depth_limit=50000
        ),
        # ... autres endpoints
    }
)
```

### 📊 Volume de Données
- **Stations actives** : ~3000 stations
- **Sites** : ~2000 sites
- **Observations/jour** : ~72,000 observations temps réel
- **Observations élaborées/jour** : ~24,000 observations

---

## 🕳️ Piézométrie

### 📝 Description
API pour les niveaux des nappes phréatiques (piézométrie).

### 🔗 Informations Générales
- **Nom officiel** : Piézométrie
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/piezometrie`
- **Version** : v1
- **Documentation** : https://hubeau.eaufrance.fr/page/api-piezometrie
- **Type de données** : Historiques
- **Fréquence de mise à jour** : Mensuelle

### ⚡ Rate Limiting & Performance
- **Max retries** : 5
- **Rate limit delay** : 0.7 secondes
- **Limite de profondeur** : Aucune limite spécifique
- **Page size** : 1000-2000 records par page
- **Concurrence** : 10 requêtes simultanées max

### 📅 Temporalité
- **Partitions** : Annuelles (YEARLY_PARTITIONS)
- **Données disponibles** : Depuis 1950 jusqu'à maintenant
- **Fréquence** : Données mensuelles, trimestrielles selon les stations
- **Période de récupération** : Année complète par partition

### 🎯 Endpoints Intégrés

#### 1. `stations`
- **Path** : `/stations`
- **Description** : Stations piézométriques
- **Page size** : 2000
- **Max pages** : 20
- **Cache** : 30 jours
- **Filtrage spatial** : Par département
- **Attributs principaux** :
  - `code_bss` : Code BSS (Banque du Sous-Sol)
  - `nom_station` : Nom de la station
  - `coordonnee_x`, `coordonnee_y` : Coordonnées géographiques
  - `code_departement` : Code département
  - `profondeur_nappe` : Profondeur de la nappe (m)
  - `en_service` : Station en service

#### 2. `chroniques_tr`
- **Path** : `/chroniques_tr`
- **Description** : Chroniques piézométriques temps réel
- **Page size** : 1000
- **Max pages** : 50
- **Cache** : 1 heure
- **Filtrage spatial** : Par département
- **Filtrage temporel** : Année complète
- **Attributs principaux** :
  - `code_bss` : Code BSS de la station
  - `date_mesure` : Date de mesure
  - `niveau_nappe_eau` : Niveau de la nappe (m NGF)
  - `qualification` : Qualification de la mesure
  - `type_mesure` : Type de mesure

#### 3. `chroniques`
- **Path** : `/chroniques`
- **Description** : Chroniques piézométriques historiques
- **Page size** : 1000
- **Max pages** : 50
- **Cache** : 1 heure
- **Filtrage spatial** : Par département
- **Filtrage temporel** : Année complète
- **Attributs principaux** :
  - `code_bss` : Code BSS de la station
  - `date_mesure` : Date de mesure
  - `niveau_nappe_eau` : Niveau de la nappe (m NGF)
  - `qualification` : Qualification de la mesure
  - `type_mesure` : Type de mesure

### 📊 Volume de Données
- **Stations** : ~15,000 stations
- **Observations/mois** : ~45,000 observations
- **Période couverte** : 70+ années de données

---

## 🏞️ Qualité des Cours d'Eau

### 📝 Description
API pour les analyses de qualité des cours d'eau superficiels.

### 🔗 Informations Générales
- **Nom officiel** : Qualité des Cours d'Eau
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_rivieres`
- **Version** : v1
- **Documentation** : https://hubeau.eaufrance.fr/page/api-qualite-rivieres
- **Type de données** : Historiques
- **Fréquence de mise à jour** : Trimestrielle

### ⚡ Rate Limiting & Performance
- **Max retries** : 5
- **Rate limit delay** : 0.7 secondes
- **Limite de profondeur** : Aucune limite spécifique
- **Page size** : 1000 records par page
- **Concurrence** : 10 requêtes simultanées max
- **Chunking** : 1 département à la fois (API sensible)

### 📅 Temporalité
- **Partitions** : Annuelles (YEARLY_PARTITIONS)
- **Données disponibles** : Depuis 2000 jusqu'à maintenant
- **Fréquence** : Données trimestrielles, semestrielles selon les stations
- **Période de récupération** : Année complète par partition

### 🎯 Endpoints Intégrés

#### 1. `stations`
- **Path** : `/stations`
- **Description** : Stations de mesure de qualité
- **Page size** : 1000
- **Max pages** : 20
- **Cache** : 30 jours
- **Filtrage spatial** : Par département
- **Attributs principaux** :
  - `code_station` : Code unique de la station
  - `libelle_station` : Nom de la station
  - `coordonnee_x`, `coordonnee_y` : Coordonnées géographiques
  - `code_departement` : Code département
  - `en_service` : Station en service

#### 2. `analyse_pc`
- **Path** : `/analyse_pc`
- **Description** : Analyses physico-chimiques des cours d'eau
- **Page size** : 1000
- **Max pages** : 50
- **Cache** : 1 heure
- **Filtrage spatial** : Par département
- **Filtrage temporel** : Année complète
- **Attributs principaux** :
  - `code_station` : Code de la station
  - `date_prelevement` : Date de prélèvement
  - `libelle_parametre` : Nom du paramètre analysé
  - `resultat` : Résultat de l'analyse
  - `unite` : Unité de mesure
  - `qualification` : Qualification de l'analyse
  - `type_parametre` : Type de paramètre (physique, chimique, biologique)

### 📊 Volume de Données
- **Stations** : ~3,000 stations
- **Analyses/an** : ~500,000 analyses
- **Paramètres** : 200+ paramètres différents

---

## 💧 Qualité des Nappes

### 📝 Description
API pour les analyses de qualité des eaux souterraines (nappes phréatiques).

### 🔗 Informations Générales
- **Nom officiel** : Qualité des Nappes
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_nappes_eau`
- **Version** : v1
- **Documentation** : https://hubeau.eaufrance.fr/page/api-qualite-nappes-eau
- **Type de données** : Historiques
- **Fréquence de mise à jour** : Trimestrielle

### ⚡ Rate Limiting & Performance
- **Max retries** : 5
- **Rate limit delay** : 0.7 secondes
- **Limite de profondeur** : Aucune limite spécifique
- **Page size** : 1000 records par page
- **Concurrence** : 10 requêtes simultanées max
- **Chunking** : 1 département à la fois (API sensible)

### 📅 Temporalité
- **Partitions** : Annuelles (YEARLY_PARTITIONS)
- **Données disponibles** : Depuis 2000 jusqu'à maintenant
- **Fréquence** : Données trimestrielles, semestrielles selon les stations
- **Période de récupération** : Année complète par partition

### 🎯 Endpoints Intégrés

#### 1. `stations`
- **Path** : `/stations`
- **Description** : Stations de mesure de qualité des nappes
- **Page size** : 1000
- **Max pages** : 20
- **Cache** : 30 jours
- **Filtrage spatial** : Par département
- **Attributs principaux** :
  - `code_bss` : Code BSS de la station
  - `nom_station` : Nom de la station
  - `coordonnee_x`, `coordonnee_y` : Coordonnées géographiques
  - `code_departement` : Code département
  - `profondeur_nappe` : Profondeur de la nappe (m)

#### 2. `analyses`
- **Path** : `/analyses`
- **Description** : Analyses physico-chimiques des nappes
- **Page size** : 1000
- **Max pages** : 50
- **Cache** : 1 heure
- **Filtrage spatial** : Par département
- **Filtrage temporel** : Année complète
- **Attributs principaux** :
  - `code_bss` : Code BSS de la station
  - `date_prelevement` : Date de prélèvement
  - `libelle_parametre` : Nom du paramètre analysé
  - `resultat` : Résultat de l'analyse
  - `unite` : Unité de mesure
  - `qualification` : Qualification de l'analyse
  - `type_parametre` : Type de paramètre

### 📊 Volume de Données
- **Stations** : ~8,000 stations
- **Analyses/an** : ~200,000 analyses
- **Paramètres** : 150+ paramètres différents

---

## 🐟 Hydrobiologie

### 📝 Description
API pour les données hydrobiologiques (indices biologiques et taxons).

### 🔗 Informations Générales
- **Nom officiel** : Hydrobiologie
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/hydrobio`
- **Version** : v1
- **Documentation** : https://hubeau.eaufrance.fr/page/api-hydrobiologie
- **Type de données** : Historiques
- **Fréquence de mise à jour** : Annuelle

### ⚡ Rate Limiting & Performance
- **Max retries** : 5
- **Rate limit delay** : 0.7 secondes
- **Limite de profondeur** : 10,000 records par requête
- **Page size** : 1000 records par page
- **Concurrence** : 10 requêtes simultanées max
- **Chunking** : 1 département à la fois (limite stricte)

### 📅 Temporalité
- **Partitions** : Annuelles (YEARLY_PARTITIONS)
- **Données disponibles** : Depuis 2000 jusqu'à maintenant
- **Fréquence** : Données annuelles, semestrielles selon les stations
- **Période de récupération** : Année complète par partition

### 🎯 Endpoints Intégrés

#### 1. `stations`
- **Path** : `/stations`
- **Description** : Stations hydrobiologiques
- **Page size** : 1000
- **Max pages** : 20
- **Cache** : 30 jours
- **Filtrage spatial** : Par département
- **Attributs principaux** :
  - `code_station` : Code unique de la station
  - `libelle_station` : Nom de la station
  - `coordonnee_x`, `coordonnee_y` : Coordonnées géographiques
  - `code_departement` : Code département
  - `en_service` : Station en service

#### 2. `indices`
- **Path** : `/indices`
- **Description** : Indices biologiques (IBGN, I2M2, etc.)
- **Page size** : 1000
- **Max pages** : 20
- **Cache** : 1 heure
- **Filtrage spatial** : Par département
- **Filtrage temporel** : Année complète
- **Attributs principaux** :
  - `code_station` : Code de la station
  - `date_campagne` : Date de campagne
  - `libelle_indice` : Nom de l'indice
  - `valeur_indice` : Valeur de l'indice
  - `qualification` : Qualification de la mesure
  - `type_indice` : Type d'indice biologique

#### 3. `taxons`
- **Path** : `/taxons`
- **Description** : Taxons biologiques identifiés
- **Page size** : 1000
- **Max pages** : 20
- **Cache** : 1 heure
- **Filtrage spatial** : Par département
- **Filtrage temporel** : Année complète
- **Attributs principaux** :
  - `code_station` : Code de la station
  - `date_campagne` : Date de campagne
  - `libelle_taxon` : Nom du taxon
  - `abondance` : Abondance du taxon
  - `qualification` : Qualification de l'identification
  - `type_taxon` : Type de taxon (macroinvertébrés, diatomées, etc.)

### 📊 Volume de Données
- **Stations** : ~2,000 stations
- **Indices/an** : ~5,000 indices
- **Taxons/an** : ~50,000 identifications

---

## 🌊 Écoulement des Cours d'Eau

### 📝 Description
API pour l'écoulement des cours d'eau (anciennement ONDE - Observatoire National Des Étiages).

### 🔗 Informations Générales
- **Nom officiel** : Écoulement des Cours d'Eau
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/ecoulement`
- **Version** : v1
- **Documentation** : https://hubeau.eaufrance.fr/page/api-ecoulement-cours-eau
- **Type de données** : Historiques
- **Fréquence de mise à jour** : Saisonnière (campagnes)

### ⚡ Rate Limiting & Performance
- **Max retries** : 5
- **Rate limit delay** : 0.7 secondes
- **Limite de profondeur** : Aucune limite spécifique
- **Page size** : 1000-5000 records par page
- **Concurrence** : 10 requêtes simultanées max

### 📅 Temporalité
- **Partitions** : Annuelles (YEARLY_PARTITIONS)
- **Données disponibles** : Depuis 2012 jusqu'à maintenant
- **Fréquence** : Données saisonnières (campagnes mai-octobre)
- **Période de récupération** : Année complète par partition
- **Spécificité** : Utilise les campagnes pour déterminer les bonnes dates d'observation

### 🎯 Endpoints Intégrés

#### 1. `stations`
- **Path** : `/stations`
- **Description** : Stations d'observation de l'écoulement
- **Page size** : 5000
- **Max pages** : 10
- **Cache** : 30 jours
- **Attributs principaux** :
  - `code_station` : Code unique de la station
  - `libelle_station` : Nom de la station
  - `coordonnee_x`, `coordonnee_y` : Coordonnées géographiques
  - `code_departement` : Code département
  - `en_service` : Station en service
  - `type_station` : Type de station

#### 2. `campagnes`
- **Path** : `/campagnes`
- **Description** : Campagnes d'observation (périodes d'étiage)
- **Page size** : 1000
- **Max pages** : Aucune limite
- **Cache** : 30 jours
- **Filtrage temporel** : Année complète
- **Attributs principaux** :
  - `code_station` : Code de la station
  - `date_campagne` : Date de la campagne
  - `type_campagne` : Type de campagne
  - `etat_ecoulement` : État de l'écoulement observé
  - `qualification` : Qualification de l'observation

#### 3. `observations`
- **Path** : `/observations`
- **Description** : Observations d'écoulement
- **Page size** : 1000
- **Max pages** : Aucune limite
- **Cache** : 30 jours
- **Filtrage temporel** : `date_observation_min` / `date_observation_max`
- **Attributs principaux** :
  - `code_station` : Code de la station
  - `date_observation` : Date d'observation
  - `etat_ecoulement` : État de l'écoulement
  - `qualification` : Qualification de l'observation
  - `type_observation` : Type d'observation

### 🔧 Logique Spéciale
Cette API utilise une logique particulière :
1. **Récupération des campagnes** : Pour déterminer les bonnes dates d'observation
2. **Filtrage des observations** : Seules les observations correspondant aux dates de campagnes sont conservées
3. **Période saisonnière** : Les campagnes se déroulent généralement de mai à octobre

### 📊 Volume de Données
- **Stations** : ~3,500 stations
- **Campagnes/an** : ~9,500 campagnes
- **Observations/an** : ~15,000 observations

---

## 💧 Prélèvements en Eau

### 📝 Description
API pour les prélèvements d'eau (captages, pompages, etc.).

### 🔗 Informations Générales
- **Nom officiel** : Prélèvements en Eau
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/prelevements`
- **Version** : v1
- **Documentation** : https://hubeau.eaufrance.fr/page/api-prelevements-eau
- **Type de données** : Historiques
- **Fréquence de mise à jour** : Mensuelle

### ⚡ Rate Limiting & Performance
- **Max retries** : 8 (API sensible aux erreurs 500)
- **Rate limit delay** : 1.0 seconde (rate limit respectueux)
- **Limite de profondeur** : 20,000 records par requête
- **Page size** : 1000-2000 records par page
- **Concurrence** : 10 requêtes simultanées max
- **Chunking** : 1 département à la fois (limite stricte 20k)

### 📅 Temporalité
- **Partitions** : Annuelles (YEARLY_PARTITIONS)
- **Données disponibles** : Depuis 2000 jusqu'à maintenant
- **Fréquence** : Données mensuelles, trimestrielles selon les ouvrages
- **Période de récupération** : Année complète par partition

### 🎯 Endpoints Intégrés

#### 1. `points_prelevement`
- **Path** : `/referentiel/points_prelevement`
- **Description** : Points de prélèvement (lieux de captage)
- **Page size** : 2000
- **Max pages** : Aucune limite
- **Cache** : 30 jours
- **Filtrage spatial** : Par département
- **Attributs principaux** :
  - `code_point_prelevement` : Code unique du point
  - `libelle_point_prelevement` : Nom du point
  - `coordonnee_x`, `coordonnee_y` : Coordonnées géographiques
  - `code_departement` : Code département
  - `type_point_prelevement` : Type de point
  - `en_service` : Point en service

#### 2. `ouvrages`
- **Path** : `/referentiel/ouvrages`
- **Description** : Ouvrages de prélèvement (pompes, captages)
- **Page size** : 2000
- **Max pages** : Aucune limite
- **Cache** : 30 jours
- **Filtrage spatial** : Par département
- **Attributs principaux** :
  - `code_ouvrage` : Code unique de l'ouvrage
  - `libelle_ouvrage` : Nom de l'ouvrage
  - `code_point_prelevement` : Code du point parent
  - `type_ouvrage` : Type d'ouvrage
  - `capacite_nominale` : Capacité nominale (m³/h)
  - `en_service` : Ouvrage en service

#### 3. `chroniques`
- **Path** : `/chroniques`
- **Description** : Chroniques de prélèvement (volumes prélevés)
- **Page size** : 1000
- **Max pages** : Aucune limite
- **Cache** : 30 jours
- **Filtrage spatial** : Par département
- **Filtrage temporel** : `annee_min` / `annee_max`
- **Attributs principaux** :
  - `code_ouvrage` : Code de l'ouvrage
  - `date_prelevement` : Date de prélèvement
  - `volume_preleve` : Volume prélevé (m³)
  - `unite_volume` : Unité de volume
  - `qualification` : Qualification de la mesure
  - `type_prelevement` : Type de prélèvement

### ⚠️ Limitations Spéciales
- **Limite stricte** : 20,000 records par requête (erreurs 400 si dépassé)
- **Chunking obligatoire** : 1 département à la fois
- **Erreurs 500 fréquentes** : Nécessite plus de retries et rate limiting

### 📊 Volume de Données
- **Points de prélèvement** : ~50,000 points
- **Ouvrages** : ~80,000 ouvrages
- **Chroniques/an** : ~500,000 enregistrements

---

## 🌡️ Température des Cours d'Eau

### 📝 Description
API pour la température des cours d'eau (données historiques).

### 🔗 Informations Générales
- **Nom officiel** : Température des Cours d'Eau
- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/temperature`
- **Version** : v1
- **Documentation** : https://hubeau.eaufrance.fr/page/api-temperature-continu
- **Type de données** : Historiques
- **Fréquence de mise à jour** : Trimestrielle (depuis Naïades)

### ⚡ Rate Limiting & Performance
- **Max retries** : 8 (API sensible aux erreurs 500)
- **Rate limit delay** : 1.5 secondes (rate limit très respectueux)
- **Limite de profondeur** : 20,000 records par requête
- **Page size** : 1000 records par page
- **Concurrence** : 10 requêtes simultanées max
- **Chunking** : 101 départements d'un coup (pas de chunking départemental)
- **Stratégie spéciale** : Station par station avec découpage mensuel

### 📅 Temporalité
- **Partitions** : Annuelles (YEARLY_PARTITIONS)
- **Données disponibles** : Depuis 2000 jusqu'à maintenant
- **Fréquence** : Données quotidiennes, hebdomadaires selon les stations
- **Période de récupération** : Année complète par partition
- **Spécificité** : Découpage mensuel pour éviter la limite 20k

### 🎯 Endpoints Intégrés

#### 1. `station`
- **Path** : `/station`
- **Description** : Stations de mesure de température
- **Page size** : 5000
- **Max pages** : Aucune limite (seulement ~760 stations)
- **Cache** : 30 jours
- **Filtrage spatial** : Aucun (récupère toutes les stations)
- **Attributs principaux** :
  - `code_station` : Code unique de la station
  - `libelle_station` : Nom de la station
  - `coordonnee_x`, `coordonnee_y` : Coordonnées géographiques
  - `code_departement` : Code département
  - `en_service` : Station en service
  - `type_station` : Type de station

#### 2. `chronique`
- **Path** : `/chronique`
- **Description** : Chroniques de température
- **Page size** : 1000
- **Max pages** : Aucune limite (fallback mensuel gère la troncature)
- **Cache** : 30 jours
- **Filtrage spatial** : Aucun (trop de données par département)
- **Filtrage temporel** : `date_debut_mesure` / `date_fin_mesure`
- **Attributs principaux** :
  - `code_station` : Code de la station
  - `date_mesure` : Date de mesure
  - `temperature_eau` : Température de l'eau (°C)
  - `qualification` : Qualification de la mesure
  - `type_mesure` : Type de mesure

### 🔧 Logique Spéciale
Cette API utilise une stratégie très particulière :
1. **Récupération de toutes les stations** : Pas de filtrage spatial
2. **Station par station** : Chaque station traitée individuellement
3. **Découpage mensuel** : 12 mois par station pour éviter la limite 20k
4. **Fallback automatique** : Si troncature détectée → découpage mensuel

### ⚠️ Limitations Spéciales
- **Limite stricte** : 20,000 records par requête
- **Données très denses** : Certains départements dépassent facilement 20k
- **Erreurs 500 fréquentes** : Nécessite plus de retries et rate limiting
- **Stratégie complexe** : Fallback station par station + mensuel

### 📊 Volume de Données
- **Stations** : ~760 stations (dont ~50 encore en service)
- **Observations/an** : ~200,000 observations
- **Période couverte** : 20+ années de données

---

## 📊 Résumé des APIs

| API | Endpoints | Partitions | Chunking | Limite | Volume/an |
|-----|-----------|------------|----------|--------|-----------|
| **Hydrométrie** | 4 | Non | 5 dept | Aucune | ~26M obs |
| **Piézométrie** | 3 | Annuelles | 5 dept | Aucune | ~540k obs |
| **Qualité Cours d'Eau** | 2 | Annuelles | 1 dept | Aucune | ~500k analyses |
| **Qualité Nappes** | 2 | Annuelles | 1 dept | Aucune | ~200k analyses |
| **Hydrobiologie** | 3 | Annuelles | 1 dept | 10k | ~55k données |
| **Écoulement** | 3 | Annuelles | 5 dept | Aucune | ~15k obs |
| **Prélèvements** | 3 | Annuelles | 1 dept | 20k | ~500k chroniques |
| **Température** | 2 | Annuelles | 101 dept | 20k | ~200k obs |

## 🔧 Configuration Globale

### Paramètres Communs
- **Concurrence max** : 10 requêtes simultanées
- **Timeout** : 30 secondes par requête
- **Cache** : 30 jours pour référentiels, 1 heure pour observations
- **Retry** : 5-8 tentatives selon l'API
- **Rate limiting** : 0.5-1.5 secondes selon l'API

### Stratégies de Chunking
- **Chunking départemental** : 1-5 départements selon l'API
- **Chunking temporel** : Mensuel pour température
- **Chunking par station** : Station par station pour température

### Gestion des Erreurs
- **Erreurs 400** : Limite de profondeur atteinte
- **Erreurs 500** : Surcharge serveur (retry automatique)
- **Timeouts** : Retry avec backoff exponentiel
- **Troncature** : Fallback automatique (température)

---

*Documentation générée automatiquement - Dernière mise à jour : Janvier 2025*
