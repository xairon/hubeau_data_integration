# 🌊 Sources de Données Complètes - Hub'Eau Pipeline
## Documentation Unifiée : APIs, Référentiels, Ontologies

---

## 📋 **Vue d'Ensemble**

Ce document centralise **toutes les sources de données** du pipeline Hub'Eau, basé sur les documentations officielles et les besoins d'intégration identifiés.

### **📊 Synthèse des Sources**
| **Catégorie** | **Sources** | **Volume/Fréquence** | **Usage Principal** | **Status** |
|---------------|-------------|---------------------|-------------------|------------|
| **🌊 APIs Hub'Eau** | 8 APIs temps réel | ~8,500 obs/jour | Données opérationnelles | ✅ Opérationnel |
| **🗺️ Référentiels Géo** | BDLISA WFS | Trimestriel | Contexte spatial | ✅ Intégré |
| **📚 Thésaurus** | Sandre APIs | Mensuel | Normalisation | ✅ Intégré |
| **🔗 Ontologies** | SOSA/SSN W3C | Annuel | Modélisation sémantique | 🔮 Future |

---

## 🌊 **Hub'Eau - 8 APIs Opérationnelles**

### **🏔️ 1. API Piézométrie**
- **URL Officielle** : [hubeau.eaufrance.fr/page/api-piezometrie](https://hubeau.eaufrance.fr/page/api-piezometrie)
- **URL API** : `https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/`
- **Source ADES** : Accès aux Données sur les Eaux Souterraines
- **Stations** : **~1,500 piézomètres temps réel** (doc officielle)
- **Fréquence** : Télétransmission horaire + historique quotidien

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes"

Endpoints_Complets:
  /stations:          # ✅ RÉFÉRENTIEL - Métadonnées piézomètres
  /chroniques:        # ✅ HISTORIQUE - Séries quotidiennes (exige code_bss)
  /chroniques_tr:     # ✅ TEMPS RÉEL - Données horaires (télétransmission)

Différences_Chroniques_vs_Chroniques_TR:
  chroniques_tr:
    fréquence: "Horaire (télétransmission)"
    période: "Données récentes (derniers jours)"
    lookback_days: 1
    usage: "Monitoring temps réel"
    
  chroniques:
    fréquence: "Quotidienne" 
    période: "Données historiques (plusieurs années)"
    lookback_days: 365
    usage: "Analyse historique"

Structure_Données_Chroniques_TR:
  code_bss: "06252X0063/PZ1"           # Code national BSS
  niveau_eau_ngf: 207.52               # Cote NGF (mètres)
  profondeur_nappe: 6.48               # Profondeur vs repère (mètres)  
  date_mesure: "2020-02-05T08:00:00Z"  # Date UTC
  timestamp_mesure: 1580889600000      # Timestamp Unix (millisecondes)
  longitude: 4.84129654001931
  latitude: 46.3705354087062
  altitude_repere: 214.0               # Altitude repère NGF

Paramètres_Techniques:
  size_default: 5000                   # Taille par défaut selon doc
  size_max: 20000                      # Taille maximale
  pagination_depth: "20,000 enregistrements"
  availability: "99.97% (Netvigie monitoring)"
  formats: ["JSON", "GeoJSON", "CSV"]
  url_max_length: "2083 caractères"
```

### **🌊 2. API Hydrométrie**  
- **URL Officielle** : [hubeau.eaufrance.fr/page/api-hydrometrie](https://hubeau.eaufrance.fr/page/api-hydrometrie)
- **URL API** : `https://hubeau.eaufrance.fr/api/v2/hydrometrie/` ⚠️ **VERSION v2**
- **Source** : Service Central Vigicrues (SCHAPI)
- **Stations** : **~3,000 stations** (estimation réseau national)
- **⚠️ RESTRICTION TEMPORELLE** : **Pas d'accès aux données antérieures à 1 mois** (erreur 400 si date_debut_obs < 1 mois)

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v2/hydrometrie"

Endpoints_Complets:
  /referentiel/sites:     # ✅ RÉFÉRENTIEL - Tronçons de cours d'eau
  /referentiel/stations:  # ✅ RÉFÉRENTIEL - Stations d'observation
  /observations_tr:       # ✅ TEMPS RÉEL - Observations temps réel (pagination cursor)
  /obs_elab:             # ✅ ÉLABORÉES - Observations validées/traitées

Différences_Observations_TR_vs_Obs_Elab:
  observations_tr:
    fréquence: "Temps réel (2 min)"
    période: "24h-1 mois"
    statut: "Non validées"
    usage: "Monitoring opérationnel"
    pagination: "cursor"
    
  obs_elab:
    fréquence: "Traitement différé"
    période: "Historique long terme"
    statut: "Validées et traitées"
    usage: "Analyses approfondies"
    pagination: "cursor"

Structure_Données:
  code_station: "K037041001"          # Code station hydro
  code_site: "SITE1234567890"         # Code site (v2)
  hauteur_eau: 1250                   # Hauteur en millimètres
  debit: 15.7                         # Débit en litres/seconde
  date_obs: "2020-02-05T08:00:00Z"    # Date observation UTC
  qualite_obs: "BONNE"                # Qualité observation
  longitude: 2.3522
  latitude: 48.8566
  
Paramètres_Techniques:
  size_default: 5000                  # Taille par défaut
  size_max: 20000                     # Taille maximale
  pagination_type: "curseur pour observations_tr"
  pagination_depth: "20,000 enregistrements"
  formats: ["JSON", "GeoJSON", "CSV"]
  
Conversions_Requises:
  hauteur: "mm → m"
  debit: "L/s → m³/s"
  timezone: "UTC"
```

### **🧪 3. API Qualité des Cours d'Eau**
- **URL Officielle** : [hubeau.eaufrance.fr/page/api-qualite-cours-deau](https://hubeau.eaufrance.fr/page/api-qualite-cours-deau)
- **URL API** : `https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/` ⚠️ **VERSION v2**
- **Source** : NAIADES (système d'information sur l'eau)
- **Stations** : **~15,000 stations** qualité surface

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v2/qualite_rivieres"

Endpoints_Complets:
  /station_pc:                    # ✅ RÉFÉRENTIEL - Stations de prélèvement qualité
  /operation_pc:                  # ✅ OPÉRATIONS - Opérations de prélèvement
  /condition_environnementale_pc: # ✅ CONDITIONS - Conditions environnementales
  /analyse_pc:                    # ✅ ANALYSES - Analyses physico-chimiques

Hiérarchie_Données_Qualité:
  Station_PC:
    - code_station: "STATION1234567890"
    - nom_station: "Station Qualité Seine"
    - cours_eau: "Seine"
    - coordonnées: "longitude, latitude"
    
  Operation_PC:
    - code_operation: "OP1234567890"
    - code_station: "STATION1234567890"
    - date_prelevement: "2024-01-15"
    - type_prelevement: "Physico-chimique"
    - profondeur_prelevement: 0.5  # mètres
    
  Condition_Environnementale_PC:
    - code_operation: "OP1234567890"
    - code_station: "STATION1234567890"
    - temperature_air: 15.2  # °C
    - presence_feuilles: "NON"
    - presence_mousses: "OUI"
    - presence_irisations: "NON"
    
  Analyse_PC:
    - code_operation: "OP1234567890"
    - code_station: "STATION1234567890"
    - code_parametre: "1340"  # Nitrates
    - nom_parametre: "Nitrates"
    - resultat: 25.4  # mg/L
    - limite_qualite: 50.0  # mg/L

Structure_Données_Analyse:
  code_station: "05138000"            # Code station de prélèvement
  date_prelevement: "2020-02-05"      # Date prélèvement
  code_parametre: "1301"              # Code Sandre paramètre
  libelle_parametre: "Température de l'Eau"
  resultat: 12.5                      # Valeur mesurée
  unite: "°C"                         # Unité de mesure
  symbole_unite: "°C"
  code_qualification: "1"             # Qualité mesure
  
Paramètres_Techniques:
  size_default: 5000                  # Taille par défaut
  size_max: 20000                     # Taille maximale
  pagination_depth: "20,000 enregistrements"
  formats: ["JSON", "GeoJSON", "CSV"]
  
Paramètres_Sandre_Prioritaires:
  "1301": {libelle: "Température", unite: "°C", theme: "Physico-chimie"}
  "1303": {libelle: "pH", unite: "unité pH", theme: "Physico-chimie"}
  "1304": {libelle: "Conductivité", unite: "µS/cm", theme: "Physico-chimie"}
  "1335": {libelle: "Oxygène dissous", unite: "mg/L", theme: "Physico-chimie"}
  "1340": {libelle: "DBO5", unite: "mg/L", theme: "Nutriments"}
```

### **💧 4. API Qualité des Nappes**
- **URL Officielle** : [hubeau.eaufrance.fr/page/api-qualite-nappes](https://hubeau.eaufrance.fr/page/api-qualite-nappes)
- **URL API** : `https://hubeau.eaufrance.fr/api/v1/qualite_nappes/`
- **Source** : ADES + NAIADES  
- **Stations** : **~50,000 points d'eau** surveillance nappes

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v1/qualite_nappes"

Endpoints_Complets:
  /stations:          # ✅ RÉFÉRENTIEL - Points d'eau (puits, forages, sources)
  /analyses:          # ✅ ANALYSES - Analyses physico-chimiques

Hiérarchie_Données_Qualité_Nappes:
  Stations:
    - code_bss: "06252X0063/PZ1"
    - nom_station: "Puits Principal"
    - type_point_eau: "Puits" | "Forage" | "Source"
    - coordonnées: "longitude, latitude"
    - profondeur: 45.2  # mètres
    
  Analyses:
    - code_bss: "06252X0063/PZ1"
    - code_parametre: "1340"  # Nitrates
    - nom_parametre: "Nitrates"
    - resultat: 25.4  # mg/L
    - date_prelevement: "2024-01-15"
    - limite_qualite: 50.0  # mg/L

Structure_Données_Analyse:
  code_bss: "06252X0063/PZ1"          # Code BSS point d'eau
  date_prelevement: "2020-02-05"      # Date prélèvement
  code_parametre: "1340"              # Code Sandre paramètre
  libelle_parametre: "Nitrates"       # Libellé paramètre
  resultat: 25.0                      # Concentration mesurée
  unite: "mg/L"                       # Unité de mesure
  code_qualification: "1"             # Qualité mesure
  longitude: 4.84129654001931
  latitude: 46.3705354087062
  
Paramètres_Techniques:
  size_default: 5000                  # Taille par défaut
  size_max: 20000                     # Taille maximale
  pagination_depth: "20,000 enregistrements"
  formats: ["JSON", "GeoJSON", "CSV"]
  
Substances_Prioritaires:
  nitrates: {code: "1340", seuil_legal: "50 mg/L"}
  pesticides: {code: "1506", seuil_detection: "0.1 µg/L"}
  arsenic: {code: "1369", seuil_legal: "10 µg/L"}
  bacteriologie: {code: "1506", unite: "UFC/100mL"}
```

### **🌡️ 5. API Température Continue**
- **URL Officielle** : [hubeau.eaufrance.fr/page/api-temperature-continu](https://hubeau.eaufrance.fr/page/api-temperature-continu)
- **URL API** : `https://hubeau.eaufrance.fr/api/v1/temperature/`
- **Source** : Banque Naïades
- **Stations** : **~500 stations thermiques** (réseau thermique national)
- **Mesure** : Température continue des cours d'eau
- **⚠️ LIMITATIONS** : **Peu de stations encore en service** et **pas de données après 2024** via l'API

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v1/temperature"

Endpoints_Complets:
  /station:           # ✅ RÉFÉRENTIEL - Stations de mesure température (singulier selon doc!)
  /chronique:         # ✅ TEMPORELLES - Chroniques températures (singulier selon doc!)

Hiérarchie_Données_Température:
  Station:
    - code_station: "T1234567890"
    - nom_station: "Station Thermique Principal"
    - coordonnées: "longitude, latitude"
    - cours_eau: "Rivière XYZ"
    - type_mesure: "Température continue"
    
  Chronique:
    - code_station: "T1234567890"
    - temperature: 15.2  # °C
    - date_mesure: "2024-01-15T08:00:00Z"
    - qualite_mesure: "BONNE"
    - profondeur_mesure: 0.5  # mètres

Structure_Données_Chronique:
  code_station: "04051125"             # Code station de mesure
  date_mesure_temp: "2013-05-17"       # Date mesure (champ spécifique temp)
  heure_mesure_temp: "05:00:00"        # Heure mesure
  resultat: 13.209                     # Température mesurée
  code_unite: "27"                     # Code unité Sandre
  symbole_unite: "°C"                  # Symbole unité
  code_qualification: "4"              # Code qualité mesure
  libelle_qualification: "Non qualifié"
  longitude: 2.0486187
  latitude: 47.812892122
  libelle_cours_eau: "la Dhuy"
  
Paramètres_Techniques:
  size_default: 5000                   # Taille par défaut
  size_max: 20000                      # Taille maximale
  pagination_depth: "20,000 enregistrements"
  formats: ["JSON", "GeoJSON", "CSV"]
  update_frequency: "Trimestrielle (Naïades)"
```

### **🌊 6. API Écoulement des Cours d'Eau (ONDE)**
- **URL Officielle** : [hubeau.eaufrance.fr/api-ecoulement](https://hubeau.eaufrance.fr/page/api-ecoulement)
- **URL API** : `https://hubeau.eaufrance.fr/api/v1/ecoulement/`
- **Réseau** : **~3,000 stations ONDE** (Observatoire National Des Étiages)
- **Type** : Observations visuelles par agents OFB
- **Standard** : OpenAPI 3.0

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v1/ecoulement"

Endpoints_Complets:
  /stations:           # ✅ RÉFÉRENTIEL - Stations d'observation des écoulements
  /campagnes:          # ✅ CAMPAGNES - Campagnes d'observation des écoulements
  /observations:       # ✅ OBSERVATIONS - Observations visuelles de l'écoulement

Hiérarchie_Données_ONDE:
  Stations:
    - code_station: "ONDE1234567890"
    - nom_station: "Station ONDE Seine"
    - cours_eau: "Seine"
    - coordonnées: "longitude, latitude"
    - type_station: "Observation visuelle"
    
  Campagnes:
    - code_campagne: "CAMP1234567890"
    - code_station: "ONDE1234567890"
    - date_campagne: "2024-07-15"
    - periode_campagne: "Été"
    - agent_observateur: "OFB Départemental"
    - conditions_meteo: "Soleil"
    
  Observations:
    - code_observation: "OBS1234567890"
    - code_campagne: "CAMP1234567890"
    - code_station: "ONDE1234567890"
    - date_observation: "2024-07-15T10:30:00Z"
    - code_ecoulement: "1"  # Écoulement visible
    - libelle_ecoulement: "Écoulement visible"
    - commentaire: "Écoulement normal"

#### **Modalités d'Observation**
```yaml
Écoulement_Codes:
  "1": "Écoulement visible"
  "2": "Écoulement non visible"  
  "3": "Assec"
  "4": "Observation impossible"

Période: "Mai - Octobre (saisonnière)"
Agents: "OFB départementaux"
Couverture: "France hexagonale + Corse"
```

### **🐟 7. API Hydrobiologie**
- **URL Officielle** : [hubeau.eaufrance.fr/page/api-hydrobiologie](https://hubeau.eaufrance.fr/page/api-hydrobiologie)
- **URL API** : `https://hubeau.eaufrance.fr/api/v1/hydrobio/`
- **Source** : NAIADES (peuplement cours d'eau)
- **Stations** : **~1,500 stations** analyses biologiques

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v1/hydrobio"

Endpoints_Complets:
  /stations_hydrobio:  # ✅ RÉFÉRENTIEL - Stations de prélèvement hydrobiologique
  /indices:           # ✅ INDICES - Indices biologiques calculés
  /taxons:            # ✅ TAXONS - Taxons identifiés lors des prélèvements

Hiérarchie_Données_Hydrobiologie:
  Stations_Hydrobio:
    - code_station: "HYDRO1234567890"
    - nom_station: "Station Hydrobio Seine"
    - cours_eau: "Seine"
    - coordonnées: "longitude, latitude"
    - type_prelevement: "Macroinvertébrés"
    
  Indices:
    - code_station: "HYDRO1234567890"
    - code_indice: "IBGN"  # Indice Biologique Global Normalisé
    - valeur_indice: 8.5
    - date_prelevement: "2024-01-15"
    - classe_qualite: "BONNE"
    - interpretation: "Qualité biologique bonne"
    
  Taxons:
    - code_station: "HYDRO1234567890"
    - code_taxon: "TAXON1234567890"
    - nom_taxon: "Gammarus pulex"
    - famille: "Gammaridae"
    - abondance: 15
    - date_prelevement: "2024-01-15"
    - groupe_taxonomique: "Crustacés"

#### **Types d'Analyses & Indices**
```yaml
Macroinvertébrés:
  indices: ["IBGN", "I2M2"]
  description: "Invertébrés benthiques"
  
Diatomées:
  indices: ["IBD", "IPS"] 
  description: "Diatomées benthiques"
  
Macrophytes:
  indices: ["IBMR"]
  description: "Végétaux aquatiques"
  
Poissons:
  indices: ["IPR"]
  description: "Peuplements piscicoles"
```

### **🚰 8. API Prélèvements d'Eau**
- **URL Officielle** : [hubeau.eaufrance.fr/page/api-prelevements-en-eau](https://hubeau.eaufrance.fr/page/api-prelevements-en-eau)
- **URL API** : `https://hubeau.eaufrance.fr/api/v1/prelevements/`
- **Données** : Volumes prélevés par usage
- **Couverture** : France entière (déclarations)

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v1/prelevements"

Endpoints_Complets:
  /referentiel/points_prelevement:  # ✅ RÉFÉRENTIEL - Points physiques de prélèvement
  /referentiel/ouvrages:           # ✅ RÉFÉRENTIEL - Installations techniques
  /chroniques:                     # ✅ CHRONIQUES - Volumes annuels par ouvrage

Hiérarchie_Données_Prélèvements:
  Ouvrages:
    - code_ouvrage: "OPR1234567890"
    - description: "Station de pompage principale"
    - relation: "1 ouvrage → N points de prélèvement"
    
  Points_Prélèvement:
    - code_point: "PPR1234567890" 
    - description: "Puits n°1 de l'ouvrage"
    - relation: "1 point → 1 ouvrage"
    
  Chroniques:
    - code_ouvrage: "OPR1234567890"  # ⚠️ IMPORTANT: lié à l'OUVRAGE
    - annee: 2023
    - volume_preleve: 15000  # m³/an
    - usage: "AEP"  # Alimentation Eau Potable

#### **Types d'Usage**
```yaml
Usages_Catégories:
  AEP: "Alimentation Eau Potable"
  IND: "Industriel"
  IRR: "Irrigation"
  ENE: "Énergétique (refroidissement)"
  AQU: "Aquaculture"
```

---

## ⚠️ **Restrictions Temporelles des APIs Hub'Eau**

### **🔒 Limitations Temporelles Critiques**

```yaml
APIs_Avec_Restrictions:

  Hydrométrie_v2:
    restriction: "Pas d'accès aux données antérieures à 1 mois"
    erreur: "400 Client Error: date can't be < 1 month from now"
    impact: "Impossible de récupérer des données historiques récentes"
    solution: "Utiliser lookback_days >= 30 pour éviter les erreurs"
    
  Température:
    limitation: "Peu de stations encore en service"
    limitation_data: "Pas de données après 2024 via l'API"
    impact: "Couverture réduite et données obsolètes"
    recommandation: "Vérifier la disponibilité avant intégration"

APIs_Sans_Restrictions:
  - Piézométrie: "Accès historique complet"
  - Qualité_Cours_Eau: "Accès historique complet"
  - Qualité_Nappes: "Accès historique complet"
  - Écoulement_ONDE: "Données saisonnières disponibles"
  - Hydrobiologie: "Données selon campagnes"
  - Prélèvements: "Données annuelles disponibles"

Recommandations_Techniques:
  configuration_temporelle:
    hydrometrie: "lookback_days: 30 minimum"
    temperature: "lookback_days: 365 (données historiques limitées)"
    autres_apis: "lookback_days: selon besoins (1-365)"
    
  gestion_erreurs:
    pattern: "Vérifier erreur 400 avec message temporel"
    fallback: "Réduire la période de recherche"
    monitoring: "Logger les restrictions temporelles"
```

---

## 📊 **Synthèse des Corrections Apportées**

### **🔧 Corrections Majeures Effectuées**

```yaml
APIs_Corrigées:
  
  Piézométrie:
    problème: "chroniques sans code_bss → erreur 400"
    solution: "Ajout logique récupération codes BSS depuis stations"
    endpoints: ["stations", "chroniques", "chroniques_tr"]
    
  Hydrométrie:
    problème: "Configuration incomplète v2 + restriction temporelle 1 mois"
    solution: "Ajout referentiel/sites et obs_elab + correction lookback_days"
    endpoints: ["referentiel/sites", "referentiel/stations", "observations_tr", "obs_elab"]
    restriction: "Pas d'accès aux données antérieures à 1 mois"
    
  Qualité_Cours_Eau:
    problème: "Configuration incomplète v2"
    solution: "Ajout operation_pc et condition_environnementale_pc"
    endpoints: ["station_pc", "operation_pc", "condition_environnementale_pc", "analyse_pc"]
    
  Prélèvements:
    problème: "chroniques sans code_ouvrage → erreur 500"
    solution: "Ajout referentiel/ouvrages et logique codes ouvrage"
    endpoints: ["referentiel/points_prelevement", "referentiel/ouvrages", "chroniques"]
    
  Température:
    limitation: "Peu de stations en service + pas de données après 2024"
    impact: "Couverture réduite et données obsolètes"
    recommandation: "Vérifier disponibilité avant intégration"

Différences_Clés_Entre_Endpoints:
  
  Piézométrie:
    chroniques_tr: "Temps réel horaire (1 jour lookback)"
    chroniques: "Historique quotidien (365 jours lookback)"
    
  Hydrométrie:
    observations_tr: "Non validées, temps réel (1 jour lookback)"
    obs_elab: "Validées et traitées (30 jours lookback)"
    
  Qualité_Cours_Eau:
    station_pc: "Référentiel géographique"
    operation_pc: "Opérations de prélèvement"
    condition_environnementale_pc: "Contexte des prélèvements"
    analyse_pc: "Résultats des analyses"

Logique_Récupération_Codes:
  
  Piézométrie_Qualité_Nappes:
    source: "stations → code_bss"
    usage: "chroniques avec code_bss"
    
  Prélèvements:
    source: "ouvrages → code_ouvrage"
    usage: "chroniques avec code_ouvrage"
    
  Autres_APIs:
    source: "stations → code_station"
    usage: "observations avec code_station"
```

---

## 🗺️ **BDLISA - Référentiel Hydrogéologique**

### **📋 Informations Officielles**
- **Source** : [BDLISA - Base de Données des Limites des Systèmes Aquifères](https://bdlisa.eaufrance.fr/)
- **Organisme** : BRGM + OFB (Système d'Information sur l'Eau)
- **Type** : Référentiel cartographique hydrogéologique national
- **Couverture** : France métropolitaine + DOM-TOM

### **🔌 Services Géospatiaux**
```yaml
Services_WFS_WMS:
  base_url: "https://bdlisa.eaufrance.fr/geoserver/"
  
Couches_Principales:
  Formation_Aquifere:          # Formations aquifères
    features: ~2500
    attributes: [code, nom, type_aquifere, lithologie, permeabilite]
    
  Formation_Impermeable:       # Formations imperméables
    features: ~800
    attributes: [code, nom, lithologie, role_hydrogeo]
    
  Masses_Eau_Souterraine:      # Masses d'eau DCE
    features: ~697
    attributes: [code_me, nom_me, statut_qualitatif, statut_quantitatif]
    
  Limites_Administratives:     # Découpages territoriaux
    features: ~36000
    attributes: [code_insee, nom_commune, code_departement]
```

### **📊 Classification Hydrogéologique**
```yaml
Types_Aquifères:
  LIBRE:
    écoulement: "Nappe libre"
    milieu: ["Poreux", "Fracturé", "Karstique"]
    productivité: ["Faible", "Moyenne", "Élevée"]
    
  CAPTIF:
    écoulement: "Nappe captive"
    pression: "Artésienne possible"
    profondeur: "Variable (10-500m)"
    
Formations_Imperméables:
  BARRAGE: "Barrière étanche"
  SEMI_PERMEABLE: "Aquitard - écoulement retardé"
  DRAIN: "Drainage préférentiel"
```

---

## 📚 **Sandre - Référentiel Thématique National**

### **📋 Informations Officielles**
- **Source** : [Sandre - Service d'Administration Nationale des Données sur l'Eau](https://www.sandre.eaufrance.fr/v2/)
- **Organisme** : OFB + OiEau
- **Type** : Référentiel technique et thésaurus du domaine de l'eau
- **Statut** : Nomenclatures officielles françaises

### **🔌 APIs & Services Disponibles**
```yaml
APIs_Sandre:
  api_referentiel: "https://api.sandre.eaufrance.fr/referentiel/"
  api_definition: "https://api.sandre.eaufrance.fr/definition/"
  api_recherche: "https://api.sandre.eaufrance.fr/recherche/"
  sparql_endpoint: "https://sparql.sandre.eaufrance.fr/"
  
Services_Utiles:
  - Convertisseur codes
  - Générateur BDD
  - Testeur fichiers d'échange
  - Évolution communes
  - Endpoint SPARQL
```

### **📊 Nomenclatures Essentielles Hub'Eau**
```yaml
Paramètres_Qualité:
  total_items: ~3000
  exemples:
    "1301": {libelle: "Température de l'Eau", unite: "°C", theme: "Physico-chimie"}
    "1340": {libelle: "Nitrates (en NO3)", unite: "mg/L", theme: "Nutriments"}
    "1303": {libelle: "pH", unite: "unité pH", theme: "Physico-chimie"}
    
Unités_Mesure:
  total_items: ~500
  exemples:
    "27": {symbole: "°C", libelle: "Degré Celsius", type: "Température"}
    "13": {symbole: "mg/L", libelle: "Milligramme par litre", type: "Concentration"}
    
Méthodes_Analyse:
  total_items: ~1200
  exemples:
    "130": {libelle: "Thermométrie", principe: "Mesure directe", domaine: "Terrain"}
    "24": {libelle: "Spectrophotométrie UV", principe: "Spectrophotométrie", domaine: "Laboratoire"}
    
Supports_Observation:
  exemples:
    "3": {libelle: "Eau brute", definition: "Eau naturelle non traitée"}
    "23": {libelle: "Eau souterraine", definition: "Eau présente dans les nappes"}
    
Fractions_Analysées:
  "23": {libelle: "Fraction dissoute", definition: "Fraction passant au travers d'un filtre 0,45 μm"}
  "28": {libelle: "Fraction particulaire", definition: "Fraction retenue par un filtre 0,45 μm"}
```

---

## 🔗 **SOSA/SSN - Ontologie Sémantique W3C**

### **📋 Standard International**
- **Source** : [W3C Semantic Sensor Network Ontology](https://www.w3.org/2021/sdw/ssn/)
- **Standard** : W3C Recommendation
- **Alignement** : ISO 19156 (Observations & Measurements)
- **Statut** : Nouvelle édition en préparation

### **🎯 Concepts Clés SOSA**
```yaml
Classes_Principales:
  sosa:Sensor:
    definition: "Device that detects or measures a property"
    exemples: ["Thermometer", "Piezometer", "pH meter"]
    
  sosa:Observation:
    definition: "Act of carrying out an observation procedure"
    propriétés: ["phenomenonTime", "resultTime", "hasResult"]
    
  sosa:ObservableProperty:
    definition: "Quality of a feature that can be observed"
    exemples: ["Temperature", "pH", "Water level"]
    
  sosa:FeatureOfInterest:
    definition: "Thing whose property is being observed"
    exemples: ["River", "Aquifer", "Water body"]
    
  sosa:Sample:
    definition: "Feature sampled in an act of sampling"
    usage: "Water samples for laboratory analysis"

Propriétés_Essentielles:
  sosa:observedProperty:
    domaine: "sosa:Observation"
    range: "sosa:ObservableProperty"
    
  sosa:madeBySensor:
    domaine: "sosa:Observation"
    range: "sosa:Sensor"
    
  sosa:hasFeatureOfInterest:
    domaine: "sosa:Observation"
    range: "sosa:FeatureOfInterest"
    
  sosa:phenomenonTime:
    domaine: "sosa:Observation"
    range: "xsd:dateTime"
```

### **🔄 Mapping Hub'Eau → SOSA**
```yaml
Correspondances:
  Stations_Hub'Eau → sosa:Sensor:
    piezometer: "Capteur niveau nappe"
    hydrometric_station: "Capteur débit/hauteur"
    quality_station: "Capteur qualité eau"
    temperature_sensor: "Capteur thermique"
    
  Observations_Hub'Eau → sosa:Observation:
    water_level: "Observation niveau"
    flow_rate: "Observation débit"
    temperature: "Observation température"
    chemical_analysis: "Observation chimique"
    
  Paramètres_Sandre → sosa:ObservableProperty:
    "1301": "Propriété température"
    "1340": "Propriété nitrates"
    "1303": "Propriété pH"
    
  Entités_BDLISA → sosa:FeatureOfInterest:
    aquifer: "Entité aquifère"
    river: "Entité cours d'eau"
    formation: "Entité géologique"
```

---

## ⚙️ **Configuration Technique Intégration**

### **🔧 Paramètres APIs Hub'Eau**
```yaml
Rate_Limits_Globaux:
  recommandé: "1-2 req/sec par API"
  retry_strategy: "Exponential backoff (2^n)"
  timeout: "30-60 secondes"
  
Pagination_Optimisée:
  size_optimal: "1000-5000"
  size_max_global: "20000"
  depth_limit: "20000 enregistrements"
  
Formats_Supportés: ["JSON", "GeoJSON", "CSV"]
Protocoles: ["HTTP", "HTTPS", "CORS", "JSONP"]

Limitations_Techniques:
  url_max_length: "2083 caractères"
  pagination_depth: "20,000 enregistrements max"
```

### **🗺️ Configuration BDLISA WFS**
```yaml
WFS_Paramètres:
  service: "WFS"
  version: "2.0.0"
  request: "GetFeature"
  outputFormat: "application/gml+xml;version=3.2"
  srsName: "EPSG:2154"  # RGF93/Lambert-93
  
Optimisations:
  bbox_filtering: "Recommandé pour grandes requêtes"
  feature_limit: "Pagination manuelle nécessaire"
  cache_duration: "Trimestriel (référentiel stable)"
```

### **📚 Configuration Sandre APIs**
```yaml
API_Endpoints:
  base_url: "https://api.sandre.eaufrance.fr"
  format: "JSON"
  encoding: "UTF-8"
  
Rate_Limiting:
  respectueux: "0.5-1 req/sec"
  bulk_downloads: "Préférer téléchargements complets"
  cache_recommended: "Mensuel minimum"
```

---

## 📊 **Stratégie d'Ingestion Recommandée**

### **⏰ Planning Optimal**
```yaml
Quotidien:
  - Hub'Eau APIs principales (Piézo, Hydro, Temp, Qualité)
  - Volume: ~8,500 obs/jour optimisé
  
Hebdomadaire:
  - Hub'Eau Écoulement (selon campagnes saisonnières)
  - Hub'Eau Hydrobiologie (selon campagnes)
  
Mensuel:
  - Sandre nomenclatures (évolutions)
  - Hub'Eau Prélèvements (déclarations)
  
Trimestriel:
  - BDLISA formations (référentiel stable)
  
Annuel:
  - SOSA/SSN ontologies (updates W3C)
```

### **🎯 Volumes Totaux Maîtrisés**
```yaml
Volume_Quotidien_Optimisé:
  observations: 8500
  réduction_vs_production: "94%"
  
Volume_Annuel_Estimé:
  observations: ~3.2M
  référentiels: ~50K entrées
  ontologies: ~1K concepts
  
Stockage_Estimé:
  bronze_minio: ~500GB/an
  silver_specialized: ~200GB/an
  gold_sosa: ~50GB/an
```

---

## 📚 **Références & Standards**

### **🔗 Sources Officielles Hub'Eau**
- [Portail Principal Hub'Eau](https://hubeau.eaufrance.fr/page/apis)
- [API Piézométrie](https://hubeau.eaufrance.fr/page/api-piezometrie)
- [API Écoulement ONDE](https://hubeau.eaufrance.fr/page/api-ecoulement)
- [API Hydrobiologie](https://hubeau.eaufrance.fr/page/api-hydrobiologie)
- [API Qualité des nappes](https://hubeau.eaufrance.fr/page/api-qualite-nappes)
- [API Hydrométrie](https://hubeau.eaufrance.fr/page/api-hydrometrie)
- [API Température des cours d'eau](https://hubeau.eaufrance.fr/page/api-temperature-continu)
- [API Qualité des cours d'eau](https://hubeau.eaufrance.fr/page/api-qualite-cours-deau)
- [API Prélèvements en eau](https://hubeau.eaufrance.fr/page/api-prelevements-eau)
- [Statistiques Usage 2023](https://hubeau.eaufrance.fr/page/statistiques-2023)

### **🌐 Références Externes**
- [BDLISA BRGM](https://bdlisa.eaufrance.fr/)
- [Sandre OFB](https://www.sandre.eaufrance.fr/v2/)
- [SOSA/SSN W3C](https://www.w3.org/2021/sdw/ssn/)
- [ISO 19156 OMS](https://www.iso.org/standard/32574.html)

### **🔧 Standards Techniques**
- **Géospatial** : OGC WFS 2.0, GML 3.2, EPSG:2154
- **Sémantique** : RDF/OWL, SPARQL, W3C Recommendations
- **APIs** : REST, OpenAPI 3.0, JSON, pagination standard
- **Qualité** : FAIR Data principles, ISO metadata

---

**📅 Dernière mise à jour** : Septembre 2025  
**🎯 Version** : 1.1 - Documentation unifiée avec restrictions temporelles Hub'Eau
