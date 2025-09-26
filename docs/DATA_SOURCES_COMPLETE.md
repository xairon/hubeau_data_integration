# 🌊 Sources de Données Complètes - Hub'Eau Pipeline
## Documentation Unifiée : APIs, Référentiels, Ontologies

---

## 📋 **Vue d'Ensemble**

Ce document centralise **toutes les sources de données** du pipeline Hub'Eau, basé sur les documentations officielles et les besoins d'intégration identifiés.

### **📊 Synthèse des Sources**
| **Catégorie** | **Sources** | **Volume/Fréquence** | **Usage Principal** |
|---------------|-------------|---------------------|-------------------|
| **🌊 APIs Hub'Eau** | 8 APIs temps réel | 36K obs/jour | Données opérationnelles |
| **🗺️ Référentiels Géo** | BDLISA WFS | Trimestriel | Contexte spatial |
| **📚 Thésaurus** | Sandre APIs | Mensuel | Normalisation |
| **🔗 Ontologies** | SOSA/SSN W3C | Annuel | Modélisation sémantique |

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

Endpoints_Disponibles:
  /stations:          # Métadonnées piézomètres
  /chroniques:        # Séries historiques quotidiennes (exige code_bss)
  /chroniques_tr:     # Données temps réel horaires (télétransmission)

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
- **URL API** : `https://hubeau.eaufrance.fr/api/v1/hydrometrie/`
- **Source** : Service Central Vigicrues (SCHAPI)
- **Stations** : **~3,000 stations** (estimation réseau national)

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v1/hydrometrie"

Endpoints_Disponibles:
  /stations:          # Métadonnées stations hydrométriques
  /observations:      # Observations hydrométriques (exige code_station)
  /observations_tr:   # Observations temps réel (pagination curseur)

Structure_Données:
  code_station: "K037041001"          # Code station hydro
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
- **URL API** : `https://hubeau.eaufrance.fr/api/v1/qualite_cours_eau/`
- **Source** : NAIADES (système d'information sur l'eau)
- **Stations** : **~2,000 stations** qualité surface

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v1/qualite_cours_eau"

Endpoints_Disponibles:
  /station_pc:        # Stations de prélèvement qualité (selon doc officielle)
  /analyse_pc:        # Analyses physico-chimiques (selon doc officielle)

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
- **Stations** : **~1,200 stations** surveillance nappes

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v1/qualite_nappes"

Endpoints_Disponibles:
  /analyses:          # Analyses qualité nappes (selon doc officielle)

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
- **Stations** : **~760 stations** (dont ~50 actives selon doc)
- **Mesure** : Température continue des cours d'eau

#### **Endpoints & Données**
```yaml
Base_URL: "https://hubeau.eaufrance.fr/api/v1/temperature"

Endpoints_Disponibles:
  /station:           # Stations de mesure température (singulier selon doc!)
  /chronique:         # Chroniques températures (singulier selon doc!)

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
- **Réseau** : **3,200+ stations ONDE** (Observatoire National Des Étiages)
- **Type** : Observations visuelles par agents OFB
- **Standard** : OpenAPI 3.0

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
- **URL** : `https://hubeau.eaufrance.fr/api/v1/hydrobio/`
- **Source** : NAIADES (peuplement cours d'eau)
- **Stations** : **~1,500 stations** analyses biologiques

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
- **URL** : `https://hubeau.eaufrance.fr/api/v1/prelevements/`
- **Données** : Volumes prélevés par usage
- **Couverture** : France entière (déclarations)

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

**📅 Dernière mise à jour** : Septembre 2024  
**🎯 Version** : 1.0 - Documentation unifiée sources complètes
