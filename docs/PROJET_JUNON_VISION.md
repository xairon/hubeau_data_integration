# 🌊 PROJET JUNON - Vision et Architecture

**JUNON** : **Jumeaux Numériques** au service des ressources naturelles en région Centre-Val de Loire

**Programme officiel BRGM** | Budget : 12,3M€ | Durée : 5 ans | 10 projets collaboratifs

> 🌐 Site officiel : [junon-cvl.fr](https://www.junon-cvl.fr/fr)

---

## 📋 Table des matières

1. [Introduction](#introduction)
2. [Qu'est-ce qu'un Jumeau Numérique?](#quest-ce-quun-jumeau-numérique)
3. [JUNON - Un Jumeau Numérique Hydrologique](#junon---un-jumeau-numérique-hydrologique)
4. [Architecture de Données - L'Entrepôt comme Fondation](#architecture-de-données---lentrepôt-comme-fondation)
5. [La Couche d'Abstraction Ontologique](#la-couche-dabstraction-ontologique)
6. [SOSA - L'Ontologie pour nos Observations](#sosa---lontologie-pour-nos-observations)
7. [Connecter les Dimensions](#connecter-les-dimensions)
8. [Références](#références)

---

## 🎯 Introduction

**JUNON** (Jumeaux Numériques au service des ressources naturelles) est un **programme officiel du BRGM** en région Centre-Val de Loire visant à créer des **jumeaux numériques** pour la gestion des ressources naturelles (eau, sol, air). Il s'appuie sur l'intégration massive de données environnementales provenant des APIs Hub'Eau (portail national d'accès aux données sur l'eau).

> 📍 **Programme officiel** : JUNON est un programme du BRGM (Bureau de Recherches Géologiques et Minières) doté de **12,3 millions d'euros** sur **5 ans**, fédérant **10 projets collaboratifs** en région Centre-Val de Loire.  
> 🌐 **Site officiel** : [junon-cvl.fr](https://www.junon-cvl.fr/fr)  
> 👨‍🔬 **Coordinateur** : Sébastien Dupraz (BRGM)

### Contexte

La France dispose d'un patrimoine exceptionnel de données sur l'eau, collectées par de nombreux acteurs (agences de l'eau, BRGM, OFB, etc.) et exposées via Hub'Eau :
- 🌡️ Qualité des eaux souterraines et superficielles
- 📊 Piézométrie (niveaux des nappes)
- 🌊 Hydromérie (débits, hauteurs d'eau)
- 🦐 Hydrobiologie (indices biologiques, taxons)
- 💧 Prélèvements et usages de l'eau
- 🌡️ Température des cours d'eau
- 🏞️ Observations d'écoulement (ONDE)

### Objectif Global

Créer un **entrepôt de données unifié** servant de fondation à une **couche d'abstraction ontologique** permettant :
- ✅ D'interconnecter toutes les sources de données hétérogènes
- ✅ De représenter la connaissance hydrologique de manière cohérente
- ✅ De servir de base pour l'entraînement de modèles ML
- ✅ De permettre l'explicabilité et la traçabilité des analyses
- ✅ De faciliter la Business Intelligence multi-sources

---

## 🤖 Qu'est-ce qu'un Jumeau Numérique?

### Définition Officielle JUNON

Selon le **coordinateur du programme JUNON**, Sébastien Dupraz :

> "Un jumeau numérique est une reproduction virtuelle d'un objet ou d'un environnement qui, grâce à des méthodes d'intelligence artificielle, simule le comportement de son double réel afin de mieux le comprendre et le gérer."

**Source** : [Site officiel JUNON](https://www.junon-cvl.fr/fr)

### Caractéristiques d'un Jumeau Numérique

Un **jumeau numérique** (Digital Twin) est une représentation virtuelle d'un système physique qui :
- 📡 **Réplique** : Reflète fidèlement l'état du système réel
- 🔄 **Synchronise** : Se met à jour en continu avec les données réelles
- 🧠 **Simule** : Permet de tester des scénarios et prédire des comportements
- 🎯 **Optimise** : Aide à la prise de décision basée sur les données

### Les Piliers d'un Jumeau Numérique

1. **Données Réelles** : Flux continu d'observations depuis le système physique
2. **Modélisation** : Représentation structurée et sémantique de ces données
3. **Simulation/Prédiction** : Modèles permettant d'anticiper et optimiser
4. **Synchronisation Bidirectionnelle** : Interaction jumeau ↔ système réel (optionnel selon le type de jumeau)

### Exemples d'Applications

- 🏭 **Industrie** : Maintenance prédictive d'équipements (General Electric Predix, Siemens MindSphere)
- 🏙️ **Smart Cities** : Gestion urbaine (Virtual Singapore, Dubai Digital Twin)
- 🌍 **Environnement** : Gestion ressources en eau (Pays-Bas - Digital Twin Delfland, protection inondations)

### Différence avec un Entrepôt de Données Classique

| Aspect | Entrepôt Classique | Jumeau Numérique |
|--------|-------------------|------------------|
| **Focus** | Stockage et reporting | Représentation et simulation |
| **Données** | Historiques, batch | Temps réel + historique |
| **Modélisation** | Schémas relationnels | Ontologies + graphes de connaissances |
| **Usage** | BI, dashboards | BI + ML + simulation + prédiction |

---

## 🌊 JUNON - Programme Officiel BRGM

### Les 5 Axes du Programme JUNON

D'après le [site officiel](https://www.junon-cvl.fr/fr), JUNON s'articule autour de **5 axes de projets** :

1. **EAU** : Gestion ressources en eau (nappes, cours d'eau)
2. **SOL/AIR** : Qualité sols et air
3. **DATA** : Infrastructure de données et interopérabilité
4. **PRÉDICTION** : Modélisation prédictive et IA
5. **JUMEAUX NUMÉRIQUES** : Développement des jumeaux numériques

### Axe EAU - Notre Contribution

Ce projet d'intégration Hub'Eau s'inscrit dans **l'axe EAU** du programme JUNON :

**Objectifs** :
- 🗺️ Représentation virtuelle des systèmes hydrologiques de la région Centre-Val de Loire
- 🏞️ Intégration nappes phréatiques, cours d'eau, bassins versants
- 📍 Toutes les stations de mesure disponibles via Hub'Eau
- 📊 Observations historiques complètes (2000-2025)

**Périmètre géographique** : Région **Centre-Val de Loire** (prioritaire) avec potentiel d'extension nationale


## 🏗️ Architecture de Données - L'Entrepôt comme Fondation

JUNON adopte une architecture **simple et pragmatique** avec une **couche ontologique** (extension pour le raisonnement sémantique).

```
┌─────────────────────────────────────────────────────────────┐
│                    COUCHE ONTOLOGIQUE                       │
│         (Graphe de connaissances SOSA/SANDRE)              │
│    🧠 Abstraction sémantique unifiée pour ML/BI/Simulation │
└─────────────────────────────────────────────────────────────┘
                            ▲
                            │ Mapping (Phase 2+)
                            │
┌─────────────────────────────────────────────────────────────┐
│                   TRANSFORMATIONS (futures)                 │
│         (Analytics, agrégations, features ML)               │
│    📊 Métriques DCE, Bilans bassins, Tendances qualité     │
│         🔧 Technologies: dbt, SQL, Python                   │
└─────────────────────────────────────────────────────────────┘
                            ▲
                            │ Phase 2 (3-6 mois)
                            │
┌─────────────────────────────────────────────────────────────┐
│              POSTGRESQL - Entrepôt Unique                   │
│              (Données Hub'Eau consolidées)                  │
│    📥 Ingestion directe: Hub'Eau APIs → DLT → PostgreSQL   │
│    ✅ 22 tables, 1 schéma (hubeau), déduplication auto     │
│    🌊 3 modes: FULL / YEAR / INCREMENTAL                   │
└─────────────────────────────────────────────────────────────┘
```

### Phase 1 : Ingestion PostgreSQL (Actuelle - ✅ Implémentée)

- **Objectif** : Consolider toutes les données Hub'Eau dans PostgreSQL
- **Technologies** : DLT (Data Load Tool), Dagster, PostgreSQL 16
- **Fonctionnalités** :
  - ✅ Ingestion directe CSV → PostgreSQL (pas de stockage intermédiaire)
  - ✅ 22 tables dans schéma unique `hubeau`
  - ✅ Déduplication automatique (MERGE/UPSERT sur clés primaires)
  - ✅ 3 modes d'ingestion : FULL (historique complet), YEAR (année spécifique), INCREMENTAL (derniers N jours)
  - ✅ Orchestration Dagster avec jobs par API
  - ✅ Monitoring et sensors d'erreurs
  - ✅ PostGIS activé pour géométries

### Phase 2 : Transformations Analytics (🚧 Prochaine étape - 3-6 mois)

- **Objectif** : Créer des vues/tables analytics pour BI et ML
- **Technologies envisagées** : dbt (data build tool), SQL, Python
- **Transformations prévues** :
  - Vues matérialisées pour agrégations temporelles (moyennes mensuelles, annuelles)
  - Métriques DCE (Directive Cadre sur l'Eau)
  - Features ML (indicateurs qualité, tendances, anomalies)
  - Enrichissement géographique (jointures avec référentiels SANDRE)
  - Calculs dérivés (indices qualité, statistiques descriptives)

**Note** : Cette phase sera lancée quand des besoins analytics concrets émergeront. Pour l'instant, Phase 1 = consolider l'ingestion.

### Phase 3 : Couche Ontologique (🎯 Vision long-terme)

- **Objectif** : Abstraction sémantique unifiée via graphe de connaissances
- **Rôle** : Connecter toutes les dimensions via ontologie SOSA/SANDRE
- **Usage** : Base pour raisonnement sémantique, ML explicable, simulation
- **Technologies** : RDF, SPARQL, GraphDB ou Virtuoso

**Note** : Cette phase permettra le raisonnement sémantique et l'interopérabilité avec d'autres systèmes environnementaux.

---

## 🌐 La Couche d'Abstraction Ontologique

### Pourquoi une Ontologie?

Les APIs Hub'Eau exposent des **modèles de données hétérogènes** :
- 🔤 Noms différents pour concepts similaires (`code_bss` vs `code_station`, `date_prelevement` vs `date_mesure`)
- 📊 Structures variables (stations piézo ≠ stations qualité)
- 🔗 Relations implicites (BSS ↔ masses d'eau ↔ bassins)

Une **ontologie** permet de :
- ✅ **Unifier** : Vocabulaire commun pour concepts métier
- ✅ **Relier** : Graphe de connaissances explicite
- ✅ **Raisonner** : Inférences automatiques sur les données
- ✅ **Interroger** : Queries sémantiques (SPARQL)

### Types de Données à Connecter

#### 1. Données Temporelles
- Séries chronologiques multi-fréquences
- Observations ponctuelles
- Campagnes de mesure

#### 2. Données Relationnelles
- Référentiels stations/ouvrages
- Hiérarchies géographiques (bassins → sous-bassins → stations)
- Relations entre entités (ouvrage → points de prélèvement)

#### 3. Données Géographiques
- Coordonnées GPS (Lambert 93, WGS84)
- Polygones (bassins versants, masses d'eau)
- Réseaux hydrographiques

#### 4. Référentiels Sémantiques
- **Thesaurus SANDRE** :
  - Paramètres physico-chimiques (nomenclature étendue)
  - Unités de mesure standardisées
  - Taxons biologiques (référentiel national)
  - Usages de l'eau (AEP, irrigation, industrie)
- **Ontologies domaine** :
  - Hydrologie, hydrogéologie
  - Hydrobiologie, écologie aquatique

---

## 🔬 SOSA - L'Ontologie pour nos Observations

### Présentation de SOSA

**SOSA** (Sensor, Observation, Sample, and Actuator Ontology) est une ontologie W3C conçue pour représenter les systèmes d'observation.

Elle est :
- ✅ **Standard** : Recommandation W3C officielle
- ✅ **Simple** : Core pattern léger et extensible
- ✅ **Modulaire** : Peut être étendue avec SSN (Semantic Sensor Network)
- ✅ **Interopérable** : Utilisée dans de nombreux domaines (météo, océano, environnement)

### Concepts Clés de SOSA

```turtle
# Exemple simplifié en RDF Turtle

# 1. Platform (Point de collecte physique)
:Station_06044X0009 a sosa:Platform ;
    rdfs:label "Station piézométrique BSS 06044X0009" ;
    geo:lat 43.7012 ;
    geo:long 7.2683 .

# 2. Sensor (Capteur/Instrument)
:Piezometre_06044X0009 a sosa:Sensor ;
    sosa:isHostedBy :Station_06044X0009 ;
    sosa:observes :NiveauNappe .

# 3. ObservableProperty (Propriété mesurée)
:NiveauNappe a sosa:ObservableProperty ;
    rdfs:label "Niveau piézométrique NGF" ;
    qudt:unit unit:M .

# 4. FeatureOfInterest (Entité observée)
:NappeAlluvionsVar a sosa:FeatureOfInterest ;
    rdfs:label "Nappe alluviale du Var" ;
    sandre:codeMasseEau "FRDG106" .

# 5. Observation (Mesure)
:Obs_2024_01_15_12h a sosa:Observation ;
    sosa:madeBySensor :Piezometre_06044X0009 ;
    sosa:hasFeatureOfInterest :NappeAlluvionsVar ;
    sosa:observedProperty :NiveauNappe ;
    sosa:resultTime "2024-01-15T12:00:00Z"^^xsd:dateTime ;
    sosa:hasSimpleResult 12.34 ;
    qudt:unit unit:M .
```

### Mapping Hub'Eau → SOSA

| Concept Hub'Eau | Concept SOSA | Exemple |
|-----------------|--------------|---------|
| **Station piézo (BSS)** | `sosa:Platform` + `sosa:Sensor` | 06044X0009 |
| **Station qualité** | `sosa:Platform` + `sosa:Sensor` | 06044000 |
| **Ouvrage prélèvement** | `sosa:Platform` | PRL000123 |
| **Point de prélèvement** | `sosa:Sensor` | PT000456 |
| **Analyse qualité** | `sosa:Observation` | Mesure nitrates 2024-01-15 |
| **Mesure piézométrique** | `sosa:Observation` | Niveau nappe 12.34m |
| **Paramètre (code_param)** | `sosa:ObservableProperty` | 1340 (Nitrates) |
| **Indice biologique** | `sosa:ObservableProperty` | IBGN, I2M2 |
| **Nappe phréatique** | `sosa:FeatureOfInterest` | Nappe du Var |
| **Cours d'eau** | `sosa:FeatureOfInterest` | La Loire |

### Avantages du Mapping SOSA

1. **Unification** : Même modèle pour toutes les APIs (qualité, piézo, hydro, etc.)
2. **Traçabilité** : Lien explicite sensor → observation → feature of interest
3. **Interopérabilité** : Compatible avec autres ontologies environnementales
4. **Extensibilité** : Ajout facile de nouvelles propriétés métier (qualification, protocole, etc.)

---

## 🔗 Connecter les Dimensions

### Dimension Topographique

**Objectif** : Spatialiser toutes les données

- **Géolocalisation** :
  - Coordonnées GPS des stations (lat/lon)
  - Systèmes de projection (Lambert 93, WGS84)
  - Précision et incertitude

- **Hiérarchies spatiales** :
  ```
  Bassin versant (ex: Loire-Bretagne)
    └─ Sous-bassin (ex: Haute-Loire)
       └─ Masse d'eau (ex: FRDG456)
          └─ Station (ex: 06044000)
  ```

- **Référentiels géographiques** :
  - Bassins DCE (Directive Cadre sur l'Eau)
  - Masses d'eau SANDRE
  - Départements, régions, communes
  - Réseaux hydrographiques BD Carthage

**Ontologie** : GeoSPARQL, SANDRE Référentiels Géographiques

### Dimension Métier

**Objectif** : Enrichir avec la sémantique hydrologique

- **Usages de l'eau** :
  - AEP (Alimentation en Eau Potable)
  - Irrigation agricole
  - Industrie
  - Production d'énergie

- **Protocoles de mesure** :
  - IBGN (Indice Biologique Global Normalisé)
  - I2M2 (Indice Invertébrés Multi-Métriques)
  - Normes analytiques (NF EN, ISO)

- **Qualifications** :
  - Données brutes, validées, qualifiées
  - Niveaux de qualité SANDRE (1=excellent, 5=mauvais)
  - Statuts : provisoire, définitif, corrigé

- **Contextes hydrologiques** :
  - Étiage, crue, conditions normales
  - Périodes hydrologiques (basses eaux, hautes eaux)

**Ontologies** : SANDRE Thesaurus, vocabulaires métier hydrologie

### Dimension Temporelle

**Objectif** : Gérer la complexité temporelle

- **Séries chronologiques multi-fréquences** :
  - Temps réel : Piézométrie (horaire, journalier)
  - Haute fréquence : Hydromérie (quotidien)
  - Régulier : Qualité (mensuel à trimestriel)
  - Saisonnier : Hydrobiologie (printemps, automne)
  - Ponctuel : Prélèvements (annuel)

- **Campagnes de mesure** :
  - ONDE : Campagnes mensuelles d'observation d'étiage
  - RCS : Réseau de Contrôle de Surveillance (mensuel)
  - RCO : Réseau de Contrôle Opérationnel (variable)

- **Historique et révisions** :
  - Données historiques complètes (2000-2025)
  - Corrections retroactives (gérées par lookback_days)
  - Versioning des référentiels

**Ontologie** : OWL-Time (W3C), PROV-O (provenance)

### Dimension Référentielle

**Objectif** : Connecter aux connaissances externes

- **Thesaurus SANDRE** :
  - ~3000 paramètres physico-chimiques
  - ~10000 taxons biologiques
  - ~500 unités de mesure
  - Hiérarchies (ex: Nitrates → Composés azotés → Nutriments)

- **Ontologies domaine** :
  - ENVO (Environment Ontology) : Types d'habitats aquatiques
  - SWEET (Semantic Web for Earth and Environmental Terminology)
  - BCO (Biological Collections Ontology) : Taxons

- **Liens inter-référentiels** :
  - BSS (Banque du Sous-Sol) ↔ Stations hydrométriques
  - Stations qualité ↔ Masses d'eau DCE
  - Ouvrages prélèvement ↔ Points de prélèvement

**Technologies** : SKOS (vocabulaires), OWL (ontologies), RDF (graphe)



## 📚 Références

### Standards et Ontologies

- **SOSA/SSN** : [W3C Semantic Sensor Network Ontology](https://www.w3.org/TR/vocab-ssn/)
- **GeoSPARQL** : [W3C/OGC GeoSPARQL](https://www.ogc.org/standards/geosparql)
- **OWL-Time** : [W3C Time Ontology](https://www.w3.org/TR/owl-time/)
- **PROV-O** : [W3C Provenance Ontology](https://www.w3.org/TR/prov-o/)
- **SKOS** : [W3C Simple Knowledge Organization System](https://www.w3.org/TR/skos-reference/)

### Programme JUNON

- **Site officiel JUNON** : [junon-cvl.fr](https://www.junon-cvl.fr/fr)
- **BRGM** : [Bureau de Recherches Géologiques et Minières](https://www.brgm.fr)
- **Région Centre-Val de Loire** : [Partenaire financeur](https://www.centrevaldeloire.fr)

### Référentiels Français

- **SANDRE** : [Service d'Administration Nationale des Données et Référentiels sur l'Eau](http://www.sandre.eaufrance.fr/)
- **Hub'Eau** : [Portail national d'accès aux données sur l'eau](https://hubeau.eaufrance.fr/)
- **Directive Cadre sur l'Eau (DCE)** : [AELB](https://agence.eau-loire-bretagne.fr/)
- **BD Carthage** : [Référentiel hydrographique IGN](https://geoservices.ign.fr/bdcarthage)

### Ontologies Environnementales

- **ENVO** (Environment Ontology) : [OBO Foundry](http://www.obofoundry.org/ontology/envo.html)
- **SWEET** (Semantic Web for Earth) : [NASA JPL](https://sweetontology.net/)
- **BCO** (Biological Collections) : [OBO Foundry](http://www.obofoundry.org/ontology/bco.html)

### Digital Twin

- **ISO 23247** : Digital Twin Manufacturing Framework
- **Digital Twin Consortium** : [Standards et best practices](https://www.digitaltwinconsortium.org/)
- **Gartner** : [Hype Cycle Digital Twin](https://www.gartner.com/en/documents/3980382)

### Exemples de Projets Similaires

- **Digital Twin Delfland (Pays-Bas)** : Gestion ressources en eau et protection contre les inondations dans la région du Delta
- **Virtual Singapore (2014-2018)** : Jumeau numérique 3D de la ville complète pour planification urbaine
- **Destination Earth - DestinE (EU, 2021-2030)** : Jumeau numérique de la Terre (climat, océans, catastrophes naturelles)
