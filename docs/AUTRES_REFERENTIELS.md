# Référentiels de Données - Guide Complet d'Intégration

> **Objectif** : Identifier et exploiter les référentiels externes pour enrichir les données Hub'Eau  
> **Date** : 2025-10-10  
> **Statut** : Guide de référence

## 📑 Table des Matières

1. [Vue d'Ensemble](#-vue-densemble)
2. [Référentiels Déjà Intégrés](#-référentiels-déjà-intégrés)
3. [Référentiels à Intégrer en Priorité](#-référentiels-à-intégrer-en-priorité)
4. [Référentiels Complémentaires](#-référentiels-complémentaires)
5. [Plan d'Action](#-plan-daction)

---

## 📊 Vue d'Ensemble

### Qu'est-ce qu'un Référentiel ?

Un **référentiel** est une base de données normative qui :
- Fournit des **codes standardisés** (ex: code commune INSEE)
- Assure la **cohérence** entre différentes sources de données
- Permet l'**enrichissement** avec des métadonnées contextuelles
- Garantit l'**interopérabilité** entre systèmes

### Les 3 Niveaux d'Intégration

| Niveau | Description | Exemple |
|--------|-------------|---------|
| **🟢 Niveau 1 - Codes stockés** | On stocke les codes mais sans exploiter le référentiel | `code_commune_insee` stocké mais pas de jointure |
| **🟡 Niveau 2 - Enrichissement partiel** | On joint avec certains attributs du référentiel | Jointure avec nom de commune |
| **🔵 Niveau 3 - Exploitation complète** | On exploite toute la richesse du référentiel | Population, évolution, statistiques complètes |

### Référentiels dans Hub'Eau - État des Lieux

| Type | Nombre | Déjà utilisés | À intégrer |
|------|--------|---------------|------------|
| **Géographiques** | 5 | 3 | 2 |
| **Eau & Hydrologie** | 6 | 3 | 3 |
| **Biodiversité** | 4 | 1 | 3 |
| **Réglementaires** | 3 | 0 | 3 |
| **Européens** | 3 | 1 | 2 |
| **TOTAL** | **21** | **8** | **13** |

---

## ✅ Référentiels Déjà Intégrés

### 1. SANDRE - Service d'Administration Nationale des Données et Référentiels sur l'Eau

**📍 Site** : [sandre.eaufrance.fr](https://www.sandre.eaufrance.fr)  
**🏛️ Gestionnaire** : Office Français de la Biodiversité (OFB)  
**🟢 Niveau actuel** : Niveau 1 (codes stockés uniquement)

#### Présence dans nos Endpoints

| API | Champs SANDRE | Exemples de Codes |
|-----|---------------|-------------------|
| **Toutes** | `code_qualification` | `1` = Bonne, `2` = Incertaine, `4` = Mauvaise |
| **Qualité Cours d'Eau** | `code_parametre` | `1340` = Nitrates, `1335` = Ammonium |
| **Qualité Cours d'Eau** | `code_support` | `3` = Eau, `6` = Sédiment, `17` = Biote |
| **Qualité Cours d'Eau** | `code_unite` | `27` = mg/L, `133` = µg/L |
| **Qualité Cours d'Eau** | `code_fraction` | `11` = Brute, `12` = Dissoute |
| **Hydrobiologie** | `code_appel_taxon` | Codes taxons biologiques |
| **Hydrométrie** | `code_cours_eau` | `K---0000` = Rhône, `V---0000` = Loire |
| **Toutes** | URI SANDRE | Format `https://id.eaufrance.fr/sandre/...` |

#### Opportunités d'Enrichissement

**🔵 Niveau 3 possible** :

```sql
-- Table référence SANDRE Paramètres
CREATE TABLE ref_sandre_parametres (
  code_parametre VARCHAR PRIMARY KEY,
  libelle VARCHAR,
  groupe VARCHAR,              -- "Nutriments", "Métaux", "Pesticides"
  famille_chimique VARCHAR,
  cas_number VARCHAR,           -- N° CAS chimique
  formule_chimique VARCHAR,
  unite_defaut VARCHAR,
  seuil_detection_type FLOAT,
  reglementation TEXT[]         -- Arrêtés applicables
);

-- Enrichissement analyses
SELECT 
  a.*,
  p.groupe,
  p.famille_chimique,
  p.cas_number
FROM quality_rivers_analyses a
LEFT JOIN ref_sandre_parametres p ON a.code_parametre = p.code_parametre;
```

**Gain attendu** : 
- ✅ Groupement automatique par famille chimique
- ✅ Recherche par n° CAS
- ✅ Validation codes paramètres
- ✅ Alertes si paramètres non conformes

---

### 2. BDLISA - Référentiel Hydrogéologique National

**📍 Site** : [bdlisa.eaufrance.fr](https://bdlisa.eaufrance.fr)  
**🏛️ Gestionnaire** : BRGM  
**🟢 Niveau actuel** : Niveau 1 (codes stockés en arrays)

#### Présence dans nos Endpoints

| API | Champs BDLISA | Format | Usage |
|-----|---------------|--------|-------|
| **Piézométrie** `/stations` | `codes_bdlisa` | `array[string]` | Formations traversées |
| **Piézométrie** `/stations` | `urns_bdlisa` | `array[string]` | URIs formations |
| **Qualité Nappes** `/stations` | `codes_entite_hg_bdlisa` | `array[string]` | Aquifères surveillés |
| **Qualité Nappes** `/stations` | `noms_entite_hg_bdlisa` | `array[string]` | Noms formations |
| **Prélèvements** `/ouvrages` | `code_bdlisa` | `string` | Aquifère exploité |
| **Prélèvements** `/ouvrages` | `uri_bdlisa` | `string` | URI formation |

#### Opportunités d'Enrichissement

**🔵 Niveau 3 possible** :

```sql
-- Table référence BDLISA
CREATE TABLE ref_bdlisa_formations (
  code_bdlisa VARCHAR PRIMARY KEY,
  nom_formation VARCHAR,
  lithologie VARCHAR,              -- "Calcaire", "Sable", "Grès", "Argile"
  productivite VARCHAR,            -- "Très productive", "Productive", "Peu productive"
  type_porosite VARCHAR,           -- "Intergranulaire", "Fissures/Karst"
  permeabilite_type VARCHAR,       -- "Forte", "Moyenne", "Faible"
  epaisseur_moyenne FLOAT,         -- Mètres
  profondeur_toit_moyenne FLOAT,   -- Mètres
  aire_affleurement_km2 FLOAT,
  bassin_sedimentaire VARCHAR      -- "Bassin Parisien", "Bassin Aquitain"
);

-- Analyse enrichie piézomètres
WITH piezo_formations AS (
  SELECT 
    code_bss,
    profondeur_investigation,
    unnest(codes_bdlisa) as code_bdlisa
  FROM piezometry_stations
)
SELECT 
  p.code_bss,
  p.profondeur_investigation,
  b.nom_formation,
  b.lithologie,
  b.productivite,
  b.type_porosite
FROM piezo_formations p
LEFT JOIN ref_bdlisa_formations b ON p.code_bdlisa = b.code_bdlisa;
```

**Gain attendu** :
- ✅ Contexte géologique complet
- ✅ Analyse productivité aquifères
- ✅ Corrélation lithologie ↔ qualité eau
- ✅ Identification zones vulnérables

---

### 3. BSS - Banque du Sous-Sol

**📍 Site** : [bss.brgm.fr](https://bss.brgm.fr)  
**🏛️ Gestionnaire** : BRGM  
**🟢 Niveau actuel** : Niveau 1 (code BSS uniquement)

#### Présence dans nos Endpoints

| API | Champs BSS | Format | Usage |
|-----|-----------|--------|-------|
| **Piézométrie** `/stations` | `code_bss` | `string` | Identifiant piézomètre |
| **Piézométrie** `/stations` | `bss_id` | `string` | ID alternatif |
| **Piézométrie** `/stations` | `urn_bss` | `string` | URN SANDRE |
| **Qualité Nappes** `/stations` | `code_bss`, `bss_id` | `string` | Identifiant point |
| **Prélèvements** `/points` | `code_bss_point_eau` | `string` | Si forage |

#### Opportunités d'Enrichissement

**🔵 Niveau 3 possible** :

```sql
-- Table BSS enrichie
CREATE TABLE ref_bss_detaille (
  code_bss VARCHAR PRIMARY KEY,
  nom_usuel VARCHAR,
  profondeur_totale FLOAT,
  log_geologique JSONB,        -- Coupe litho détaillée
  equipements JSONB,            -- Crépines, tubage, cimentation
  caracteristiques_aquifere JSONB,  -- Transmissivité, débit, etc.
  usage_principal VARCHAR,      -- "Surveillance", "AEP", "Irrigation"
  date_realisation DATE,
  entreprise_forage VARCHAR,
  methode_forage VARCHAR,
  historique_mesures JSONB,     -- Synthèse mesures historiques
  documents_techniques TEXT[]   -- Liens vers rapports
);

-- Enrichissement piézométrie
SELECT 
  p.*,
  b.profondeur_totale,
  b.log_geologique,
  b.equipements,
  b.usage_principal
FROM piezometry_stations p
LEFT JOIN ref_bss_detaille b ON p.code_bss = b.code_bss;
```

**Gain attendu** :
- ✅ Logs géologiques complets (coupes litho)
- ✅ Caractéristiques techniques forages
- ✅ Historique d'exploitation
- ✅ Documents associés

---

### 4. COG - Code Officiel Géographique (INSEE)

**📍 Site** : [insee.fr](https://www.insee.fr/fr/information/2560452)  
**🏛️ Gestionnaire** : INSEE  
**🟢 Niveau actuel** : Niveau 1 (codes stockés uniquement)

#### Présence dans nos Endpoints

| API | Champs COG | Utilisation |
|-----|-----------|-------------|
| **Toutes** | `code_commune_insee` | Code commune (5 caractères) |
| **Toutes** | `code_departement` | Code département (2-3 car.) |
| **Toutes** | `code_region` | Code région |
| **Qualité** | `code_bassin` | Code bassin hydrographique |

**⚠️ Problème actuel** : Évolution des communes (fusions) non suivie !

#### Opportunités d'Enrichissement

**🔵 Niveau 3 possible** :

```sql
-- Table COG complète
CREATE TABLE ref_cog_communes (
  code_commune_insee VARCHAR PRIMARY KEY,
  nom_commune VARCHAR,
  code_departement VARCHAR,
  nom_departement VARCHAR,
  code_region VARCHAR,
  nom_region VARCHAR,
  population INT,
  superficie_km2 FLOAT,
  densite_hab_km2 FLOAT,
  date_creation DATE,
  date_suppression DATE,        -- Pour communes fusionnées
  commune_nouvelle VARCHAR,     -- Code commune résultante si fusion
  arrondissement VARCHAR,
  canton VARCHAR,
  epci VARCHAR,                 -- Intercommunalité
  type_commune VARCHAR          -- "Rurale", "Urbaine"
);

-- Suivi évolutions communales
CREATE TABLE ref_cog_historique (
  code_ancien VARCHAR,
  code_nouveau VARCHAR,
  date_modification DATE,
  type_modification VARCHAR,    -- "Fusion", "Scission", "Changement code"
  PRIMARY KEY (code_ancien, date_modification)
);

-- Enrichissement avec gestion fusions
SELECT 
  s.code_station,
  s.code_commune_insee,
  COALESCE(c_new.nom_commune, c_old.nom_commune) as nom_commune_actuel,
  c_old.population,
  c_old.superficie_km2,
  c_old.type_commune
FROM hydrometry_stations s
LEFT JOIN ref_cog_communes c_old ON s.code_commune_insee = c_old.code_commune_insee
LEFT JOIN ref_cog_historique h ON s.code_commune_insee = h.code_ancien
LEFT JOIN ref_cog_communes c_new ON h.code_nouveau = c_new.code_commune_insee;
```

**Gain attendu** :
- ✅ Population, densité par station
- ✅ Gestion fusions communales
- ✅ Typologie rurale/urbaine
- ✅ Analyse par intercommunalité

---

### 5. Masses d'Eau DCE - Directive Cadre sur l'Eau

**📍 Site** : [rapportage.eaufrance.fr](https://rapportage.eaufrance.fr)  
**🏛️ Gestionnaire** : OFB / Agences de l'Eau  
**🟢 Niveau actuel** : Niveau 1 (codes stockés uniquement)

#### Présence dans nos Endpoints

| API | Champs Masses d'Eau | Usage |
|-----|---------------------|-------|
| **Qualité Cours d'Eau** | `code_masse_deau`, `code_eu_masse_deau` | Masse d'eau de surface |
| **Qualité Nappes** | `codes_masse_eau_edl`, `codes_masse_eau_rap` | Masses d'eau souterraine |
| **Hydrobiologie** | `code_masse_eau` | Masse d'eau surveillée |
| **Température** | `code_masse_eau` | Contexte DCE |

#### Opportunités d'Enrichissement

**🔵 Niveau 3 possible** :

```sql
-- Table Masses d'Eau DCE
CREATE TABLE ref_masses_eau_dce (
  code_masse_eau VARCHAR PRIMARY KEY,
  code_eu VARCHAR,              -- Code européen
  nom_masse_eau VARCHAR,
  type_masse_eau VARCHAR,       -- "Cours d'eau", "Plan d'eau", "Côtière", "Souterraine"
  categorie VARCHAR,            -- "Naturelle", "Fortement modifiée", "Artificielle"
  
  -- États
  statut_ecologique VARCHAR,    -- "Très bon", "Bon", "Moyen", "Médiocre", "Mauvais"
  statut_chimique VARCHAR,      -- "Bon", "Pas bon"
  etat_quantitatif VARCHAR,     -- Pour nappes
  
  -- Objectifs
  objectif_bon_etat_ecologique INT,    -- Année
  objectif_bon_etat_chimique INT,      -- Année
  report_echeance VARCHAR,             -- "Oui/Non"
  motif_report VARCHAR,
  
  -- Pressions
  pressions_principales TEXT[],        -- ["Agriculture", "Assainissement", "Industrie"]
  pressions_significatives JSONB,      -- Détail par type
  
  -- Programme de mesures
  programme_mesures TEXT[],
  cout_programme_mesures_eur FLOAT,
  
  -- Caractéristiques
  surface_km2 FLOAT,
  bassin_dce VARCHAR,
  sous_bassin VARCHAR,
  agence_eau VARCHAR
);

-- Analyse conformité DCE
SELECT 
  me.nom_masse_eau,
  me.statut_ecologique,
  me.objectif_bon_etat_ecologique,
  me.pressions_principales,
  COUNT(DISTINCT a.code_station) as nb_stations,
  COUNT(a.code_analyse) as nb_analyses,
  AVG(a.resultat) FILTER (WHERE a.code_parametre = '1340') as nitrates_moyen_mg_l,
  AVG(a.resultat) FILTER (WHERE a.code_parametre = '1433') as phosphates_moyen_mg_l
FROM quality_rivers_analyses a
JOIN quality_rivers_stations s ON a.code_station = s.code_station
JOIN ref_masses_eau_dce me ON s.code_masse_deau = me.code_masse_eau
GROUP BY me.code_masse_eau, me.nom_masse_eau, me.statut_ecologique, 
         me.objectif_bon_etat_ecologique, me.pressions_principales;
```

**Gain attendu** :
- ✅ Contexte réglementaire DCE complet
- ✅ Analyse conformité objectifs
- ✅ Identification zones à enjeux
- ✅ Lien pressions ↔ qualité mesurée

---

### 6. Référentiel Hydrographique (BD CARTHAGE)

**📍 Site** : [geo.data.gouv.fr](https://geo.data.gouv.fr)  
**🏛️ Gestionnaire** : IGN / OFB  
**🟢 Niveau actuel** : Niveau 1 (via codes cours d'eau SANDRE)

#### Présence dans nos Endpoints

| API | Lien avec BD CARTHAGE | Via |
|-----|----------------------|-----|
| **Hydrométrie** | `code_cours_eau` | Code SANDRE du cours d'eau |
| **Qualité Cours d'Eau** | `code_cours_eau`, `code_troncon_hydro` | Tronçon hydrographique |
| **Température** | `code_cours_eau` | Cours d'eau |
| **Écoulement** | `code_cours_eau` | Cours d'eau |

#### Opportunités d'Enrichissement

**🔵 Niveau 3 possible** :

```sql
-- Table BD CARTHAGE
CREATE TABLE ref_carthage_cours_eau (
  code_cours_eau VARCHAR PRIMARY KEY,
  nom_cours_eau VARCHAR,
  longueur_totale_km FLOAT,
  bassin_versant_amont_km2 FLOAT,
  rang_strahler INT,                    -- Ordre du cours d'eau
  geometrie GEOMETRY(LINESTRING, 2154)  -- Tracé du cours d'eau
);

CREATE TABLE ref_carthage_troncons (
  code_troncon VARCHAR PRIMARY KEY,
  code_cours_eau VARCHAR,
  troncon_amont VARCHAR[],              -- Tronçons parents
  troncon_aval VARCHAR,                 -- Tronçon fils
  pk_debut FLOAT,                       -- Point kilométrique début
  pk_fin FLOAT,                         -- Point kilométrique fin
  pente_moyenne FLOAT,
  largeur_moyenne FLOAT,
  geometrie GEOMETRY(LINESTRING, 2154)
);

-- Calculs topologiques
-- Exemple : Stations amont d'un point donné
WITH RECURSIVE amont AS (
  -- Point de départ
  SELECT code_troncon, code_cours_eau
  FROM ref_carthage_troncons
  WHERE code_troncon = 'K4470010'  -- Tronçon d'intérêt
  
  UNION
  
  -- Récursion vers l'amont
  SELECT t.code_troncon, t.code_cours_eau
  FROM ref_carthage_troncons t
  JOIN amont a ON t.code_troncon = ANY(a.troncon_amont)
)
SELECT DISTINCT s.*
FROM hydrometry_stations s
JOIN amont a ON s.code_troncon_hydro = a.code_troncon;
```

**Gain attendu** :
- ✅ Topologie réseau hydrographique
- ✅ Calculs amont/aval
- ✅ Propagation de pollutions
- ✅ Bassins versants contributifs

---

### 7. INSPIRE - Infrastructure Spatiale Européenne

**📍 Site** : [inspire.ec.europa.eu](https://inspire.ec.europa.eu)  
**🏛️ Gestionnaire** : Commission Européenne  
**🟢 Niveau actuel** : Niveau 1 (conformité partielle via géométries)

#### Présence dans nos Endpoints

| API | Conformité INSPIRE | Thème INSPIRE |
|-----|-------------------|---------------|
| **Toutes** | `geometry` (GeoJSON) | Installations de suivi environnemental |
| **Hydrométrie** | Stations hydro | Hydrographie |
| **Qualité** | Stations de surveillance | Zones de gestion |

#### Opportunités d'Enrichissement

**🔵 Niveau 3 possible** :

```sql
-- Métadonnées INSPIRE conformes
CREATE TABLE ref_inspire_metadata (
  station_id VARCHAR PRIMARY KEY,
  inspire_id UUID,                      -- Identifiant unique INSPIRE
  theme_inspire VARCHAR[],              -- Thèmes INSPIRE applicables
  mot_cles TEXT[],
  resume_ressource TEXT,
  date_creation TIMESTAMP,
  date_revision TIMESTAMP,
  organisme_responsable VARCHAR,
  contraintes_acces TEXT,
  contraintes_utilisation TEXT,
  reference_systeme_coordonnees VARCHAR,
  emprise_geographique GEOMETRY,
  conformite_specifications JSONB       -- Spécifications respectées
);

-- Export conforme INSPIRE
SELECT 
  s.code_station as inspire_id,
  s.libelle_station as nom,
  'Installations de suivi environnemental' as theme_inspire,
  s.geometry,
  'EPSG:4326' as srs,
  json_build_object(
    'organisme', 'BRGM',
    'qualite', 'Validé',
    'date_maj', s.date_maj
  ) as metadata
FROM hydrometry_stations s;
```

**Gain attendu** :
- ✅ Conformité européenne
- ✅ Interopérabilité EU
- ✅ Échanges données standardisés

---

### 8. Autres Référentiels Déjà Présents

| Référentiel | Où | Champ | Niveau |
|-------------|-----|-------|--------|
| **Codes Projections** | Toutes APIs | `code_projection`, `code_epsg` | 🟢 Niveau 1 |
| **Codes Statuts** | Chroniques | `code_statut`, `mnemo_statut` | 🟢 Niveau 1 |
| **Codes Réseaux** | Qualité, Hydrobio | `code_reseau`, `nom_reseau` | 🟢 Niveau 1 |
| **Codes Finalité** | Qualité | `code_finalite` | 🟢 Niveau 1 |

---

## 🎯 Référentiels à Intégrer en Priorité

### 1. NQE - Normes de Qualité Environnementale ⭐⭐⭐

**📍 Source** : Arrêtés ministériels, Directive DCE  
**🏛️ Gestionnaire** : Ministère Écologie / UE  
**❌ Statut actuel** : NON INTÉGRÉ

#### Pourquoi c'est Critique

Les **NQE** (Normes de Qualité Environnementale) sont les **seuils réglementaires** à ne pas dépasser pour chaque substance dans l'eau. Sans elles, **impossible de savoir si une mesure est conforme ou non** !

#### Contenu du Référentiel

```sql
CREATE TABLE ref_nqe_seuils (
  code_parametre VARCHAR,
  code_support VARCHAR,           -- Eau, Sédiment, Biote
  type_milieu VARCHAR,            -- "Cours d'eau", "Plan d'eau", "Souterrain"
  
  -- Seuils DCE
  nqe_ma FLOAT,                   -- Moyenne Annuelle (µg/L ou mg/L)
  nqe_cma FLOAT,                  -- Concentration Maximale Admissible
  
  -- Seuils complémentaires
  seuil_bon_etat FLOAT,
  seuil_tres_bon_etat FLOAT,
  
  unite VARCHAR,
  date_application DATE,
  texte_reference VARCHAR,        -- "Arrêté du 27/07/2015"
  
  PRIMARY KEY (code_parametre, code_support, type_milieu)
);
```

#### Application - Détection Automatique des Dépassements

```sql
-- Vue dépassements NQE
CREATE VIEW v_depassements_nqe AS
SELECT 
  a.code_station,
  s.libelle_station,
  a.date_prelevement,
  a.code_parametre,
  a.libelle_parametre,
  a.resultat,
  a.symbole_unite,
  n.nqe_ma,
  n.nqe_cma,
  CASE 
    WHEN a.resultat > n.nqe_cma THEN '🔴 ALERTE - Dépassement CMA'
    WHEN a.resultat > n.nqe_ma THEN '🟠 Dépassement MA'
    ELSE '🟢 Conforme'
  END as statut_conformite,
  ROUND((a.resultat / n.nqe_ma) * 100, 1) as pct_nqe
FROM quality_rivers_analyses a
JOIN quality_rivers_stations s ON a.code_station = s.code_station
JOIN ref_nqe_seuils n 
  ON a.code_parametre = n.code_parametre 
  AND a.code_support = n.code_support
WHERE a.code_qualification = '1'  -- Données fiables uniquement
  AND a.resultat > n.nqe_ma;

-- Dashboard dépassements
SELECT 
  libelle_parametre,
  COUNT(*) as nb_depassements,
  COUNT(*) FILTER (WHERE statut_conformite LIKE '%ALERTE%') as nb_alertes_cma,
  AVG(pct_nqe) as depassement_moyen_pct,
  MAX(pct_nqe) as depassement_max_pct
FROM v_depassements_nqe
GROUP BY libelle_parametre
ORDER BY nb_depassements DESC;
```

**Gain attendu** :
- ✅ **Alertes automatiques** si dépassement NQE
- ✅ **Conformité réglementaire** DCE calculée automatiquement
- ✅ **Priorisation** des zones à enjeux
- ✅ **Dashboards** de surveillance en temps réel

**🔧 Action** : Constituer table NQE depuis arrêtés ministériels

---

### 2. TAXREF - Référentiel Taxonomique National ⭐⭐

**📍 Site** : [taxref.mnhn.fr](https://taxref.mnhn.fr)  
**🏛️ Gestionnaire** : Muséum National d'Histoire Naturelle (MNHN)  
**❌ Statut actuel** : NON INTÉGRÉ (codes taxons présents mais non validés)

#### Pourquoi c'est Important

L'API Hydrobiologie retourne des **codes taxons** (`code_appel_taxon`) mais sans garantie de validité. TAXREF permettrait de :
- **Valider** les codes taxons
- **Enrichir** avec noms vernaculaires, statuts de protection
- **Analyser** la hiérarchie taxonomique

#### Contenu du Référentiel

```sql
CREATE TABLE ref_taxref (
  cd_nom INT PRIMARY KEY,                    -- Code nomenclatural TAXREF
  cd_ref INT,                                -- Code de référence (taxon valide)
  nom_scientifique VARCHAR,
  nom_vernaculaire VARCHAR,
  rang VARCHAR,                              -- "Espèce", "Genre", "Famille"
  
  -- Hiérarchie taxonomique
  regne VARCHAR,                             -- "Animalia", "Plantae"
  phylum VARCHAR,
  classe VARCHAR,
  ordre VARCHAR,
  famille VARCHAR,
  
  -- Statuts
  statut_protection VARCHAR[],               -- "Protection nationale", "Directive Habitats"
  statut_menace VARCHAR,                     -- "LC", "NT", "VU", "EN", "CR" (Liste Rouge)
  statut_invasif BOOLEAN,
  
  -- Répartition
  presence_france VARCHAR,                   -- "Présent", "Introduit", "Occasionnel"
  habitat_principal VARCHAR                  -- "Eau douce", "Marin", "Terrestre"
);
```

#### Application - Validation et Enrichissement Taxons

```sql
-- Enrichissement taxons hydrobiologie
SELECT 
  t.code_station_hydrobio,
  t.date_prelevement,
  t.code_appel_taxon,
  t.libelle_appel_taxon,
  t.resultat_taxon,
  
  -- Enrichissement TAXREF
  r.nom_scientifique,
  r.nom_vernaculaire,
  r.rang,
  r.classe,
  r.ordre,
  r.famille,
  r.statut_protection,
  r.statut_menace,
  r.statut_invasif,
  
  CASE 
    WHEN r.cd_nom IS NULL THEN '⚠️ Taxon non référencé TAXREF'
    WHEN r.statut_invasif THEN '🔴 Espèce invasive'
    WHEN r.statut_protection IS NOT NULL THEN '🟢 Espèce protégée'
    ELSE '⚪ Espèce commune'
  END as alerte_ecologique
  
FROM hydrobio_taxons t
LEFT JOIN ref_taxref r ON t.code_appel_taxon::INT = r.cd_nom;

-- Analyse biodiversité par station
SELECT 
  code_station_hydrobio,
  COUNT(DISTINCT r.famille) as nb_familles,
  COUNT(DISTINCT r.cd_nom) as nb_especes,
  COUNT(*) FILTER (WHERE r.statut_protection IS NOT NULL) as nb_especes_protegees,
  COUNT(*) FILTER (WHERE r.statut_invasif) as nb_especes_invasives,
  COUNT(*) FILTER (WHERE r.statut_menace IN ('VU', 'EN', 'CR')) as nb_especes_menacees
FROM hydrobio_taxons t
JOIN ref_taxref r ON t.code_appel_taxon::INT = r.cd_nom
GROUP BY code_station_hydrobio;
```

**Gain attendu** :
- ✅ **Validation** codes taxons
- ✅ **Statuts de protection** automatiques
- ✅ **Détection espèces invasives**
- ✅ **Analyse biodiversité** enrichie

**🔧 Action** : Télécharger TAXREF et intégrer via API ou CSV

---

### 3. Occupation des Sols - Corine Land Cover ⭐⭐

**📍 Site** : [land.copernicus.eu](https://land.copernicus.eu)  
**🏛️ Gestionnaire** : Programme Copernicus (UE)  
**❌ Statut actuel** : NON INTÉGRÉ

#### Pourquoi c'est Utile

L'**occupation des sols** autour d'une station influence directement la qualité de l'eau :
- **Zones agricoles** → Nitrates, pesticides
- **Zones urbaines** → Assainissement, pollutions diffuses
- **Zones forestières** → Qualité préservée

#### Contenu du Référentiel

```sql
-- Table Corine Land Cover (raster → vecteur)
CREATE TABLE ref_corine_land_cover (
  id SERIAL PRIMARY KEY,
  code_clc VARCHAR,                    -- Code CLC (ex: "211" = Terres arables)
  niveau_1 VARCHAR,                    -- "Territoires artificialisés"
  niveau_2 VARCHAR,                    -- "Zones urbanisées"
  niveau_3 VARCHAR,                    -- "Tissu urbain continu"
  geometrie GEOMETRY(POLYGON, 2154),
  surface_ha FLOAT,
  annee INT                            -- 2006, 2012, 2018, etc.
);

-- Buffer zone autour stations
CREATE TABLE analyse_occupation_sols AS
SELECT 
  s.code_station,
  s.libelle_station,
  ST_Buffer(s.geometry::geography, 1000)::geometry as buffer_1km,  -- 1 km autour
  
  -- Calcul surfaces par type
  SUM(c.surface_ha) FILTER (WHERE c.niveau_1 = 'Territoires agricoles') as surface_agricole_ha,
  SUM(c.surface_ha) FILTER (WHERE c.niveau_1 = 'Territoires artificialisés') as surface_urbaine_ha,
  SUM(c.surface_ha) FILTER (WHERE c.niveau_1 = 'Forêts et milieux semi-naturels') as surface_naturelle_ha,
  SUM(c.surface_ha) FILTER (WHERE c.code_clc LIKE '2%') as surface_cultures_ha,
  
  -- Pourcentages
  ROUND(100.0 * SUM(c.surface_ha) FILTER (WHERE c.niveau_1 = 'Territoires agricoles') / 
        SUM(c.surface_ha), 1) as pct_agricole

FROM quality_rivers_stations s
JOIN ref_corine_land_cover c 
  ON ST_Intersects(s.geometry, c.geometrie)
  AND ST_DWithin(s.geometry::geography, c.geometrie::geography, 1000)
GROUP BY s.code_station, s.libelle_station, s.geometry;
```

#### Application - Corrélation Occupation Sols ↔ Qualité

```sql
-- Corrélation nitrates vs agriculture
SELECT 
  CASE 
    WHEN o.pct_agricole < 20 THEN '0-20% agricole'
    WHEN o.pct_agricole < 50 THEN '20-50% agricole'
    WHEN o.pct_agricole < 80 THEN '50-80% agricole'
    ELSE '80-100% agricole'
  END as classe_agriculture,
  
  COUNT(DISTINCT a.code_station) as nb_stations,
  ROUND(AVG(a.resultat), 2) as nitrates_moyen_mg_l,
  ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY a.resultat), 2) as nitrates_p90,
  COUNT(*) FILTER (WHERE a.resultat > 50) as nb_depassements_50mg
  
FROM quality_rivers_analyses a
JOIN analyse_occupation_sols o ON a.code_station = o.code_station
WHERE a.code_parametre = '1340'  -- Nitrates
  AND a.code_qualification = '1'
GROUP BY classe_agriculture
ORDER BY classe_agriculture;
```

**Gain attendu** :
- ✅ **Identification sources** de pollution
- ✅ **Corrélation** usage sols ↔ paramètres
- ✅ **Priorisation** zones agricoles à risque
- ✅ **Évolution temporelle** (CLC multi-dates)

**🔧 Action** : Télécharger CLC et croiser avec buffer autour stations

---

### 4. RPG - Registre Parcellaire Graphique (Agriculture) ⭐⭐

**📍 Site** : [data.gouv.fr/rpg](https://www.data.gouv.fr/fr/datasets/registre-parcellaire-graphique-rpg/)  
**🏛️ Gestionnaire** : ASP (Agence de Services et de Paiement)  
**❌ Statut actuel** : NON INTÉGRÉ

#### Pourquoi c'est Précis

Plus fin que Corine Land Cover (10m vs 100m), le **RPG** donne le **type de culture exact** par parcelle agricole. Idéal pour :
- Pesticides spécifiques par culture
- Quantification pression agricole précise
- Analyse à l'échelle de la parcelle

#### Contenu du Référentiel

```sql
CREATE TABLE ref_rpg_parcelles (
  id_parcelle VARCHAR PRIMARY KEY,
  code_culture VARCHAR,              -- "BTH" = Blé tendre hiver
  libelle_culture VARCHAR,           -- "Blé tendre d'hiver"
  code_groupe VARCHAR,               -- "CER" = Céréales
  libelle_groupe VARCHAR,            -- "Céréales"
  surface_ha FLOAT,
  geometrie GEOMETRY(POLYGON, 2154),
  campagne_annee INT                 -- 2023, 2024, etc.
);

-- Cultures autour stations
SELECT 
  s.code_station,
  r.libelle_culture,
  r.libelle_groupe,
  COUNT(*) as nb_parcelles,
  SUM(r.surface_ha) as surface_totale_ha,
  r.campagne_annee
FROM quality_rivers_stations s
JOIN ref_rpg_parcelles r 
  ON ST_DWithin(s.geometry::geography, r.geometrie::geography, 500)  -- 500m
WHERE r.campagne_annee = 2024
GROUP BY s.code_station, r.libelle_culture, r.libelle_groupe, r.campagne_annee
ORDER BY surface_totale_ha DESC;
```

#### Application - Lien Pesticides ↔ Cultures

```sql
-- Pesticides attendus selon cultures présentes
WITH cultures_voisines AS (
  SELECT 
    s.code_station,
    r.code_groupe,
    SUM(r.surface_ha) as surface_ha
  FROM quality_rivers_stations s
  JOIN ref_rpg_parcelles r ON ST_DWithin(s.geometry::geography, r.geometrie::geography, 1000)
  WHERE r.campagne_annee = 2024
  GROUP BY s.code_station, r.code_groupe
),
pesticides_attendus AS (
  SELECT 
    code_station,
    CASE code_groupe
      WHEN 'CER' THEN ARRAY['Glyphosate', 'Prosulfocarbe', 'Chlortoluron']
      WHEN 'VIT' THEN ARRAY['Glyphosate', 'Folpel', 'Cuivre']
      WHEN 'MAI' THEN ARRAY['S-métolachlore', 'Atrazine déséthyl']
      ELSE ARRAY[]::VARCHAR[]
    END as pesticides_probables
  FROM cultures_voisines
  WHERE surface_ha > 10  -- Cultures significatives
)
SELECT 
  a.code_station,
  a.libelle_parametre,
  a.resultat,
  p.pesticides_probables,
  CASE 
    WHEN a.libelle_parametre = ANY(p.pesticides_probables) 
    THEN '✅ Pesticide cohérent avec cultures'
    ELSE '❓ Pesticide inattendu'
  END as coherence
FROM quality_rivers_analyses a
JOIN pesticides_attendus p ON a.code_station = p.code_station
WHERE a.code_groupe_parametre = 'Pesticides';
```

**Gain attendu** :
- ✅ **Anticipation** pesticides selon cultures
- ✅ **Cohérence** mesures ↔ pratiques agricoles
- ✅ **Ciblage** zones à surveiller
- ✅ **Évolution** pratiques agricoles

**🔧 Action** : Télécharger RPG annuel et croiser avec stations

---

### 5. Zones Protégées - INPN (ZNIEFF, Natura 2000, PNR/PNN) ⭐

**📍 Site** : [inpn.mnhn.fr](https://inpn.mnhn.fr)  
**🏛️ Gestionnaire** : MNHN / OFB  
**❌ Statut actuel** : NON INTÉGRÉ

#### Pourquoi c'est Stratégique

Savoir si une station est en **zone protégée** change tout :
- **Contraintes réglementaires** renforcées
- **Sensibilité écologique** élevée
- **Exigences qualité** accrues

#### Contenu du Référentiel

```sql
-- Zones ZNIEFF
CREATE TABLE ref_znieff (
  id_znieff VARCHAR PRIMARY KEY,
  type_znieff VARCHAR,           -- "ZNIEFF 1" (remarquable) ou "ZNIEFF 2" (cohérence écologique)
  nom_znieff VARCHAR,
  surface_ha FLOAT,
  especes_determinantes TEXT[],
  habitats_determinants TEXT[],
  geometrie GEOMETRY(MULTIPOLYGON, 2154)
);

-- Zones Natura 2000
CREATE TABLE ref_natura2000 (
  code_site VARCHAR PRIMARY KEY,
  type_site VARCHAR,             -- "ZSC" (Directive Habitats) ou "ZPS" (Directive Oiseaux)
  nom_site VARCHAR,
  surface_ha FLOAT,
  habitats_ic TEXT[],            -- Habitats d'intérêt communautaire
  especes_ic TEXT[],             -- Espèces d'intérêt communautaire
  objectifs_conservation TEXT[],
  geometrie GEOMETRY(MULTIPOLYGON, 2154)
);

-- Parcs Naturels
CREATE TABLE ref_parcs_naturels (
  code_parc VARCHAR PRIMARY KEY,
  type_parc VARCHAR,             -- "PNR" (Régional) ou "PNN" (National)
  nom_parc VARCHAR,
  date_creation DATE,
  charte_parc TEXT,
  geometrie GEOMETRY(MULTIPOLYGON, 2154)
);
```

#### Application - Contextualisation Écologique

```sql
-- Stations en zones protégées
CREATE VIEW v_stations_zones_protegees AS
SELECT 
  s.code_station,
  s.libelle_station,
  s.geometry,
  
  -- ZNIEFF
  STRING_AGG(DISTINCT z.nom_znieff, ', ') as znieff,
  STRING_AGG(DISTINCT z.type_znieff, ', ') as types_znieff,
  
  -- Natura 2000
  STRING_AGG(DISTINCT n.nom_site, ', ') as natura2000,
  STRING_AGG(DISTINCT n.type_site, ', ') as types_natura,
  
  -- Parcs
  STRING_AGG(DISTINCT p.nom_parc, ', ') as parcs,
  
  -- Niveau protection
  CASE 
    WHEN p.code_parc IS NOT NULL THEN '🔴 Protection forte (Parc)'
    WHEN n.code_site IS NOT NULL THEN '🟠 Protection EU (Natura 2000)'
    WHEN z.type_znieff = 'ZNIEFF 1' THEN '🟡 Zone remarquable (ZNIEFF 1)'
    WHEN z.type_znieff = 'ZNIEFF 2' THEN '🟢 Zone écologique (ZNIEFF 2)'
    ELSE '⚪ Hors zone protégée'
  END as niveau_protection

FROM hydrometry_stations s
LEFT JOIN ref_znieff z ON ST_Intersects(s.geometry, z.geometrie)
LEFT JOIN ref_natura2000 n ON ST_Intersects(s.geometry, n.geometrie)
LEFT JOIN ref_parcs_naturels p ON ST_Intersects(s.geometry, p.geometrie)
GROUP BY s.code_station, s.libelle_station, s.geometry, p.code_parc, n.code_site, z.type_znieff;

-- Analyse qualité en zones protégées
SELECT 
  v.niveau_protection,
  COUNT(DISTINCT a.code_station) as nb_stations,
  ROUND(AVG(a.resultat) FILTER (WHERE a.code_parametre = '1340'), 2) as nitrates_moyen,
  COUNT(*) FILTER (WHERE a.resultat > nqe.nqe_ma) as nb_depassements_nqe
FROM quality_rivers_analyses a
JOIN v_stations_zones_protegees v ON a.code_station = v.code_station
LEFT JOIN ref_nqe_seuils nqe 
  ON a.code_parametre = nqe.code_parametre 
  AND a.code_support = nqe.code_support
WHERE a.code_qualification = '1'
GROUP BY v.niveau_protection;
```

**Gain attendu** :
- ✅ **Identification zones sensibles**
- ✅ **Contexte réglementaire** renforcé
- ✅ **Priorisation** surveillance
- ✅ **Conformité** Directives EU

**🔧 Action** : Télécharger couches INPN et croiser avec stations

---

## 📋 Référentiels Complémentaires

### Référentiels Géographiques Avancés

| Référentiel | Gestionnaire | Intérêt | Complexité | Priorité |
|-------------|--------------|---------|------------|----------|
| **BD TOPO (IGN)** | IGN | Géospatial 3D précis, altimétrie | 🔴 Élevée | ⭐ |
| **MNT - Modèle Numérique Terrain** | IGN | Pentes, bassins versants | 🔴 Élevée | ⭐ |
| **RGE Alti** | IGN | Altimétrie précise | 🟡 Moyenne | ⭐ |
| **Référentiel Adresses (BAN)** | IGN/DINUM | Géocodage adresses | 🟢 Faible | ⭐ |

### Référentiels Thématiques

| Référentiel | Gestionnaire | Intérêt | Complexité | Priorité |
|-------------|--------------|---------|------------|----------|
| **Référentiel Pédologique (Donesol)** | INRAE | Types de sols, texture | 🟡 Moyenne | ⭐⭐ |
| **BASIAS (Sites pollués)** | BRGM | Pollution historique sols | 🟡 Moyenne | ⭐⭐ |
| **BASOL (Sites pollués actifs)** | Ministère Écologie | Pollution active sols | 🟢 Faible | ⭐⭐ |
| **Installations Classées (ICPE)** | DREAL | Pressions industrielles | 🟢 Faible | ⭐⭐ |
| **Stations d'Épuration (SANDRE)** | SANDRE/Agences | Rejets assainissement | 🟢 Faible | ⭐⭐ |

### Référentiels Internationaux

| Référentiel | Gestionnaire | Intérêt | Complexité | Priorité |
|-------------|--------------|---------|------------|----------|
| **GEMET (Thésaurus EU)** | Agence EU Environnement | Terminologie multilingue | 🟢 Faible | ⭐ |
| **AGROVOC (FAO)** | FAO | Thésaurus agriculture | 🟢 Faible | ⭐ |
| **WFD CodeLists** | Commission EU | Codes rapportage DCE | 🟡 Moyenne | ⭐⭐ |
| **EUNIS (Habitats EU)** | Agence EU Environnement | Classification habitats | 🟡 Moyenne | ⭐ |

---

## 🚀 Plan d'Action

### Phase 1 - Quick Wins (1-2 semaines)

**Objectif** : Gains immédiats avec référentiels simples

| Action | Référentiel | Effort | Impact |
|--------|-------------|--------|--------|
| 1️⃣ Créer table NQE | Seuils réglementaires | 🟢 Faible | ⭐⭐⭐ |
| 2️⃣ Enrichir COG | INSEE Communes | 🟢 Faible | ⭐⭐⭐ |
| 3️⃣ Intégrer Masses d'Eau DCE | Rapportage DCE | 🟢 Faible | ⭐⭐⭐ |
| 4️⃣ Valider taxons TAXREF | MNHN | 🟡 Moyenne | ⭐⭐ |

### Phase 2 - Enrichissement Géospatial (1 mois)

**Objectif** : Analyses spatiales avancées

| Action | Référentiel | Effort | Impact |
|--------|-------------|--------|--------|
| 5️⃣ Croiser Corine Land Cover | Copernicus | 🟡 Moyenne | ⭐⭐⭐ |
| 6️⃣ Intégrer RPG parcelles | ASP | 🟡 Moyenne | ⭐⭐ |
| 7️⃣ Cartographier zones protégées | INPN | 🟡 Moyenne | ⭐⭐ |
| 8️⃣ Enrichir BSS détaillé | BRGM | 🔴 Élevée | ⭐⭐ |

### Phase 3 - Topologie Hydrologique (2 mois)

**Objectif** : Analyses réseau hydrographique

| Action | Référentiel | Effort | Impact |
|--------|-------------|--------|--------|
| 9️⃣ Intégrer BD CARTHAGE | IGN/OFB | 🔴 Élevée | ⭐⭐⭐ |
| 🔟 Calculer bassins versants | MNT + CARTHAGE | 🔴 Élevée | ⭐⭐⭐ |
| 1️⃣1️⃣ Modéliser propagation | Topologie réseau | 🔴 Élevée | ⭐⭐ |

### Phase 4 - Conformité & Interopérabilité (continu)

**Objectif** : Standards européens

| Action | Référentiel | Effort | Impact |
|--------|-------------|--------|--------|
| 1️⃣2️⃣ Conformité INSPIRE | Métadonnées INSPIRE | 🔴 Élevée | ⭐⭐ |
| 1️⃣3️⃣ Export WFD | CodeLists DCE | 🟡 Moyenne | ⭐ |
| 1️⃣4️⃣ Multilingue GEMET | Thésaurus EU | 🟢 Faible | ⭐ |

---

## 📊 Matrice Effort / Impact

```
Impact
  ⬆️
  │
3 │  🟢 NQE          🟢 COG         🟡 Corine LC    🔴 CARTHAGE
  │  🟢 DCE          🟡 TAXREF      🟡 Zones INPN   🔴 MNT/BV
  │
2 │                  🟡 RPG         🔴 BSS détaillé  🔴 INSPIRE
  │                  🔴 BD TOPO
  │
1 │  🟢 GEMET        🟡 WFD         🟡 BASIAS
  │
  └────────────────────────────────────────────────────➡️
     Faible 🟢     Moyen 🟡      Élevé 🔴          Effort
```

**Recommandation** : Commencer par quadrant **haut-gauche** (Impact élevé, Effort faible)

---

## 📈 ROI Estimé par Phase

| Phase | Durée | Référentiels | Analyses Nouvelles | Gain Métier |
|-------|-------|--------------|-------------------|-------------|
| **Phase 1** | 2 semaines | 4 | Conformité NQE, contexte démographique | ⭐⭐⭐ |
| **Phase 2** | 1 mois | 4 | Pressions agricoles, zones sensibles | ⭐⭐⭐ |
| **Phase 3** | 2 mois | 3 | Propagation pollutions, bassins versants | ⭐⭐⭐ |
| **Phase 4** | Continu | 3 | Interopérabilité EU, conformité | ⭐⭐ |

**ROI Global** : Enrichissement de **60-80%** des données avec contexte réglementaire, écologique et géographique !

---

## 🔗 Ressources de Téléchargement

### Référentiels Prioritaires

| Référentiel | Format | URL de Téléchargement |
|-------------|--------|----------------------|
| **NQE** | Manuel | Arrêtés ministériels (à compiler) |
| **COG INSEE** | CSV, Excel | [insee.fr/telechargement](https://www.insee.fr/fr/information/2560452) |
| **Masses d'Eau DCE** | SHP, GeoJSON | [geo.data.gouv.fr](https://geo.data.gouv.fr) |
| **TAXREF** | CSV, API | [taxref.mnhn.fr](https://taxref.mnhn.fr) |
| **Corine Land Cover** | Raster, Vector | [land.copernicus.eu](https://land.copernicus.eu) |
| **RPG** | SHP, GeoJSON | [data.gouv.fr/rpg](https://www.data.gouv.fr/fr/datasets/registre-parcellaire-graphique-rpg/) |
| **ZNIEFF/Natura** | SHP, GeoJSON | [inpn.mnhn.fr/telechargement](https://inpn.mnhn.fr/telechargement/cartes-et-information-geographique) |
| **BSS détaillé** | API, CSV | [infoterre.brgm.fr](https://infoterre.brgm.fr) |
| **BD CARTHAGE** | SHP | [geo.data.gouv.fr](https://geo.data.gouv.fr) |

### Portails de Référence

- **data.gouv.fr** : Portail OpenData national
- **geo.data.gouv.fr** : Portail géographique national
- **geocatalogue.fr** : Catalogue métadonnées géographiques
- **sandre.eaufrance.fr** : Référentiels eau
- **inpn.mnhn.fr** : Biodiversité et espaces naturels

---

## 📝 Conclusion

### Référentiels Actuels : Sous-Exploités

- ✅ **8 référentiels** déjà présents dans les données Hub'Eau
- ⚠️ **Niveau 1 uniquement** : codes stockés mais pas de jointures
- 📈 **Potentiel** : Passer au Niveau 3 = **gains majeurs**

### Référentiels à Intégrer : Opportunités Majeures

- 🎯 **13 référentiels prioritaires** identifiés
- ⭐ **Top 5** à intégrer en premier :
  1. NQE (conformité réglementaire)
  2. COG enrichi (contexte démographique)
  3. Masses d'Eau DCE (objectifs, pressions)
  4. TAXREF (validation taxons)
  5. Corine Land Cover (occupation sols)

### Impact Attendu

**Avec les 5 référentiels prioritaires** :
- ✅ **Alertes automatiques** dépassements réglementaires
- ✅ **Contexte écologique** complet (zones protégées, espèces)
- ✅ **Pressions identifiées** (agriculture, urbanisation)
- ✅ **Analyses spatiales** avancées
- ✅ **Conformité DCE** calculée automatiquement

**ROI** : Transformation données brutes → **Analyses métier actionnables** ! 🚀

