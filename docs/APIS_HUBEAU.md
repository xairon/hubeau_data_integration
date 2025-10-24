# Documentation de Référence Complète - APIs Hub'Eau

> **Source** : Documentation officielle Hub'Eau + Extraction réelle des schémas  
> **Dernière mise à jour** : 2025-10-10  
> **Version** : 2.0 - Documentation unifiée exhaustive

## 📑 Table des Matières

1. [Vue d'Ensemble](#-vue-densemble)
2. [Standards Techniques Communs](#-standards-techniques-communs)
3. [APIs Intégrées](#-apis-intégrées)
   - [1. Hydrométrie](#1-hydrométrie)
   - [2. Piézométrie](#2-piézométrie)
   - [3. Qualité des Cours d'Eau](#3-qualité-des-cours-deau)
   - [4. Qualité des Nappes](#4-qualité-des-nappes)
   - [5. Température](#5-température)
   - [6. Écoulement (ONDE)](#6-écoulement-onde)
   - [7. Hydrobiologie](#7-hydrobiologie)
   - [8. Prélèvements](#8-prélèvements)
4. [Limites et Contraintes](#-limites-et-contraintes)
5. [Références](#-références)
6. [Référentiels de Données](#-référentiels-de-données)
   - [SANDRE](#sandre---service-dadministration-nationale-des-données-et-référentiels-sur-leau)
   - [BDLISA](#bdlisa---référentiel-hydrogéologique-national)
7. [Ressources Complémentaires](#-ressources-complémentaires)

---

## 📊 Vue d'Ensemble

### Présentation

**Hub'Eau** est la plateforme nationale française de diffusion des données publiques sur l'eau via des APIs REST.  
Notre pipeline intègre **8 APIs principales** couvrant l'ensemble des données hydrologiques françaises.

### Couverture

- **Géographique** : France métropolitaine + DOM-TOM
- **Temporelle** : Données historiques (20+ ans) + temps réel
- **Mise à jour** : Quotidienne à mensuelle selon les APIs
- **Accès** : Gratuit, ouvert, respecte RGPD

### Statistiques d'Intégration

| API | Endpoints Intégrés | Total Attributs |
|-----|-------------------|-----------------|
| Hydrométrie | 3 | 85 |
| Piézométrie | 3 | 49 |
| Qualité Cours d'Eau | 4 | 197 |
| Qualité Nappes | 2 | 117 |
| Température | 2 | 63 |
| Écoulement | 3 | 65 |
| Hydrobiologie | 3 | 123 |
| Prélèvements | 3 | 79 |
| **TOTAL** | **23** | **778** |

---

## 🔧 Standards Techniques Communs

### Formats Supportés

Toutes les APIs Hub'Eau supportent :

| Format | Extension | Usage |
|--------|-----------|-------|
| **JSON** | `.json` | Format par défaut (recommandé) |
| **GeoJSON** | `.geojson` | Données géolocalisées avec géométrie |
| **CSV** | `.csv` | Export tableur |
| **JSONP** | `.json?callback=...` | Cross-domain (navigateurs anciens) |

### Protocoles

- **HTTP** : `http://hubeau.eaufrance.fr/api/...`
- **HTTPS** : `https://hubeau.eaufrance.fr/api/...` ✅ **Recommandé**
- **CORS** : Cross-Origin Resource Sharing activé

### Pagination

#### Types de Pagination

| Type | APIs | Paramètres | Avantage |
|------|------|------------|----------|
| **Page-based** | v1 (majorité) | `page`, `size` | Simple, navigation libre |
| **Cursor-based** | v2 (Hydrométrie) | `cursor`, `size` | Efficace pour grands volumes |

#### Structure Réponse Paginée

```json
{
  "count": 15234,
  "first": "https://hubeau.eaufrance.fr/api/v1/...",
  "last": "https://hubeau.eaufrance.fr/api/v1/...",
  "prev": null,
  "next": "https://hubeau.eaufrance.fr/api/v1/...",
  "api_version": "1.0.0",
  "data": [...]
}
```

### Filtrage Standard

Tous les endpoints stations supportent :

| Type Filtre | Paramètres | Exemple |
|-------------|------------|---------|
| **Géographique** | `longitude`, `latitude`, `distance` | `?longitude=2.3488&latitude=48.8534&distance=10` |
| **Temporel** | `date_debut_*`, `date_fin_*` | `?date_debut_mesure=2024-01-01&date_fin_mesure=2024-12-31` |
| **Administratif** | `code_departement`, `code_commune_insee`, `code_region` | `?code_departement=69` |
| **Hydrographique** | `code_cours_eau`, `code_bassin` | `?code_cours_eau=V---0000` |
| **Sélection champs** | `fields` | `?fields=code_station,libelle_station,resultat` |

### Codes de Qualification (Standard SANDRE)

| Code | Signification | Usage |
|------|---------------|-------|
| `1` | Bonne/Correcte | Donnée fiable, utilisable |
| `2` | Incertaine/Douteuse | À utiliser avec prudence |
| `4` | Mauvaise | Ne pas utiliser pour analyses |
| `16` | Non qualifiée | Qualification non effectuée |

---

## 📚 APIs Intégrées

## 1. Hydrométrie

**📍 Base URL** : `https://hubeau.eaufrance.fr/api/v2/hydrometrie`  
**📖 Documentation** : [API Hydrométrie](https://hubeau.eaufrance.fr/page/api-hydrometrie)  
**🔧 Version** : v2 (OpenAPI 3.0)  
**📊 Source** : Service Central d'Hydrométéorologie (SCHAPI)

### Description

L'API Hydrométrie diffuse les données de **débit**, **hauteur** et **niveau** des cours d'eau français.

### Endpoints Intégrés : 3

---

### 1.1 `/referentiel/stations`

**URL** : `https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations`  
**Description** : Référentiel des stations hydrométriques  
**Total attributs** : 38 | **Primary Key** : `code_station` | **Replication** : `date_maj_station`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_site` | string | Code du site hydrométrique parent |
| | `libelle_site` | string | Nom du site hydrométrique |
| | `code_station` | string [PK] | Code unique station (ex: "K4470010") |
| | `libelle_station` | string | Nom station (ex: "Le Rhône à Lyon [Perrache]") |
| **Localisation** | `coordonnee_x_station` | float | Coordonnée X (projection locale) |
| | `coordonnee_y_station` | float | Coordonnée Y (projection locale) |
| | `code_projection` | string | Code EPSG projection |
| | `longitude_station` | float | Longitude WGS84 |
| | `latitude_station` | float | Latitude WGS84 |
| | `altitude_ref_alti_station` | float | Altitude repère altimétrique (m NGF) |
| | `code_systeme_alti_site` | string | Code système altimétrique |
| | `geometry` | GeoJSON | Géométrie Point |
| **Administratif** | `code_commune_station` | string | Code INSEE commune |
| | `libelle_commune` | string | Nom commune |
| | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |
| | `code_region` | string | Code région |
| | `libelle_region` | string | Nom région |
| **Cours d'Eau** | `code_cours_eau` | string | Code SANDRE cours d'eau |
| | `libelle_cours_eau` | string | Nom cours d'eau |
| | `uri_cours_eau` | string | URI SANDRE |
| **Caractéristiques** | `type_station` | string | Type (hauteur, débit, niveau) |
| | `influence_locale_station` | string | Code influence locale |
| | `commentaire_influence_locale_station` | string | Détails influence |
| | `commentaire_station` | string | Commentaires généraux |
| | `descriptif_station` | string | Description détaillée |
| **État** | `en_service` | boolean | En service (true/false) |
| | `date_ouverture_station` | date | Date mise en service |
| | `date_fermeture_station` | date | Date fermeture (null si active) |
| | `date_maj_station` | datetime [REP] | Date dernière MAJ |
| **Qualité** | `code_regime_station` | string | Code régime hydraulique |
| | `qualification_donnees_station` | string | Qualification données |
| | `code_finalite_station` | string | Finalité station |
| | `type_contexte_loi_stat_station` | string | Contexte loi statistique |
| | `type_loi_station` | string | Type de loi |
| | `code_sandre_reseau_station` | string | Code réseau SANDRE |
| **Altimétrie** | `date_debut_ref_alti_station` | date | Date début référence |
| | `date_activation_ref_alti_station` | date | Date activation référence |
| | `date_maj_ref_alti_station` | date | Date MAJ référence |

**Filtres** : `code_station`, `code_site`, `code_departement`, `code_cours_eau`, `libelle_station`, `en_service`, `longitude`, `latitude`, `distance`

---

### 1.2 `/referentiel/sites`

**URL** : `https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/sites`  
**Description** : Référentiel des sites hydrométriques (groupements de stations)  
**Total attributs** : 34 | **Primary Key** : `code_site` | **Replication** : `date_maj_site`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_site` | string [PK] | Code unique site (ex: "K447000101") |
| | `libelle_site` | string | Nom site (ex: "Le Rhône à Lyon") |
| | `type_site` | string | Type de site |
| **Localisation** | `coordonnee_x_site` | float | Coordonnée X |
| | `coordonnee_y_site` | float | Coordonnée Y |
| | `code_projection` | string | Code EPSG |
| | `longitude_site` | float | Longitude WGS84 |
| | `latitude_site` | float | Latitude WGS84 |
| | `altitude_site` | float | Altitude (m NGF) |
| | `code_systeme_alti_site` | string | Système altimétrique |
| | `geometry` | GeoJSON | Géométrie Point |
| **Administratif** | `code_commune_site` | string | Code commune |
| | `libelle_commune` | string | Nom commune |
| | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |
| | `code_region` | string | Code région |
| | `libelle_region` | string | Nom région |
| **Hydrologie** | `code_cours_eau` | string | Code cours d'eau |
| | `libelle_cours_eau` | string | Nom cours d'eau |
| | `uri_cours_eau` | string | URI cours d'eau |
| | `code_entite_hydro_site` | string | Code entité hydrographique |
| | `code_troncon_hydro_site` | string | Code tronçon |
| | `code_zone_hydro_site` | string | Code zone hydro |
| **Bassin Versant** | `surface_bv` | float | Surface bassin versant (km²) |
| | `premier_mois_etiage_site` | int | Premier mois étiage (1-12) |
| | `premier_mois_annee_hydro_site` | int | Premier mois année hydrologique |
| **Caractéristiques** | `statut_site` | string | Statut site |
| | `influence_generale_site` | string | Code influence générale |
| | `commentaire_influence_generale_site` | string | Détails influence |
| | `commentaire_site` | string | Commentaires |
| **Données** | `grandeur_hydro` | string | Grandeurs mesurées (Q, H, N) |
| | `date_premiere_donnee_dispo_site` | date | Date première donnée |
| | `date_maj_site` | datetime [REP] | Date MAJ |
| **Réglementaire** | `type_contexte_loi_stat_site` | string | Contexte loi |
| | `type_loi_site` | string | Type de loi |

**Relation** : 1 Site → N Stations (ex: 1 site "Loire Tours" → 3 stations hauteur/débit/niveau)

---

### 1.3 `/obs_elab`

**URL** : `https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab`  
**Description** : Observations élaborées (historique complet validé)  
**Total attributs** : 13 | **Primary Keys** : `[code_station, date_obs_elab, grandeur_hydro_elab]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_site` | string | Code site |
| | `code_station` | string [PK1] | Code station |
| | `date_obs_elab` | datetime [PK2] | Date/heure observation |
| | `grandeur_hydro_elab` | string [PK3] | Type grandeur (Q, H, N) |
| **Localisation** | `longitude` | float | Longitude WGS84 |
| | `latitude` | float | Latitude WGS84 |
| **Résultat** | `resultat_obs_elab` | float | Valeur mesurée (m³/s pour Q, m pour H/N) |
| | `date_prod` | datetime | Date production donnée |
| **Qualification** | `code_statut` | string | Code statut |
| | `libelle_statut` | string | Libellé statut |
| | `code_qualification` | string | Code qualification |
| | `libelle_qualification` | string | Libellé qualification |
| | `code_methode` | string | Code méthode mesure |
| | `libelle_methode` | string | Libellé méthode |

**Grandeurs** :
- `Q` = Débit (m³/s)
- `H` = Hauteur d'eau (m)
- `N` = Niveau (m NGF)

**✅ Avantage** : Historique complet 20+ ans (vs observations_tr limité à 30j)

---

## 2. Piézométrie

**📍 Base URL** : `https://hubeau.eaufrance.fr/api/v1/niveaux_nappes`  
**📖 Documentation** : [API Piézométrie](https://hubeau.eaufrance.fr/page/api-piezometrie)  
**🔧 Version** : v1  
**📊 Source** : Portail ADES (Accès aux Données sur les Eaux Souterraines)

### Description

L'API Piézométrie diffuse les données de **niveau des nappes phréatiques**.

### Endpoints Intégrés : 3

---

### 2.1 `/stations`

**URL** : `https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations`  
**Description** : Référentiel des stations piézométriques (piézomètres)  
**Total attributs** : 22 | **Primary Key** : `code_bss` | **Replication** : `date_maj`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_bss` | string [PK] | Code BSS BRGM (ex: "08225X0037/F") |
| | `bss_id` | string | Identifiant BSS alternatif |
| | `urn_bss` | string | URN SANDRE point d'eau |
| **Localisation** | `x` | float | Coordonnée X (Lambert 93) |
| | `y` | float | Coordonnée Y (Lambert 93) |
| | `longitude` | float | Longitude WGS84 |
| | `latitude` | float | Latitude WGS84 |
| | `geometry` | GeoJSON | Géométrie Point |
| **Administratif** | `code_commune_insee` | string | Code INSEE commune |
| | `nom_commune` | string | Nom commune |
| | `code_departement` | string | Code département |
| | `nom_departement` | string | Nom département |
| **Caractéristiques** | `altitude_station` | float | Altitude station (m NGF) |
| | `profondeur_investigation` | float | Profondeur investigation forage (m) |
| | `libelle_pe` | string | Libellé point d'eau |
| **Temporel** | `date_debut_mesure` | date | Date première mesure |
| | `date_fin_mesure` | date | Date dernière mesure (null si active) |
| | `nb_mesures_piezo` | int | Nombre mesures disponibles |
| | `date_maj` | datetime [REP] | Date MAJ |
| **Géologie** | `codes_bdlisa` | array[string] | Codes formations géologiques BDLISA |
| | `urns_bdlisa` | array[string] | URNs BDLISA |
| **Masses d'Eau** | `codes_masse_eau_edl` | array[string] | Codes masses d'eau (État des lieux DCE) |
| | `noms_masse_eau_edl` | array[string] | Noms masses d'eau |
| | `urns_masse_eau_edl` | array[string] | URNs masses d'eau |

**BSS** = Banque du Sous-Sol (référentiel national BRGM des ouvrages souterrains)

---

### 2.2 `/chroniques_tr`

**URL** : `https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques_tr`  
**Description** : Chroniques temps réel (télétransmission horaire)  
**Total attributs** : 12 | **Primary Keys** : `[code_bss, timestamp_mesure]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_bss` | string [PK1] | Code BSS |
| | `bss_id` | string | ID BSS alternatif |
| | `urn_bss` | string | URN SANDRE |
| **Temporel** | `date_mesure` | date | Date mesure |
| | `timestamp_mesure` | datetime [PK2] | Timestamp précis |
| | `date_maj` | datetime | Date MAJ |
| **Localisation** | `longitude` | float | Longitude WGS84 |
| | `latitude` | float | Latitude WGS84 |
| **Niveaux** | `altitude_station` | float | Altitude station (m NGF) |
| | `altitude_repere` | float | Altitude repère (m NGF) |
| | `niveau_eau_ngf` | float | **Niveau nappe en m NGF** (altitude absolue) |
| | `profondeur_nappe` | float | **Profondeur vs repère** (m, >0 = sous sol) |

**Formule** : `niveau_eau_ngf = altitude_repere - profondeur_nappe`  
**Stations** : ~1700 piézomètres équipés télétransmission  
**Fréquence** : Horaire

---

### 2.3 `/chroniques`

**URL** : `https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques`  
**Description** : Chroniques archivées validées (historique complet)  
**Total attributs** : ~15 | **Primary Keys** : `[code_bss, timestamp_mesure]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| *(Tous champs de chroniques_tr)* | | | Voir section 2.2 |
| **Qualification** | `code_qualification` | string | Code qualification (1, 2, 4, 16) |
| | `libelle_qualification` | string | Libellé qualification |
| | `mode_obtention` | string | Mode (manuel, automatique, télétransmission) |
| | `statut_mesure` | string | Statut (brute, validée, qualifiée) |
| | `producteur` | string | Producteur (DREAL, Agence Eau, etc.) |

**Différence vs chroniques_tr** : Données validées, fréquence mensuelle/trimestrielle, historique complet

---

## 3. Qualité des Cours d'Eau

**📍 Base URL** : `https://hubeau.eaufrance.fr/api/v2/qualite_rivieres`  
**📖 Documentation** : [API Qualité Cours d'Eau](https://hubeau.eaufrance.fr/page/api-qualite-cours-deau)  
**🔧 Version** : v2  
**📊 Source** : Portail Naïades

### Description

L'API Qualité diffuse les **analyses physico-chimiques et microbiologiques** des eaux de surface.

### Endpoints Intégrés : 4 (COMPLET)

---

### 3.1 `/station_pc`

**URL** : `https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/station_pc`  
**Description** : Stations de prélèvement physico-chimique  
**Total attributs** : 44 | **Primary Key** : `code_station` | **Replication** : `date_maj_information`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_station` | string [PK] | Code unique station |
| | `libelle_station` | string | Nom station |
| | `uri_station` | string | URI SANDRE |
| **Localisation** | `coordonnee_x` | float | X Lambert |
| | `coordonnee_y` | float | Y Lambert |
| | `code_projection` | string | Code projection |
| | `libelle_projection` | string | Libellé projection |
| | `longitude` | float | Longitude WGS84 |
| | `latitude` | float | Latitude WGS84 |
| | `geometry` | GeoJSON | Géométrie Point |
| **Administratif** | `code_commune` | string | Code commune |
| | `libelle_commune` | string | Nom commune |
| | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |
| | `code_region` | string | Code région |
| | `libelle_region` | string | Nom région |
| **Cours d'Eau** | `code_cours_eau` | string | Code cours d'eau |
| | `nom_cours_eau` | string | Nom cours d'eau |
| | `uri_cours_eau` | string | URI cours d'eau |
| **Masse d'Eau DCE** | `code_masse_deau` | string | Code masse d'eau |
| | `code_eu_masse_deau` | string | Code EU masse d'eau |
| | `nom_masse_deau` | string | Nom masse d'eau |
| | `uri_masse_deau` | string | URI masse d'eau |
| **Bassins** | `code_eu_sous_bassin` | string | Code EU sous-bassin |
| | `nom_sous_bassin` | string | Nom sous-bassin |
| | `uri_sous_bassin` | string | URI sous-bassin |
| | `code_bassin` | string | Code bassin |
| | `code_eu_bassin` | string | Code EU bassin |
| | `nom_bassin` | string | Nom bassin |
| | `uri_bassin` | string | URI bassin |
| **Caractéristiques** | `durete` | string | Dureté eau (douce, moyenne, dure) |
| | `type_entite_hydro` | string | Type entité hydrographique |
| | `nature` | string | Nature station |
| | `localisation_precise` | string | Localisation précise |
| | `point_kilometrique` | float | PK sur cours d'eau |
| | `altitude_point_caracteristique` | float | Altitude point (m NGF) |
| | `superficie_bassin_versant_reel` | float | Surface BV réelle (km²) |
| | `superficie_bassin_versant_topo` | float | Surface BV topo (km²) |
| | `premier_mois_annee_etiage` | int | Premier mois étiage |
| **État** | `finalite` | string | Finalité (DCE, contrôle opérationnel, etc.) |
| | `commentaire` | string | Commentaires |
| | `date_creation` | date | Date création |
| | `date_arret` | date | Date arrêt (null si active) |
| | `date_maj_information` | datetime [REP] | Date MAJ |

---

### 3.2 `/analyse_pc`

**URL** : `https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/analyse_pc`  
**Description** : Analyses physico-chimiques des prélèvements  
**Total attributs** : 70 | **Primary Keys** : `[code_station, date_prelevement, code_parametre]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_analyse` | string | Code unique analyse |
| | `code_prelevement` | string | Code prélèvement |
| | `code_operation` | string | Code opération |
| | `code_point_eau_surface` | string | Code point d'eau |
| | `code_banque_reference` | string | Code banque référence |
| **Station** | `code_station` | string [PK1] | Code station |
| | `libelle_station` | string | Nom station |
| | `uri_station` | string | URI station |
| | `longitude` | float | Longitude |
| | `latitude` | float | Latitude |
| | `geometry` | GeoJSON | Géométrie |
| **Temporel** | `date_prelevement` | date [PK2] | Date prélèvement |
| | `heure_prelevement` | time | Heure prélèvement |
| | `date_maj_analyse` | datetime | Date MAJ analyse |
| | `heure_analyse` | time | Heure analyse |
| **Support** | `code_support` | string | Code support (3=eau, 6=sédiment, 17=biote) |
| | `libelle_support` | string | Libellé support |
| | `uri_support` | string | URI support |
| | `code_fraction` | string | Fraction (brute, dissoute, particulaire) |
| | `libelle_fraction` | string | Libellé fraction |
| | `uri_fraction` | string | URI fraction |
| **Paramètre** | `code_parametre` | string [PK3] | Code SANDRE (ex: 1340=Nitrates) |
| | `libelle_parametre` | string | Nom paramètre |
| | `uri_parametre` | string | URI paramètre |
| | `code_groupe_parametre` | string | Groupe (nutriments, métaux, pesticides, etc.) |
| | `libelle_groupe_parametre` | string | Libellé groupe |
| | `uri_groupe_parametre` | string | URI groupe |
| **Résultat** | `resultat` | float | Valeur mesurée |
| | `code_unite` | string | Code unité SANDRE |
| | `symbole_unite` | string | Symbole (mg/L, µg/L, ng/L, etc.) |
| | `uri_unite` | string | URI unité |
| **Limites** | `limite_detection` | float | Limite détection (LD) |
| | `limite_quantification` | float | Limite quantification (LQ) |
| | `limite_saturation` | float | Limite saturation |
| | `incertitude_analytique` | float | Incertitude (%) |
| **Qualification** | `code_qualification` | string | Code (1=correct, 2=incorrect, 4=douteux, 6=non qualifié) |
| | `libelle_qualification` | string | Libellé qualification |
| | `code_statut` | string | Code statut |
| | `mnemo_statut` | string | Mnémonique statut |
| | `code_remarque` | string | Remarque (< LQ, > saturation, etc.) |
| | `mnemo_remarque` | string | Mnémonique remarque |
| | `code_insitu` | string | Code mesure in-situ |
| | `libelle_insitu` | string | Libellé in-situ |
| | `code_difficulte_analyse` | string | Difficulté analyse |
| | `mnemo_difficulte_analyse` | string | Mnémonique difficulté |
| **Méthodes** | `code_methode_analyse` | string | Méthode analytique (chromatographie, spectrométrie) |
| | `nom_methode_analyse` | string | Nom méthode |
| | `uri_methode_analyse` | string | URI méthode |
| | `code_methode_fractionnement` | string | Méthode fractionnement |
| | `nom_methode_fractionnement` | string | Nom |
| | `uri_methode_fractionnement` | string | URI |
| | `code_methode_extraction` | string | Méthode extraction |
| | `nom_methode_extraction` | string | Nom |
| | `uri_methode_extraction` | string | URI |
| | `rendement_extraction` | float | Rendement extraction (%) |
| **Accréditation** | `code_accreditation` | string | Code accréditation COFRAC |
| | `mnemo_accreditation` | string | Mnémonique |
| | `agrement` | string | Agrément laboratoire |
| **Commentaires** | `commentaires_analyse` | string | Commentaires généraux |
| | `commentaires_resultat_analyse` | string | Commentaires résultat |
| **Réseau** | `code_reseau` | string | Code réseau (DCE, RCS, ROE, etc.) |
| | `nom_reseau` | string | Nom réseau |
| | `uri_reseau` | string | URI réseau |
| **Acteurs** | `code_producteur_analyse` | string | Code producteur |
| | `nom_producteur_analyse` | string | Nom producteur (Agence Eau, etc.) |
| | `uri_producteur_prelevement` | string | URI producteur |
| | `code_preleveur` | string | Code préleveur |
| | `nom_preleveur` | string | Nom préleveur |
| | `uri_preleveur` | string | URI préleveur |
| | `code_laboratoire` | string | Code laboratoire |
| | `nom_laboratoire` | string | Nom laboratoire |
| | `uri_laboratoire` | string | URI laboratoire |

**Groupes de Paramètres** (200+ au total) :
- **Physico-chimie** : pH, conductivité, MES, COD, etc.
- **Nutriments** : Nitrates (NO3), Nitrites (NO2), Phosphates (PO4), Ammonium (NH4)
- **Métaux** : Plomb, Mercure, Cadmium, Chrome, Nickel, Arsenic, etc.
- **Pesticides** : Glyphosate, Atrazine, Diuron, etc.
- **Hydrocarbures** : HAP, BTEX, etc.
- **Microbiologie** : E. coli, Entérocoques

---

### 3.3 `/operation_pc`

**URL** : `https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/operation_pc`  
**Description** : Opérations de prélèvement (métadonnées terrain)  
**Total attributs** : 41 | **Primary Keys** : `[code_station, date_prelevement, code_operation]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_station` | string [PK1] | Code station |
| | `libelle_station` | string | Nom station |
| | `uri_station` | string | URI station |
| | `code_operation` | string [PK2] | Code unique opération |
| | `code_prelevement` | string [PK3] | Code prélèvement |
| | `code_point_eau_surface` | string | Code point d'eau |
| | `code_banque_reference` | string | Code banque |
| **Localisation Prélèvement** | `longitude` | float | Longitude prélèvement |
| | `latitude` | float | Latitude prélèvement |
| | `x_prelevement` | float | X prélèvement |
| | `y_prelevement` | float | Y prélèvement |
| | `code_projection` | string | Code projection |
| | `libelle_projection` | string | Libellé projection |
| | `geometry` | GeoJSON | Géométrie |
| **Temporel** | `date_prelevement` | date | Date prélèvement |
| | `heure_prelevement` | time | Heure début |
| | `date_fin` | date | Date fin |
| | `heure_fin` | time | Heure fin |
| **Support** | `code_support` | string | Code support |
| | `libelle_support` | string | Libellé support |
| | `uri_support` | string | URI support |
| **Méthode** | `code_methode` | string | Méthode prélèvement |
| | `nom_methode` | string | Nom méthode |
| | `uri_methode` | string | URI méthode |
| **Caractéristiques** | `code_zone_verticale_prospectee` | string | Zone verticale (surface, fond, mi-profondeur) |
| | `mnemo_zone_verticale_prospectee` | string | Mnémonique zone |
| | `profondeur` | float | Profondeur prélèvement (m) |
| **Qualité** | `code_difficulte` | string | Difficulté prélèvement |
| | `mnemo_difficulte` | string | Mnémonique difficulté |
| | `code_accreditation` | string | Accréditation |
| | `mnemo_accreditation` | string | Mnémonique |
| | `agrement` | string | Agrément |
| **Finalité** | `code_finalite` | string | Finalité (DCE, police eau, etc.) |
| | `libelle_finalite` | string | Libellé finalité |
| **Réseau** | `code_reseau` | string | Code réseau |
| | `nom_reseau` | string | Nom réseau |
| | `uri_reseau` | string | URI réseau |
| **Acteurs** | `code_producteur` | string | Code producteur |
| | `nom_producteur` | string | Nom producteur |
| | `uri_producteur` | string | URI producteur |
| | `code_preleveur` | string | Code préleveur |
| | `nom_preleveur` | string | Nom préleveur |
| | `uri_preleveur` | string | URI préleveur |
| **Commentaires** | `commentaires` | string | Commentaires opération |

**Usage** : Contexte et traçabilité des analyses - Permet d'associer une analyse à son prélèvement terrain

---

### 3.4 `/condition_environnementale_pc`

**URL** : `https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/condition_environnementale_pc`  
**Description** : Conditions environnementales lors du prélèvement (mesures in-situ)  
**Total attributs** : 42 | **Primary Keys** : `[code_station, date_prelevement, code_parametre]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_station` | string [PK1] | Code station |
| | `libelle_station` | string | Nom station |
| | `uri_station` | string | URI station |
| | `code_prelevement` | string | Code prélèvement |
| | `code_operation_cep` | string | Code opération |
| | `code_banque_reference` | string | Code banque |
| | `code_point_eau_surface` | string | Code point d'eau |
| **Paramètre** | `code_parametre` | string [PK2] | Code paramètre environnemental |
| | `libelle_parametre` | string [PK3] | Nom paramètre |
| | `uri_parametre` | string | URI paramètre |
| | `code_groupe_parametre` | string | Groupe |
| | `libelle_groupe_parametre` | string | Libellé groupe |
| | `uri_groupe_parametre` | string | URI groupe |
| **Résultat** | `resultat` | float | Valeur mesurée |
| | `libelle_resultat` | string | Libellé (si qualitatif) |
| | `code_unite` | string | Code unité |
| | `symbole_unite` | string | Symbole (°C, mg/L, m³/s, etc.) |
| | `uri_unite` | string | URI unité |
| **Temporel** | `date_prelevement` | date | Date prélèvement |
| | `date_mesure` | date | Date mesure paramètre |
| | `heure_mesure` | time | Heure mesure |
| | `date_maj` | datetime | Date MAJ |
| **Qualification** | `code_qualification` | string | Code qualification |
| | `libelle_qualification` | string | Libellé |
| | `code_statut` | string | Code statut |
| | `mnemo_statut` | string | Mnémonique |
| | `code_remarque` | string | Code remarque |
| | `mnemo_remarque` | string | Mnémonique |
| **Méthode** | `code_methode` | string | Méthode mesure |
| | `nom_methode` | string | Nom méthode |
| | `uri_methode` | string | URI méthode |
| **Acteurs** | `code_producteur` | string | Code producteur |
| | `nom_producteur` | string | Nom producteur |
| | `uri_producteur` | string | URI producteur |
| | `code_preleveur` | string | Code préleveur |
| | `nom_preleveur` | string | Nom préleveur |
| | `uri_preleveur` | string | URI préleveur |
| **Localisation** | `longitude` | float | Longitude |
| | `latitude` | float | Latitude |
| | `geometry` | GeoJSON | Géométrie |
| **Masse d'Eau** | `code_masse_deau` | string | Code masse d'eau |
| | `code_eu_masse_deau` | string | Code EU |
| | `nom_masse_deau` | string | Nom masse d'eau |
| **Commentaires** | `commentaire` | string | Commentaires |

**Paramètres Environnementaux Typiques** :
- Température eau (°C)
- pH (unité pH)
- Conductivité (µS/cm)
- Oxygène dissous (mg/L ou % saturation)
- Débit cours d'eau (m³/s)
- Turbidité (NTU)
- Conditions météo

**Usage** : Interprétation des résultats d'analyses - Contextualisation physico-chimique

---

### Stratégie d'Ingestion Qualité Cours d'Eau

| Endpoint | Mode Slicing | Optimisation |
|----------|--------------|--------------|
| `station_pc` | `dept` | 107 départements |
| `analyse_pc` | `station_month_chunked` | 20 stations × 12 mois ≈ 960 requêtes |
| `operation_pc` | `station_month_chunked` | Idem |
| `condition_environnementale_pc` | `station_month_chunked` | Idem |

---

## 4. Qualité des Nappes

**📍 Base URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_nappes`  
**📖 Documentation** : [API Qualité Nappes](https://hubeau.eaufrance.fr/page/api-qualite-nappes)  
**🔧 Version** : v1  
**📊 Source** : Portail ADES

### Description

L'API Qualité Nappes diffuse les **analyses des eaux souterraines**.

### Endpoints Intégrés : 2

---

### 4.1 `/stations`

**URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_nappes/stations`  
**Description** : Stations de surveillance qualité nappes  
**Total attributs** : 47 | **Primary Key** : `bss_id` ou `code_bss`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `bss_id` | string [PK] | ID BSS principal |
| | `code_bss` | string | Code BSS |
| | `urn_bss` | string | URN SANDRE |
| **Localisation** | `longitude` | float | Longitude WGS84 |
| | `latitude` | float | Latitude WGS84 |
| | `altitude` | float | Altitude (m NGF) |
| | `precision_coordonnees` | string | Précision |
| | `geometry` | GeoJSON | Géométrie |
| **Administratif** | `code_insee` | string | Code commune |
| | `nom_commune` | string | Nom commune |
| | `num_departement` | string | Numéro département |
| | `nom_departement` | string | Nom département |
| | `nom_region` | string | Nom région |
| | `circonscriptions_administrative_bassin` | string | Circonscriptions |
| **Bassins DCE** | `bassin_dce` | string | Bassin DCE |
| | `code_bassin_dce` | string | Code bassin |
| | `urn_bassin_dce` | string | URN bassin |
| **Point d'Eau** | `code_nature_pe` | string | Nature PE (forage, source, puits) |
| | `nom_nature_pe` | string | Nom nature |
| | `uri_nature_pe` | string | URI nature |
| | `libelle_pe` | string | Libellé PE |
| | `code_etat_pe` | string | État (actif, abandonné) |
| | `nom_etat_pe` | string | Nom état |
| | `uri_etat_pe` | string | URI état |
| | `commentaire_pe` | string | Commentaires |
| **Aquifère** | `code_caracteristique_aquifere` | string | Caractéristique aquifère |
| | `nom_caracteristique_aquifere` | string | Nom |
| | `uri_caracteristique_aquifere` | string | URI |
| | `code_mode_gisement` | string | Mode gisement (libre, captif) |
| | `nom_mode_gisement` | string | Nom mode |
| | `uri_mode_gisement` | string | URI |
| | `profondeur_investigation` | float | Profondeur (m) |
| **Géologie BDLISA** | `codes_entite_hg_bdlisa` | array[string] | Codes formations géologiques |
| | `noms_entite_hg_bdlisa` | array[string] | Noms formations |
| | `urns_bdlisa` | array[string] | URNs BDLISA |
| **Masses Eau (Rapportage)** | `codes_masse_eau_rap` | array[string] | Codes masses d'eau rapportage DCE |
| | `noms_masse_eau_rap` | array[string] | Noms masses d'eau |
| | `urns_masse_eau_rap` | array[string] | URNs masses d'eau |
| **Masses Eau (État Lieux)** | `codes_masse_eau_edl` | array[string] | Codes masses d'eau état des lieux |
| | `noms_masse_eau_edl` | array[string] | Noms masses d'eau |
| | `urns_masse_eau_edl` | array[string] | URNs masses d'eau |
| **Réseaux** | `codes_reseau` | array[string] | Codes réseaux surveillance |
| | `noms_reseau` | array[string] | Noms réseaux |
| | `uris_reseau` | array[string] | URIs réseaux |
| **Temporel** | `date_debut_mesure` | date | Date première mesure |
| | `date_fin_mesure` | date | Date dernière mesure |

---

### 4.2 `/analyses`

**URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_nappes/analyses`  
**Description** : Analyses physico-chimiques eaux souterraines  
**Total attributs** : ~70 | **Primary Keys** : `[code_bss, date_debut_prelevement, code_param]`

**Schéma** : Identique à `/analyse_pc` de Qualité Cours d'Eau (section 3.2)  
**Particularité** : Utilise `code_bss` au lieu de `code_station`

---

## 5. Température

**📍 Base URL** : `https://hubeau.eaufrance.fr/api/v1/temperature`  
**📖 Documentation** : [API Température](https://hubeau.eaufrance.fr/page/api-temperature-continu)  
**🔧 Version** : v1  
**📊 Source** : Portail Naïades

### Description

L'API Température diffuse les **chroniques de température en continu** des cours d'eau.

### Endpoints Intégrés : 2

---

### 5.1 `/station`

**URL** : `https://hubeau.eaufrance.fr/api/v1/temperature/station`  
**Description** : Stations de température  
**Total attributs** : 42 | **Primary Key** : `code_station` | **Replication** : `date_maj_infos`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_station` | string [PK] | Code unique station |
| | `libelle_station` | string | Nom station |
| | `uri_station` | string | URI SANDRE |
| **Localisation** | `coordonnee_x` | float | X |
| | `coordonnee_y` | float | Y |
| | `code_type_projection` | string | Projection |
| | `libelle_type_projection` | string | Libellé projection |
| | `longitude` | float | Longitude WGS84 |
| | `latitude` | float | Latitude WGS84 |
| | `altitude` | float | Altitude (m NGF) |
| | `pk` | float | Point kilométrique |
| | `localisation` | string | Description localisation |
| | `geometry` | GeoJSON | Géométrie |
| **Administratif** | `code_commune` | string | Code commune |
| | `libelle_commune` | string | Nom commune |
| | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |
| | `code_region` | string | Code région |
| | `libelle_region` | string | Nom région |
| **Cours d'Eau** | `code_troncon_hydro` | string | Tronçon hydrographique |
| | `code_cours_eau` | string | Code cours d'eau |
| | `libelle_cours_eau` | string | Nom cours d'eau |
| | `uri_cours_eau` | string | URI cours d'eau |
| **Masse d'Eau** | `code_masse_eau` | string | Code masse d'eau |
| | `code_eu_masse_eau` | string | Code EU |
| | `libelle_masse_eau` | string | Nom masse d'eau |
| | `uri_masse_eau` | string | URI |
| **Bassin** | `code_sous_bassin` | string | Code sous-bassin |
| | `libelle_sous_bassin` | string | Nom sous-bassin |
| | `uri_sous_bassin` | string | URI |
| | `code_bassin` | string | Code bassin |
| | `code_eu_bassin` | string | Code EU |
| | `libelle_bassin` | string | Nom bassin |
| | `uri_bassin` | string | URI bassin |
| **Bassin Versant** | `superficie_topo` | float | Surface BV topo (km²) |
| | `superficie_reelle` | float | Surface BV réelle (km²) |
| | `premier_mois_etiage` | int | Premier mois étiage |
| **Caractéristiques** | `nature_station` | string | Nature station |
| | `type_entite_hydro` | string | Type entité hydro |
| | `commentaire` | string | Commentaires |
| **État** | `date_mise_en_service` | date | Date mise en service |
| | `date_mise_hors_service` | date | Date mise hors service |
| | `date_maj_infos` | datetime [REP] | Date MAJ |

---

### 5.2 `/chronique`

**URL** : `https://hubeau.eaufrance.fr/api/v1/temperature/chronique`  
**Description** : Chroniques température en continu  
**Total attributs** : 21 | **Primary Keys** : `[code_station, date_mesure_temp]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Station** | `code_station` | string [PK1] | Code station |
| | `libelle_station` | string | Nom station |
| | `uri_station` | string | URI station |
| | `localisation` | string | Description localisation |
| **Localisation** | `longitude` | float | Longitude |
| | `latitude` | float | Latitude |
| | `geometry` | GeoJSON | Géométrie |
| | `code_commune` | string | Code commune |
| | `libelle_commune` | string | Nom commune |
| **Cours d'Eau** | `code_cours_eau` | string | Code cours d'eau |
| | `libelle_cours_eau` | string | Nom cours d'eau |
| | `uri_cours_eau` | string | URI cours d'eau |
| **Mesure** | `code_parametre` | string | Code paramètre température |
| | `libelle_parametre` | string | Libellé |
| | `date_mesure_temp` | datetime [PK2] | Date/heure mesure |
| | `heure_mesure_temp` | time | Heure (redondant) |
| | `resultat` | float | Température (°C) |
| **Unité** | `code_unite` | string | Code unité |
| | `symbole_unite` | string | Symbole (°C) |
| **Qualification** | `code_qualification` | string | Code qualification |
| | `libelle_qualification` | string | Libellé |

---

## 6. Écoulement (ONDE)

**📍 Base URL** : `https://hubeau.eaufrance.fr/api/v1/ecoulement`  
**📖 Documentation** : [API Écoulement](https://hubeau.eaufrance.fr/page/api-ecoulement)  
**🔧 Version** : v1 (OpenAPI 3.0)  
**📊 Source** : Observatoire National Des Étiages (OFB)

### Description

L'API Écoulement diffuse les **observations visuelles d'écoulement** des petits et moyens cours d'eau.

**Spécificité** : Observation visuelle uniquement (pas de mesure instrumentale), principalement période estivale.

### Endpoints Intégrés : 3

---

### 6.1 `/stations`

**URL** : `https://hubeau.eaufrance.fr/api/v1/ecoulement/stations`  
**Description** : Stations ONDE  
**Total attributs** : 23 | **Primary Key** : `code_station` | **Replication** : `date_maj_station`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_station` | string [PK] | Code unique station ONDE |
| | `libelle_station` | string | Nom station |
| | `uri_station` | string | URI SANDRE |
| **Localisation** | `coordonnee_x_station` | float | X |
| | `coordonnee_y_station` | float | Y |
| | `code_projection_station` | string | Code projection |
| | `libelle_projection_station` | string | Libellé projection |
| | `code_epsg_station` | string | Code EPSG |
| | `longitude` | float | Longitude WGS84 |
| | `latitude` | float | Latitude WGS84 |
| | `geometry` | GeoJSON | Géométrie |
| **Administratif** | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |
| | `code_commune` | string | Code commune |
| | `libelle_commune` | string | Nom commune |
| | `code_region` | string | Code région |
| | `libelle_region` | string | Nom région |
| | `code_bassin` | string | Code bassin |
| | `libelle_bassin` | string | Nom bassin |
| **Cours d'Eau** | `code_cours_eau` | string | Code cours d'eau |
| | `libelle_cours_eau` | string | Nom cours d'eau |
| | `uri_cours_eau` | string | URI cours d'eau |
| **État** | `etat_station` | string | État station |
| | `date_maj_station` | datetime [REP] | Date MAJ |

---

### 6.2 `/observations`

**URL** : `https://hubeau.eaufrance.fr/api/v1/ecoulement/observations`  
**Description** : Observations visuelles d'écoulement  
**Total attributs** : 27 | **Primary Keys** : `[code_station, date_observation]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Station** | `code_station` | string [PK1] | Code station |
| | `libelle_station` | string | Nom station |
| | `uri_station` | string | URI station |
| | `coordonnee_x_station` | float | X station |
| | `coordonnee_y_station` | float | Y station |
| | `code_projection_station` | string | Projection |
| | `libelle_projection_station` | string | Libellé projection |
| | `longitude` | float | Longitude |
| | `latitude` | float | Latitude |
| | `geometry` | GeoJSON | Géométrie |
| **Administratif** | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |
| | `code_commune` | string | Code commune |
| | `libelle_commune` | string | Nom commune |
| | `code_region` | string | Code région |
| | `libelle_region` | string | Nom région |
| | `code_bassin` | string | Code bassin |
| | `libelle_bassin` | string | Nom bassin |
| **Cours d'Eau** | `code_cours_eau` | string | Code cours d'eau |
| | `libelle_cours_eau` | string | Nom cours d'eau |
| | `uri_cours_eau` | string | URI cours d'eau |
| **Observation** | `date_observation` | date [PK2] | Date observation |
| | `code_campagne` | string | Code campagne |
| | `code_ecoulement` | string | Code écoulement observé |
| | `libelle_ecoulement` | string | Libellé écoulement |
| **Réseau** | `code_reseau` | string | Code réseau |
| | `libelle_reseau` | string | Nom réseau |
| | `uri_reseau` | string | URI réseau |

**Codes Écoulement** :
- `1` = Écoulement visible
- `1a` = Écoulement visible faible
- `2` = Écoulement non visible
- `3` = Assec (cours d'eau à sec)
- `4` = Observation impossible

---

### 6.3 `/campagnes`

**URL** : `https://hubeau.eaufrance.fr/api/v1/ecoulement/campagnes`  
**Description** : Campagnes d'observation ONDE  
**Total attributs** : ~15 | **Primary Keys** : `[code_departement, date_campagne]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_departement` | string [PK1] | Code département |
| | `date_campagne` | date [PK2] | Date campagne |
| | `code_campagne` | string | Code unique campagne |
| **Descriptif** | `libelle_campagne` | string | Libellé (ex: "Campagne ONDE Juillet 2024") |
| | `commentaire` | string | Commentaires |
| **Statistiques** | `nb_stations` | int | Nombre stations observées |
| | `nb_observations` | int | Nombre observations réalisées |

**Usage** : Permet de caler les fenêtres temporelles de requête sur les périodes réelles de campagne

---

## 7. Hydrobiologie

**📍 Base URL** : `https://hubeau.eaufrance.fr/api/v1/hydrobio`  
**📖 Documentation** : [API Hydrobiologie](https://hubeau.eaufrance.fr/page/api-hydrobiologie)  
**🔧 Version** : v1  
**📊 Source** : Portail Naïades

### Description

L'API Hydrobiologie diffuse les données de **peuplement biologique** : macroinvertébrés, diatomées, macrophytes, poissons.

### Endpoints Intégrés : 3

---

### 7.1 `/stations_hydrobio`

**URL** : `https://hubeau.eaufrance.fr/api/v1/hydrobio/stations_hydrobio`  
**Description** : Stations hydrobiologiques  
**Total attributs** : 34 | **Primary Key** : `code_station_hydrobio`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_station_hydrobio` | string [PK] | Code station |
| | `libelle_station_hydrobio` | string | Nom station |
| | `uri_station_hydrobio` | string | URI SANDRE |
| **Localisation** | `coordonnee_x` | float | X |
| | `coordonnee_y` | float | Y |
| | `code_projection` | string | Projection |
| | `longitude` | float | Longitude |
| | `latitude` | float | Latitude |
| | `geometry` | GeoJSON | Géométrie |
| **Administratif** | `code_commune` | string | Code commune |
| | `libelle_commune` | string | Nom commune |
| | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |
| | `code_region` | string | Code région |
| | `libelle_region` | string | Nom région |
| **Cours d'Eau** | `code_cours_eau` | string | Code cours d'eau |
| | `libelle_cours_eau` | string | Nom cours d'eau |
| | `uri_cours_eau` | string | URI cours d'eau |
| **Masse d'Eau** | `code_masse_eau` | string | Code masse d'eau |
| | `libelle_masse_eau` | string | Nom masse d'eau |
| | `uri_masse_eau` | string | URI masse d'eau |
| **Bassin** | `code_sous_bassin` | string | Code sous-bassin |
| | `libelle_sous_bassin` | string | Nom sous-bassin |
| | `code_bassin` | string | Code bassin |
| | `libelle_bassin` | string | Nom bassin |
| **Réseaux** | `codes_reseaux` | array[string] | Codes réseaux surveillance |
| | `libelles_reseaux` | array[string] | Noms réseaux |
| **Supports** | `codes_supports` | array[string] | Supports analysés |
| | `libelles_supports` | array[string] | Libellés supports |
| **Taxons Disponibles** | `codes_appel_taxons` | array[string] | Codes taxons station |
| | `libelles_appel_taxons` | array[string] | Noms taxons |
| **Indices Disponibles** | `codes_indices` | array[string] | Codes indices station |
| | `libelles_indices` | array[string] | Noms indices |
| **Temporel** | `date_premier_prelevement` | date | Date 1er prélèvement |
| | `date_dernier_prelevement` | date | Date dernier prélèvement |

---

### 7.2 `/indices`

**URL** : `https://hubeau.eaufrance.fr/api/v1/hydrobio/indices`  
**Description** : Indices biologiques (IBGN, IBD, I2M2, IBMR, IPR)  
**Total attributs** : 41 | **Primary Keys** : `[code_station_hydrobio, date_prelevement, code_indice]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Indice** | `code_indice` | string [PK3] | Code indice (5856=IBD, 5910=IBGN, etc.) |
| | `libelle_indice` | string | Nom complet indice |
| | `resultat_indice` | float | Valeur indice calculé |
| | `unite_indice` | string | Unité indice |
| **Station** | `code_station_hydrobio` | string [PK1] | Code station |
| | `libelle_station_hydrobio` | string | Nom station |
| | `uri_station_hydrobio` | string | URI station |
| | `coordonnee_x` | float | X |
| | `coordonnee_y` | float | Y |
| | `code_projection` | string | Projection |
| | `longitude` | float | Longitude |
| | `latitude` | float | Latitude |
| | `geometry` | GeoJSON | Géométrie |
| **Administratif** | `code_commune` | string | Code commune |
| | `libelle_commune` | string | Nom commune |
| | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |
| | `code_region` | string | Code région |
| | `libelle_region` | string | Nom région |
| **Cours d'Eau** | `code_cours_eau` | string | Code cours d'eau |
| | `libelle_cours_eau` | string | Nom cours d'eau |
| | `uri_cours_eau` | string | URI cours d'eau |
| **Masse d'Eau** | `code_masse_eau` | string | Code masse d'eau |
| | `libelle_masse_eau` | string | Nom masse d'eau |
| | `uri_masse_eau` | string | URI masse d'eau |
| **Bassin** | `code_sous_bassin` | string | Code sous-bassin |
| | `libelle_sous_bassin` | string | Nom sous-bassin |
| | `code_bassin` | string | Code bassin |
| | `libelle_bassin` | string | Nom bassin |
| **Prélèvement** | `date_prelevement` | date [PK2] | Date prélèvement |
| | `code_prelevement` | string | Code prélèvement |
| | `code_operation_prelevement` | string | Code opération |
| | `code_banque_reference` | string | Code banque |
| **Support** | `code_support` | string | Code support |
| | `libelle_support` | string | Libellé support |
| **Qualification** | `code_qualification` | string | Code qualification |
| | `libelle_qualification` | string | Libellé |
| | `code_methode` | string | Méthode calcul |
| | `libelle_methode` | string | Nom méthode |
| | `libelle_accreditation` | string | Accréditation |

**Supports (code_support)** :
- `3` = Eau
- `10` = Diatomées benthiques
- `17` = Macroinvertébrés benthiques
- `19` = Macrophytes
- `20` = Poissons

**Indices Principaux** :
- **IBGN** = Indice Biologique Global Normalisé (macroinvertébrés)
- **IBD** = Indice Biologique Diatomées
- **I2M2** = Indice Invertébrés Multi-Métriques
- **IBMR** = Indice Biologique Macrophytique en Rivière
- **IPR** = Indice Poisson Rivière

---

### 7.3 `/taxons`

**URL** : `https://hubeau.eaufrance.fr/api/v1/hydrobio/taxons`  
**Description** : Taxons biologiques identifiés  
**Total attributs** : 48 | **Primary Keys** : `[code_station_hydrobio, date_prelevement, code_support]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Taxon** | `code_appel_taxon` | string | Code taxon identifié |
| | `libelle_appel_taxon` | string | Nom taxon (ex: "Baetis rhodani") |
| | `codes_taxons_parents` | array[string] | Taxons parents (hiérarchie) |
| | `libelles_taxons_parents` | array[string] | Noms taxons parents |
| | `code_type_resultat` | string | Type résultat |
| | `libelle_type_resultat` | string | Libellé type |
| | `resultat_taxon` | float/int | Résultat (nb individus, densité) |
| **Station** | *(Mêmes champs que /indices)* | | Code station, nom, URI, coordonnées |
| **Administratif** | *(Idem)* | | Commune, département, région |
| **Cours d'Eau** | *(Idem)* | | Code, nom, URI cours d'eau |
| **Masse d'Eau** | *(Idem)* | | Code, nom, URI masse d'eau |
| **Bassin** | *(Idem)* | | Sous-bassin, bassin |
| **Prélèvement** | `date_prelevement` | date [PK2] | Date prélèvement |
| | `code_prelevement` | string | Code prélèvement |
| | `code_operation_prelevement` | string | Code opération |
| | `code_banque_reference` | string | Code banque |
| **Support** | `code_support` | string [PK3] | Code support |
| | `libelle_support` | string | Libellé support |
| **Qualification** | `code_qualification` | string | Code qualification |
| | `libelle_qualification` | string | Libellé |
| | `code_methode` | string | Méthode prélèvement |
| | `libelle_methode` | string | Nom méthode |
| | `libelle_liste_faune_flore` | string | Liste référence utilisée |
| **Caractéristiques** | `code_lot` | string | Code lot échantillon |
| | `hauteur_moyenne_lame_eau` | float | Hauteur lame d'eau (cm) |
| | `largeur_moyenne_lame_eau` | float | Largeur (cm) |
| | `longueur_prospectee` | float | Longueur prospectée (m) |
| **Indices** | `codes_indices_operation` | array[string] | Indices calculés pour opération |

**Groupes Taxonomiques** :
- **Éphémères** (Ephemeroptera)
- **Trichoptères** (Trichoptera)
- **Plécoptères** (Plecoptera)
- **Diptères** (Diptera - Chironomidae)
- **Coléoptères** (Coleoptera)
- **Diatomées** (Bacillariophyta)
- **Macrophytes** (Plantes aquatiques)

---

## 8. Prélèvements

**📍 Base URL** : `https://hubeau.eaufrance.fr/api/v1/prelevements`  
**📖 Documentation** : [API Prélèvements](https://hubeau.eaufrance.fr/page/api-prelevements-eau)  
**🔧 Version** : v1  
**📊 Source** : Portail Naïades

### Description

L'API Prélèvements diffuse les **volumes prélevés** en eau (surface + souterrain).

### Distinction Importante

| Concept | Définition | Usage |
|---------|------------|-------|
| **OUVRAGE** | Installation technique de prélèvement | **Utilisé pour chroniques** |
| **POINT** | Emplacement spécifique sur ouvrage | Métadonnées détaillées |

**⚠️ Clé pour chroniques** : `code_ouvrage` (PAS `code_point_prelevement`)

### Endpoints Intégrés : 3

---

### 8.1 `/referentiel/ouvrages`

**URL** : `https://hubeau.eaufrance.fr/api/v1/prelevements/referentiel/ouvrages`  
**Description** : Ouvrages de prélèvement (infrastructures)  
**Total attributs** : 28 | **Primary Key** : `code_ouvrage` | **Replication** : `date_maj_infos`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_ouvrage` | string [PK] | Code unique ouvrage (ex: "OPR0000606259") |
| | `nom_ouvrage` | string | Nom ouvrage |
| | `uri_ouvrage` | string | URI SANDRE |
| | `id_local_ouvrage` | string | ID local gestionnaire |
| **Localisation** | `longitude` | float | Longitude WGS84 |
| | `latitude` | float | Latitude WGS84 |
| | `code_precision_coord` | string | Précision coordonnées |
| | `libelle_precision_coord` | string | Libellé précision |
| | `geometry` | GeoJSON | Géométrie |
| **Administratif** | `code_commune_insee` | string | Code commune |
| | `nom_commune` | string | Nom commune |
| | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |
| **Type** | `code_type_milieu` | string | Type milieu (1=surface, 2=souterrain) |
| | `libelle_type_milieu` | string | Libellé type |
| **Ressources Eau** | `code_entite_hydro_cours_eau` | string | Cours d'eau prélevé |
| | `uri_entite_hydro_cours_eau` | string | URI cours d'eau |
| | `code_entite_hydro_plan_eau` | string | Plan d'eau |
| | `uri_entite_hydro_plan_eau` | string | URI plan d'eau |
| | `code_mer_ocean` | string | Mer/océan |
| | `ressource_cont_non_referencee` | boolean | Ressource non référencée |
| | `ressource_cont_non_referencee_info` | string | Info ressource |
| **Référence** | `code_point_referent` | string | Point référent |
| **Géologie** | `code_bdlisa` | string | Formation géologique BDLISA |
| | `uri_bdlisa` | string | URI BDLISA |
| **Points** | `codes_points_prelevements` | array[string] | Points rattachés à l'ouvrage |
| **État** | `date_exploitation_debut` | date | Date début exploitation |
| | `date_exploitation_fin` | date | Date fin exploitation |
| | `commentaire` | string | Commentaires |

---

### 8.2 `/referentiel/points_prelevement`

**URL** : `https://hubeau.eaufrance.fr/api/v1/prelevements/referentiel/points_prelevement`  
**Description** : Points de prélèvement (emplacements mesure)  
**Total attributs** : 28 | **Primary Key** : `code_point_prelevement`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Identifiants** | `code_point_prelevement` | string [PK] | Code unique point |
| | `nom_point_prelevement` | string | Nom point |
| | `code_ouvrage` | string [FK] | Code ouvrage parent |
| | `uri_ouvrage` | string | URI ouvrage |
| **Type** | `code_type_milieu` | string | Type milieu |
| | `libelle_type_milieu` | string | Libellé |
| | `code_nature` | string | Nature point (compteur, etc.) |
| | `libelle_nature` | string | Libellé nature |
| **Localisation** | `lieu_dit` | string | Lieu-dit |
| | `code_commune_insee` | string | Code commune |
| | `nom_commune` | string | Nom commune |
| | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |
| **Ressources** | `code_entite_hydro_cours_eau` | string | Cours d'eau |
| | `uri_entite_hydro_cours_eau` | string | URI |
| | `code_entite_hydro_plan_eau` | string | Plan d'eau |
| | `uri_entite_hydro_plan_eau` | string | URI |
| | `code_zone_hydro` | string | Zone hydro |
| | `uri_zone_hydro` | string | URI |
| | `code_mer_ocean` | string | Mer/océan |
| | `nappe_accompagnement` | string | Nappe |
| **Point d'Eau** | `uri_bss_point_eau` | string | URI BSS si forage |
| | `code_bss_point_eau` | string | Code BSS |
| **Géologie** | `code_bdlisa` | string | Code BDLISA |
| | `uri_bdlisa` | string | URI BDLISA |
| **État** | `date_exploitation_debut` | date | Date début |
| | `date_exploitation_fin` | date | Date fin |
| | `commentaire` | string | Commentaires |

**Relation** : 1 Ouvrage → N Points (ex: 1 barrage → 3 points de comptage)

---

### 8.3 `/chroniques`

**URL** : `https://hubeau.eaufrance.fr/api/v1/prelevements/chroniques`  
**Description** : Chroniques de prélèvements (volumes)  
**Total attributs** : 23 | **Primary Keys** : `[code_ouvrage, annee, code_usage]`

| Catégorie | Champ | Type | Description |
|-----------|-------|------|-------------|
| **Ouvrage** | `code_ouvrage` | string [PK1] | Code ouvrage |
| | `nom_ouvrage` | string | Nom ouvrage |
| | `uri_ouvrage` | string | URI ouvrage |
| **Temporel** | `annee` | int [PK2] | Année prélèvement |
| **Usage** | `code_usage` | string [PK3] | Code usage |
| | `libelle_usage` | string | Libellé usage |
| **Volume** | `volume` | float | Volume prélevé (m³) |
| | `code_statut_volume` | string | Statut (PROV, DEF, EST) |
| | `libelle_statut_volume` | string | Libellé statut |
| | `code_qualification_volume` | string | Qualification |
| | `libelle_qualification_volume` | string | Libellé qualification |
| | `code_statut_instruction` | string | Statut instruction administrative |
| | `libelle_statut_instruction` | string | Libellé statut instruction |
| | `code_mode_obtention_volume` | string | Mode obtention |
| | `libelle_mode_obtention_volume` | string | Libellé (compteur, estimation, forfait) |
| **Métadonnées** | `prelevement_ecrasant` | boolean | Prélèvement écrasant un autre |
| | `producteur_donnee` | string | Producteur (Agence Eau, DDT, etc.) |
| **Localisation** | `longitude` | float | Longitude |
| | `latitude` | float | Latitude |
| | `geometry` | GeoJSON | Géométrie |
| | `code_commune_insee` | string | Code commune |
| | `nom_commune` | string | Nom commune |
| | `code_departement` | string | Code département |
| | `libelle_departement` | string | Nom département |

**⚠️ IMPORTANT** : **PAS de champ `mois`** dans cette API !

**Codes Usage** :
- `AEP` = Alimentation en Eau Potable
- `IRR` = Irrigation agricole
- `IND` = Usage industriel
- `ENE` = Production énergie (hydroélectricité)
- `AQU` = Aquaculture/pisciculture
- `AUT` = Autres usages

**Statuts Volume** :
- `PROV` = Provisoire (données préliminaires)
- `DEF` = Définitif (données validées)
- `EST` = Estimé (pas de comptage direct)

---

## 🚫 Limites et Contraintes

### Limites de Pagination

| API | Taille Défaut | Taille Max | Profondeur Max |
|-----|---------------|------------|----------------|
| Hydrométrie v2 | 1000 | 20000 | 20000 |
| Piézométrie | 1000 | 20000 | 20000 |
| Qualité Cours d'Eau | 1000 | 20000 | 20000 |
| Qualité Nappes | 1000 | 20000 | 20000 |
| Température | 1000 | 20000 | 20000 |
| Écoulement | 5000 | 20000 | 20000 |
| **Hydrobiologie** | **2000** | **2000** | **10000** |
| Prélèvements | 1000 | 20000 | 20000 |

**⚠️ Profondeur d'accès** : `(page × size) ≤ max_depth`  
Exemple : page 3 avec size=10000 → 30000 > 20000 ❌ **ERREUR**

**Solution** : Utiliser filtrage (départements, stations, dates) pour réduire le volume

### Limites Temporelles

| API | Endpoint | Restriction |
|-----|----------|-------------|
| Hydrométrie | `/observations_tr` | **30 jours maximum** (non intégré) |
| Hydrométrie | `/obs_elab` | ✅ **Aucune** (historique complet) |
| Autres | Tous | ✅ **Aucune** |

### Taille URL

**Limite** : 2083 caractères maximum  
**Dépassement** : Erreur HTTP 400  
**Solution** : Réduire paramètres ou découper requêtes

### Rate Limiting

**Notre Configuration** :
- **Target** : 5.0 requêtes/seconde
- **Concurrency** : 1 (évite surcharge)
- **Auto-adjust** : Réduction automatique si HTTP 429
- **Backoff** : Exponentiel 2s → 120s max

**Best Practices** :
- Respecter les limites
- Implémenter retry intelligent
- Filtrer au maximum
- Utiliser pagination efficace

---

## 📖 Références

### Documentation Officielle

- [Hub'Eau - Accueil](https://hubeau.eaufrance.fr)
- [API Hydrométrie](https://hubeau.eaufrance.fr/page/api-hydrometrie)
- [API Piézométrie](https://hubeau.eaufrance.fr/page/api-piezometrie)
- [API Qualité Cours d'Eau](https://hubeau.eaufrance.fr/page/api-qualite-cours-deau)
- [API Qualité Nappes](https://hubeau.eaufrance.fr/page/api-qualite-nappes)
- [API Température](https://hubeau.eaufrance.fr/page/api-temperature-continu)
- [API Écoulement](https://hubeau.eaufrance.fr/page/api-ecoulement)
- [API Hydrobiologie](https://hubeau.eaufrance.fr/page/api-hydrobiologie)
- [API Prélèvements](https://hubeau.eaufrance.fr/page/api-prelevements-eau)

### Consoles API (Swagger/OpenAPI)

Testez les APIs interactivement :
- Hydrométrie v2 : `https://hubeau.eaufrance.fr/api/v2/hydrometrie/api-docs`
- Qualité Cours d'Eau v2 : `https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/api-docs`
- Autres : `https://hubeau.eaufrance.fr/api/v1/{api}/api-docs`

### Portails de Données Sources

- **ADES** : Accès aux Données sur les Eaux Souterraines - [ades.eaufrance.fr](https://ades.eaufrance.fr)
- **Naïades** : Système d'Information sur l'Eau - [naiades.eaufrance.fr](https://naiades.eaufrance.fr)
- **SCHAPI** : Service Central d'Hydrométéorologie - [schapi.ecologie.gouv.fr](https://www.schapi.ecologie.gouv.fr)
- **OFB** : Office Français de la Biodiversité - [ofb.gouv.fr](https://www.ofb.gouv.fr)

---

## 📚 Référentiels de Données

### SANDRE - Service d'Administration Nationale des Données et Référentiels sur l'Eau

**📍 Site officiel** : [sandre.eaufrance.fr](https://www.sandre.eaufrance.fr)  
**🏛️ Gestionnaire** : Office Français de la Biodiversité (OFB)

#### Description

Le **SANDRE** est le service de standardisation des données sur l'eau en France. Il administre les référentiels nationaux et dictionnaires de données utilisés par tous les systèmes d'information sur l'eau.

#### Rôle dans Hub'Eau

Le SANDRE fournit les **codes normalisés** utilisés dans toutes les APIs Hub'Eau :

| Domaine | Codes SANDRE | Exemples | Usage dans APIs |
|---------|--------------|----------|-----------------|
| **Paramètres** | Physico-chimie | `1340` = Nitrates, `1335` = Ammonium | Qualité Cours d'Eau, Qualité Nappes |
| **Stations** | Codes stations | Format variable par réseau | Toutes les APIs |
| **Cours d'Eau** | Entités hydro | `V---0000` = Loire, `K---0000` = Rhône | Hydrométrie, Qualité, Température |
| **Masses d'Eau** | DCE | Format `FRXGXXX` | Qualité, Écoulement |
| **Unités** | Mesures | `27` = mg/L, `133` = µg/L | Qualité, Température |
| **Méthodes** | Analytiques | Codes normalisés | Qualité (analyses) |
| **Qualification** | Statuts données | `1` = Bon, `2` = Incertain, `4` = Mauvais | Toutes les chroniques |
| **Supports** | Prélèvement | `3` = Eau, `6` = Sédiment, `17` = Biote | Qualité, Hydrobiologie |
| **Taxons** | Biologie | Codes taxons SANDRE | Hydrobiologie |

#### Nomenclatures Principales

**Codes Qualification (standard toutes APIs)** :
- `1` = Correcte/Bonne
- `2` = Incertaine/Douteuse  
- `4` = Mauvaise
- `16` = Non qualifiée

**Codes Support** :
- `3` = Eau brute
- `6` = Sédiment
- `10` = Diatomées benthiques
- `17` = Macroinvertébrés benthiques
- `19` = Macrophytes
- `20` = Poissons

**Fractions Analysées** :
- `11` = Brute (non filtrée)
- `12` = Dissoute (< 0.45 µm)
- `23` = Particulaire (> 0.45 µm)

#### URI SANDRE

Format : `https://id.eaufrance.fr/sandre/nomenclature/{type}/{code}`

Exemple : `https://id.eaufrance.fr/sandre/nomenclature/PAR/1340` (Paramètre Nitrates)

#### Ressources

- [Référentiels SANDRE](https://www.sandre.eaufrance.fr/atlas/srv/fre/catalog.search#/home)
- [Dictionnaire national des données](https://www.sandre.eaufrance.fr/dictionnaire)
- [Nomenclatures](https://www.sandre.eaufrance.fr/nomenclatures)

---

### BDLISA - Référentiel Hydrogéologique National

**📍 Site officiel** : [bdlisa.eaufrance.fr](https://bdlisa.eaufrance.fr)  
**🏛️ Gestionnaire** : BRGM (Bureau de Recherches Géologiques et Minières)

#### Description

La **BDLISA** (Banque de Données du LIthologique du Sous-sol et des Aquifères) est le référentiel national des **entités hydrogéologiques** françaises. Elle décrit les formations géologiques aquifères et leur organisation spatiale.

#### Rôle dans Hub'Eau

La BDLISA fournit les **codes d'identification des aquifères et formations géologiques** pour :

| API | Champs BDLISA | Description | Format |
|-----|---------------|-------------|--------|
| **Piézométrie** | `codes_bdlisa`, `urns_bdlisa` | Formations traversées par le piézomètre | Array de codes |
| **Qualité Nappes** | `codes_entite_hg_bdlisa`, `urns_bdlisa` | Aquifères surveillés | Array de codes + URNs |
| **Prélèvements** | `code_bdlisa`, `uri_bdlisa` | Aquifère exploité par l'ouvrage | Code unique |

#### Structure des Données BDLISA

**Types d'Entités** :
1. **Entités aquifères** : Formations géologiques productives en eau
2. **Entités non aquifères** : Formations imperméables (aquitards)
3. **Systèmes aquifères** : Ensembles d'entités liées hydrauliquement

**Attributs Principaux** :
- **Code BDLISA** : Identifiant unique national
- **Nom** : Nom géologique (ex: "Calcaires du Dogger du Bassin Parisien")
- **Nature lithologique** : Calcaire, sable, grès, argile, etc.
- **Productivité** : Très productive, productive, peu productive
- **Porosité** : Primaire (intergranulaire) ou secondaire (fissures, karst)

**Exemples de Codes** :
- Craie du Bassin Parisien
- Alluvions de la Loire
- Formations volcaniques d'Auvergne
- Grès du Trias

#### URI BDLISA

Format : `https://id.eaufrance.fr/urn/bdlisa/{code}`

#### Relation avec Masses d'Eau Souterraine

La BDLISA est utilisée pour définir les **masses d'eau souterraine** (Directive Cadre sur l'Eau) :
- 1 masse d'eau = 1 ou plusieurs entités BDLISA
- Les codes masses d'eau (`codes_masse_eau_edl`, `codes_masse_eau_rap`) référencent les formations BDLISA

#### Usages dans l'Ingestion

**Piézométrie `/stations`** :
```json
{
  "code_bss": "08225X0037/F",
  "codes_bdlisa": ["102AB01", "102AB02"],
  "urns_bdlisa": [
    "https://id.eaufrance.fr/urn/bdlisa/102AB01",
    "https://id.eaufrance.fr/urn/bdlisa/102AB02"
  ]
}
```

**Qualité Nappes `/stations`** :
```json
{
  "bss_id": "BSS000XXXX",
  "codes_entite_hg_bdlisa": ["156AA01"],
  "noms_entite_hg_bdlisa": ["Calcaires et marnes du Dogger"],
  "urns_bdlisa": ["https://id.eaufrance.fr/urn/bdlisa/156AA01"]
}
```

#### Ressources

- [Portail BDLISA](https://bdlisa.eaufrance.fr)
- [InfoTerre BRGM](https://infoterre.brgm.fr) - Consultation cartographique
- [Documentation référentiel hydrogéologique](https://www.brgm.fr/fr/reference/referentiel-hydrogeologique-francais-bdlisa)

---

## 🔗 Ressources Complémentaires

### Tutoriels et Guides

- [Tutoriel Hub'Eau](https://hubeau.eaufrance.fr/page/tutoriel) - Guide de démarrage
- [Exemples d'usage](https://hubeau.eaufrance.fr/page/exemples-dusage) - Cas pratiques
- [FAQ](https://hubeau.eaufrance.fr/page/faq) - Questions fréquentes
- [Statistiques 2023](https://hubeau.eaufrance.fr/page/statistiques-2023) - Volumétrie et usage

### Support et Communauté

- [Forum Eau France](https://forum.eaufrance.fr) - Discussions et entraide
- [Contact Hub'Eau](https://hubeau.eaufrance.fr/page/contact) - Support technique

