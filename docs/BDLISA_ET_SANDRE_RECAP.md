# Récapitulatif : BDLISA et Sandre — ce qu’on intègre

Ce document décrit ce que sont BDLISA et Sandre, ce que le pipeline intègre, d’où viennent les données et sous quelle forme.

---

## 1. BDLISA (Base de Données des Limites des Systèmes Aquifères)

### Qu’est-ce que c’est ?

- **Référentiel hydrogéologique national français** : il décrit les entités hydrogéologiques (nappes, unités aquifères, systèmes aquifères) et leurs limites géographiques.
- Porté par le **SIE** (Système d’Information sur l’Eau) ; la BDLISA est hébergée et documentée sur [bdlisa.eaufrance.fr](https://bdlisa.eaufrance.fr/).
- Les entités sont organisées en **Tableau Multi-Échelles (TME)** : une même donnée peut être vue à plusieurs niveaux (national, régional, local) avec des attributs communs (code, libellé, nature, milieu, thème, état, origine, etc.).

### Ce qu’on intègre

| Élément | Description |
|--------|-------------|
| **Données géographiques** | Polygones des entités hydrogéologiques (limites des systèmes/unités aquifères) avec géométrie. |
| **Attributs TME** | Pour chaque entité : `CodeEH`, `LibelleEH`, `NiveauEH`, `NatureEH`, `MilieuEH`, `ThemeEH`, `EtatEH`, `OrigineEH`, etc. (noms réels selon le jeu BDLISA). |

### D’où ça vient ?

- **Source** : téléchargement officiel Eaufrance.
- **URL par défaut** :  
  `https://reseau.eaufrance.fr/geotraitements/bdlisa/files/telechargement/BDLISA_V3/BDLISA_V3_METRO-gpkg.zip`
- **Format** : un **ZIP** contenant un **GeoPackage** (`.gpkg`) — un seul fichier par défaut, un seul layer chargé (index configurable, souvent les entités NV3 ou fusion).
- **Périmètre** : **métropole** par défaut ; des ZIP par région (ex. La Réunion) sont disponibles sur [bdlisa.eaufrance.fr/telechargement](https://bdlisa.eaufrance.fr/telechargement).
- **Config** : `configs/bdlisa/bdlisa_entites.yml` — `perimeters` (liste `{code, url}`), `layer_indexes` (ex. `[0, 1, 2]` pour NV1/NV2/NV3), `extraction.schema`, `table`.

### Sous quelle forme dans le pipeline ?

1. **Asset Dagster** `bdlisa_entites_raw` :
   - Pour chaque **périmètre** configuré (ex. METRO, ARA), télécharge le ZIP, extrait le `.gpkg`, lit les **couches** indiquées par `layer_indexes` (ex. 0, 1, 2 = NV1/NV2/NV3) avec **GeoPandas** (moteur **pyogrio**).
   - Ajoute les colonnes `perimeter` (code du périmètre) et `niveau_layer` (index de la couche) à chaque GeoDataFrame.
   - Concatène et charge dans PostgreSQL/PostGIS : **table** `bronze.bdlisa_entites_raw` (géométrie WGS84).
   - Crée une **vue** `bronze.bdlisa_entites` avec schéma fixe pour dbt : `code_eh`, `libelle_eh`, `perimeter`, `niveau_layer`, `niveau_eh`, `etat_eh`, `nature_eh`, `milieu_eh`, `theme_eh`, `origine_eh`, `geometry`.
2. **Fallback** : si le ZIP ne contient pas de `.gpkg` mais du CSV, chargement en table sans géométrie (colonnes texte).

Les colonnes BDLISA (CodeEH, LibelleEH, NatureEH, etc.) contiennent des **codes** ; les **libellés** officiels pour ces codes viennent du **Sandre** (voir ci‑dessous).

---

## 2. Sandre (Service d’administration nationale des données et référentiels sur l’eau)

### Qu’est-ce que c’est ?

- **Service national** qui définit et diffuse le **langage commun** des données sur l’eau en France : nomenclatures, dictionnaires, référentiels (codes + libellés).
- Les **nomenclatures** sont des listes de codes (`CdElement`) et libellés (`LbElement`) : paramètres, stations, **entités hydrogéologiques**, etc.
- Pour la BDLISA / TME, le dictionnaire de données utilisé est le **dictionnaire Hydrogéologie (SAQ)** ; les nomenclatures concernées sont identifiées par un **numéro** (ex. 339, 338, 348, 349, 341).

### Ce qu’on intègre

On n’intègre **pas** la BDLISA depuis le Sandre ; on intègre les **nomenclatures Sandre** qui permettent de passer des **codes** des colonnes TME (NatureEH, MilieuEH, ThemeEH, EtatEH, OrigineEH) aux **libellés** officiels.

| Colonne TME (BDLISA) | Libellé de la donnée | Nomenclature Sandre n° | Table bronze |
|----------------------|----------------------|-------------------------|--------------|
| **NatureEH** | Nature de l’entité hydrogéologique | **339** | `ref_nature_eh` |
| **MilieuEH** | Milieu de l’entité hydrogéologique | **338** | `ref_milieu_eh` |
| **ThemeEH** | Thème de l’entité hydrogéologique | **348** | `ref_theme_eh` |
| **EtatEH** | État de l’entité hydrogéologique | **349** | `ref_etat_eh` |
| **OrigineEH** | Origine de l’entité hydrogéologique | **341** | `ref_origine_eh` |
| **NiveauEH** | Niveau hiérarchique (1=National, 2=Régional, 3=Local) | — (pas de nomenclature NSA dédiée) | `ref_niveau_eh` (fallback en dur) |

Chaque table `bronze.ref_*_eh` a deux colonnes : **`code`** (ex. `"1"`) et **`libelle`** (ex. `"Grand système aquifère"`).

### D’où ça vient ?

- **Source** : API Sandre « Référentiels » v1, jeu **NSA** (nomenclatures).
- **URL** : `https://api.sandre.eaufrance.fr/referentiels/v1/nsa.json`  
  Ce fichier contient **toutes** les nomenclatures ; on filtre côté code par **CdReferentiel** (339, 338, 348, 349, 341) pour extraire les éléments (CdElement, LbElement) et remplir les tables `ref_*_eh`.
- **Alternative documentée** (par nomenclature) :  
  `https://api.sandre.eaufrance.fr/referentiels/v1/nomenclatures/{NUMERO}` (ex. 348 pour le thème).
- En cas d’indisponibilité de l’API : **fallback** avec des listes codées en dur dans `src/hubeau_pipeline/assets/bronze/_sandre_fallback.py` (source : dictionnaire SAQ / PRL).

### Sous quelle forme dans le pipeline ?

1. **Asset Dagster** `sandre_nomenclatures_eh` :
   - Appel HTTP GET sur `nsa.json`, parsing du JSON, extraction des référentiels 339, 338, 348, 349, 341.
   - Pour chaque nomenclature : création / remplacement des tables `bronze.ref_nature_eh`, `ref_milieu_eh`, `ref_theme_eh`, `ref_etat_eh`, `ref_origine_eh` (colonnes `code`, `libelle`).
   - **ref_niveau_eh** : toujours remplie par le fallback en dur (1=National, 2=Régional, 3=Local).
   - Si l’API échoue ou ne renvoie pas ces référentiels : toutes les tables `ref_*_eh` sont alimentées depuis `_sandre_fallback.py`.

---

## 3. Synthèse : flux d’intégration

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ BDLISA                                                                       │
│ • URL : reseau.eaufrance.fr/.../BDLISA_V3_METRO-gpkg.zip                    │
│ • Format : ZIP → GeoPackage (.gpkg) → PostGIS                                │
│ • Asset : bdlisa_entites_raw                                                 │
│ • Sortie : bronze.bdlisa_entites_raw (table) + bronze.bdlisa_entites (vue)     │
│   Colonnes : code_eh, libelle_eh, niveau_eh, nature_eh, milieu_eh,          │
│              theme_eh, etat_eh, origine_eh, geometry                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Sandre                                                                       │
│ • URL : api.sandre.eaufrance.fr/referentiels/v1/nsa.json                    │
│ • Format : JSON (nomenclatures NSA) → tables code/libelle                   │
│ • Asset : sandre_nomenclatures_eh                                            │
│ • Sortie : bronze.ref_nature_eh, ref_milieu_eh, ref_theme_eh,               │
│            ref_etat_eh, ref_origine_eh, ref_niveau_eh (fallback)            │
│   N° Sandre : 339, 338, 348, 349, 341 ; NiveauEH en dur                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ dbt : stg_tme_entites                                                        │
│ • Lit : source('staging', 'bdlisa_entites')                                   │
│ • Joint : source('staging', 'ref_niveau_eh'), ref_etat_eh, ref_nature_eh,   │
│           ref_milieu_eh, ref_theme_eh, ref_origine_eh) sur code = *_eh      │
│ • Sortie : silver.stg_tme_entites (entités + libellés Sandre + geometry)     │
└─────────────────────────────────────────────────────────────────────────────┘
```

- **Bronze** : données brutes BDLISA (table + vue) + nomenclatures Sandre (6 tables ref_*_eh).
- **Silver** : `stg_tme_entites` = TME avec codes **et** libellés (jointures aux ref_*_eh), prêt pour cartes (Superset) et marts (ex. `gold.stations_piezo_carte`).

**Note** : Le layer 0 du GeoPackage BDLISA V3 Métropole n’expose souvent que **code** et **libellé**. Les colonnes niveau, etat, nature, milieu, theme, origine (et leurs libellés) restent alors **NULL** dans `stg_tme_entites`. Voir `scripts/inspect_bdlisa_gpkg.py` et [BDLISA_INTEGRATION.md](BDLISA_INTEGRATION.md).

---

## 4. Rôle du TME (Tableau Multi-Échelles)

- Le **TME** est la structure « cœur » de la BDLISA : une entité = une ligne avec code, libellé, niveau (1/2/3), nature, milieu, thème, état, origine, (optionnel) ordre stratigraphique, parent (InclusEH), etc.
- Dans le pipeline, la **table / vue qui joue le rôle de TME** est :
  - en bronze : **`bronze.bdlisa_entites`** (vue sur `bdlisa_entites_raw` avec schéma fixe) ;
  - en silver : **`silver.stg_tme_entites`** (TME + libellés Sandre).
- Les colonnes **NatureEH, MilieuEH, ThemeEH, EtatEH, OrigineEH** sont des **codes** ; les **libellés** viennent des tables Sandre `ref_*_eh` via les jointures dans `stg_tme_entites`.

---

## 5. Fichiers et config impliqués

| Rôle | Fichier / ressource |
|------|----------------------|
| Config BDLISA | `configs/bdlisa/bdlisa_entites.yml` (perimeters, layer_indexes, schema, table) |
| Asset BDLISA | `src/hubeau_pipeline/assets/bronze/bdlisa_assets.py` |
| Asset Sandre | `src/hubeau_pipeline/assets/bronze/sandre_nomenclatures_assets.py` |
| Fallback Sandre | `src/hubeau_pipeline/assets/bronze/_sandre_fallback.py` |
| Référentiels géo | `src/hubeau_pipeline/assets/bronze/referentiel_geo_assets.py` (régions, départements, zones hydro) |
| Sources dbt | `src/dbt_hubeau/models/staging/sources.yml` (staging = schema bronze, bdlisa_entites, ref_*_eh, referentiel_regions, referentiel_departements, referentiel_zones_hydro) |
| Modèle TME | `src/dbt_hubeau/models/staging/stg_tme_entites.sql` |
| Doc intégration | `docs/BDLISA_INTEGRATION.md` |

---

## 6. En résumé

| | BDLISA | Sandre |
|---|--------|--------|
| **Rôle** | Référentiel hydrogéologique national (entités + géométrie) | Nomenclatures officielles (codes → libellés) pour les colonnes TME |
| **On intègre** | Données géo + attributs TME (codes) | Listes code/libellé pour Nature, Milieu, Thème, État, Origine (et Niveau en dur) |
| **Source** | ZIP GeoPackage (reseau.eaufrance.fr) | API nsa.json (api.sandre.eaufrance.fr) |
| **Forme** | Table PostGIS + vue bronze | 6 tables bronze `ref_*_eh` |
| **Utilisation** | Carte des nappes, jointure stations → entité, marts gold | Jointures dans `stg_tme_entites` pour afficher les libellés au lieu des seuls codes |

Sans Sandre, on aurait les **codes** BDLISA (ex. `nature_eh = '1'`) mais pas les **libellés** officiels (« Grand système aquifère »). Les nomenclatures Sandre fournissent ces libellés et permettent un affichage cohérent dans les cartes et rapports.

---

## 7. Multi-périmètres, multi-couches et référentiels géographiques

### BDLISA : plusieurs périmètres et plusieurs couches

- **Périmètres** : la config `configs/bdlisa/bdlisa_entites.yml` accepte une liste **`perimeters`** (`{code, url}`). On peut charger **métropole** (METRO) et **régions** (ex. ARA, BFC, …) en une seule table ; chaque ligne a une colonne **`perimeter`** (code du périmètre).
- **Couches** : **`layer_indexes`** (ex. `[0, 1, 2]`) permet de charger plusieurs layers du GeoPackage (NV1, NV2, NV3). Chaque ligne a une colonne **`niveau_layer`** (index de la couche).
- **Sortie** : une seule table `bronze.bdlisa_entites_raw` et une vue `bronze.bdlisa_entites` avec `perimeter`, `niveau_layer`, prêtes pour filtres et calques dans Superset.

### Référentiels géographiques (calques Superset)

- **Régions** : `bronze.referentiel_regions` — contours des régions (data.gouv.fr, GeoJSON généralisé 1000 m). Colonnes : `code`, `nom`, `geometry`.
- **Départements** : `bronze.referentiel_departements` — contours des départements (data.gouv.fr, GeoJSON 1000 m). Colonnes : `code`, `nom`, `geometry`.
- **Zones hydrographiques** : `bronze.referentiel_zones_hydro` — zones BD Carthage (Sandre, Shapefile métropole). Colonne géométrie : `geometry`.

Ces tables sont chargées par les assets Dagster **`referentiel_regions`**, **`referentiel_departements`**, **`referentiel_zones_hydro`** (voir `src/hubeau_pipeline/assets/bronze/referentiel_geo_assets.py`). Elles sont incluses dans le job **`reference_data_bronze`** avec BDLISA et Sandre, et exposées dans **Superset** comme calques (filtres par région, département, zone hydro).
