# Intégration BDLISA

La [BDLISA](https://bdlisa.eaufrance.fr/) (Base de Données des LImites des Systèmes Aquifères) est le référentiel hydrogéologique français. Ce document décrit comment l’intégrer au pipeline (données + nomenclatures).

## Nomenclatures : BDLISA vs Sandre

Le site BDLISA **ne publie pas** les listes de codes/libellés (Niveau, Etat, Nature, Milieu, Thème, Origine) comme pages ou fichiers séparés. Les nomenclatures utilisées sont celles du **Sandre** (dictionnaire PRL, SAQ 2002-1). Les services WFS/WMS BDLISA renvoient des **codes** ; les **libellés** officiels sont dans les seeds dbt `ref_*_eh.csv` (source : [Sandre PRL](https://api.sandre.eaufrance.fr/definitions/v1/dictionnaire/PRL/1.0)).

Voir aussi : `src/dbt_hubeau/seeds/README_NOMENCLATURES_EH.md`.

## Téléchargement des données BDLISA

- **Page officielle** : [bdlisa.eaufrance.fr/telechargement](https://bdlisa.eaufrance.fr/telechargement)
- **Formats** : Geodatabase, Shapefile, SQLite, **GeoPackage**, CSV
- **Périmètres** : Métropole, ou par région

On charge le **GeoPackage** (géométrie PostGIS). Lien direct métropole (V3) :

- `https://reseau.eaufrance.fr/geotraitements/bdlisa/files/telechargement/BDLISA_V3/BDLISA_V3_METRO-gpkg.zip`

## Intégration dans le pipeline

1. **Asset `bdlisa_entites_raw`**  
   Télécharge le ZIP gpkg, charge dans `bronze.bdlisa_entites_raw` (PostGIS avec géométrie), crée la vue `bronze.bdlisa_entites` (schéma fixe pour dbt).

2. **Asset `sandre_nomenclatures_eh`**  
   Charge les nomenclatures Sandre dans `bronze.ref_*_eh`. Plus de seeds CSV.

3. **dbt `stg_tme_entites`**  
   Lit `source('staging', 'bdlisa_entites')` et joint les `source('staging', 'ref_*_eh')`.

## Config

- `configs/bdlisa/bdlisa_entites.yml` : URL du ZIP et options (périmètre, format).

## Références

- [BDLISA – Accueil](https://bdlisa.eaufrance.fr/)
- [BDLISA – Téléchargement](https://bdlisa.eaufrance.fr/telechargement)
- [BDLISA – Services de valorisation (WMS/WFS)](https://bdlisa.eaufrance.fr/decouvrir/les-services-de-valorisation-de-la-bdlisa)
- [Sandre – Dictionnaire PRL](https://api.sandre.eaufrance.fr/definitions/v1/dictionnaire/PRL/1.0)
