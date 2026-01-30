# Intégration BDLISA

La [BDLISA](https://bdlisa.eaufrance.fr/) (Base de Données des LImites des Systèmes Aquifères) est le référentiel hydrogéologique français. Ce document décrit comment l’intégrer au pipeline (données + nomenclatures).

## Nomenclatures : BDLISA vs Sandre

Le site BDLISA **ne publie pas** les listes de codes/libellés (Niveau, Etat, Nature, Milieu, Thème, Origine) comme pages ou fichiers séparés. Les nomenclatures officielles viennent du **Sandre** (dictionnaire PRL, SAQ 2002-1).

**Mode actuel (simplifié)** : seules les données TME sont intégrées, **sans** chargement Sandre. Les libellés `libelle_*_eh` ne sont donc pas renseignés.

## Téléchargement des données BDLISA

- **Page officielle** : [bdlisa.eaufrance.fr/telechargement](https://bdlisa.eaufrance.fr/telechargement)
- **Formats** : Geodatabase, Shapefile, SQLite, **GeoPackage**, CSV
- **Périmètres** : Métropole, ou par région

On charge le **GeoPackage** (géométrie PostGIS). Lien direct métropole (V3) :

- `https://reseau.eaufrance.fr/geotraitements/bdlisa/files/telechargement/BDLISA_V3/BDLISA_V3_METRO-gpkg.zip`

## Intégration dans le pipeline (mode simplifié)

1. **Asset `tme_entites_hydrogeo`**  
   Charge le TME (attributs niveau, etat, nature, milieu, theme, origine) dans `bronze.tme_entites_hydrogeo`.  
   Source : fichier local `TME.csv` (prioritaire), puis ZIP BDLISA national, puis ZIP gpkg (si CSV présent).
   Chemins locaux reconnus :
   - `TME.csv` à la racine du repo
   - `D:/BDLISA_V3_NATIONAL-csv(1)/CSV/TME.csv`
   - variable d’environnement `BDLISA_CSV_DIR` (dossier contenant `TME.csv`)

2. **dbt `stg_tme_entites`**  
   Lit **uniquement** `source('staging', 'tme_entites_hydrogeo')`.  
   Pas de jointure Sandre, pas de géométrie BDLISA.

## Alignement des codes : TME (codes EH)

Le pipeline utilise les **codes EH** provenant du TME.  
Vérification rapide :

`SELECT code_eh FROM bronze.tme_entites_hydrogeo WHERE code_eh LIKE '221AA%' LIMIT 5;`

---

## Pourquoi des NULL en silver et gold ? (mode simplifié)

Les valeurs NULL dans les colonnes `libelle_*_eh` en **silver** et **gold** viennent d'une seule cause : **Sandre est désactivé** dans le mode simplifié.

Chaîne :

1. **Source (TME)**  
   `bronze.tme_entites_hydrogeo` contient les codes EH et attributs (niveau, état, nature, ...).

2. **Silver**  
   `stg_tme_entites` reprend les codes et **n’ajoute pas** de libellés Sandre.

3. **Gold**  
   Les marts recopient ces colonnes → les `libelle_*_eh` restent NULL.

**En résumé** : ce n'est pas un bug du pipeline. Les tables `ref_*_eh` en bronze sont bien remplies (code + libelle), mais comme le **fichier BDLISA** ne fournit pas les codes niveau/etat/nature/milieu/theme/origine, on n'a rien à joindre : la source ne les expose pas, donc bronze → silver → gold propagent des NULL pour ces champs.

---

## Géométrie

La géométrie **n’est pas intégrée** en mode simplifié.  
Le GeoPackage BDLISA disponible expose des codes EC (`codeec`) qui ne matchent pas les codes EH du TME.

Pour vérifier les colonnes réellement présentes :

```bash
python scripts/inspect_bdlisa_gpkg.py          # layer 0
python scripts/inspect_bdlisa_gpkg.py --list-layers
```

Dépendances : `geopandas`, `httpx`. L’asset `bdlisa_entites_raw` logue aussi les colonnes et le mapping TME dans Dagster.

## Config

- `configs/bdlisa/bdlisa_entites.yml` : URL du ZIP et options (périmètre, format, `layer_index` / `layer_indexes`).

## Références

- [BDLISA – Accueil](https://bdlisa.eaufrance.fr/)
- [BDLISA – Téléchargement](https://bdlisa.eaufrance.fr/telechargement)
- [BDLISA – Services de valorisation (WMS/WFS)](https://bdlisa.eaufrance.fr/decouvrir/les-services-de-valorisation-de-la-bdlisa)
- [Sandre – Dictionnaire PRL](https://api.sandre.eaufrance.fr/definitions/v1/dictionnaire/PRL/1.0)
