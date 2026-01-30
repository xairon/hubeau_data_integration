# Intégration BDLISA

La [BDLISA](https://bdlisa.eaufrance.fr/) (Base de Données des LImites des Systèmes Aquifères) est le référentiel hydrogéologique français. Ce document décrit comment l’intégrer au pipeline (données + nomenclatures).

## Nomenclatures : BDLISA vs Sandre

Le site BDLISA **ne publie pas** les listes de codes/libellés (Niveau, Etat, Nature, Milieu, Thème, Origine) comme pages ou fichiers séparés. Les nomenclatures utilisées sont celles du **Sandre** (dictionnaire PRL, SAQ 2002-1). Les services WFS/WMS BDLISA renvoient des **codes** ; les **libellés** officiels sont chargés par l’asset `sandre_nomenclatures_eh` depuis l’API Sandre (avec fallback local).

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

3. **Asset `tme_entites_hydrogeo`**  
   Charge le TME (attributs niveau, etat, nature, milieu, theme, origine) dans `bronze.tme_entites_hydrogeo`. Source : **archive BDLISA** (même URL que `bdlisa_entites_raw`, extraction d’un CSV du zip si présent), puis archive CSV nationale BDLISA, puis fallback fichier `TME.csv` local. Fait partie du job **reference_data_bronze** (chargement des référentiels).

4. **dbt `stg_tme_entites`**  
   Lit `source('staging', 'bdlisa_entites')`, enrichit avec `source('staging', 'tme_entites_hydrogeo')` si présent (jointure sur `code_eh`), puis joint les `source('staging', 'ref_*_eh')` pour les libellés.

## Alignement des codes : bdlisa_entites (METRO) vs tme_entites_hydrogeo (TME)

**bdlisa_entites** vient du gpkg **METRO** (Métropole) → codes type `020AB10`, `030AA01`, etc.  
**tme_entites_hydrogeo** vient du TME (zip NATIONAL ou fichier local) → doit contenir **les mêmes codes** pour que la jointure en silver remplisse les colonnes _eh.

- Si **tme_entites_hydrogeo** est chargé depuis le **fichier local** `TME.csv` (souvent Réunion uniquement : 974*, 974AH, …), il n’y a **aucun recouvrement** avec les codes Métropole → toutes les colonnes _eh restent NULL en silver.
- Il faut charger le TME depuis le **zip BDLISA NATIONAL** (`BDLISA_V3_NATIONAL-csv.zip` → `CSV/TME.csv`), qui contient Métropole + DOM. Sur le serveur : lancer l’asset **tme_entites_hydrogeo** (ou le job **reference_data_bronze**) et vérifier dans les logs que la source est bien l’URL du zip NATIONAL et non le fichier local.

Vérification rapide :  
`SELECT code_eh FROM bronze.tme_entites_hydrogeo WHERE code_eh LIKE '020%' LIMIT 5;`  
→ si aucun résultat, la table n’a pas les codes Métropole ; recharger depuis le zip NATIONAL.

---

## Pourquoi des NULL en silver et gold ? (chaîne de causalité)

Les valeurs NULL dans les colonnes `*_eh` (niveau, etat, nature, milieu, theme, origine et leurs `libelle_*_eh`) en **silver** et **gold** viennent d'une seule cause : **le GeoPackage BDLISA V3 Métropole (layer 0) ne contient pas ces champs**.

Chaîne :

1. **Source (fichier)**  
   Le layer 0 du gpkg BDLISA V3 Métropole n'a en pratique que : **code entité**, **libellé**, **géométrie**. Pas de colonnes Niveau, Etat, Nature, Milieu, Thème, Origine.

2. **Bronze**  
   - `bdlisa_entites_raw` : table chargée telle quelle depuis le gpkg → seules les colonnes présentes dans le fichier existent (code, libellé, geometry, etc.).  
   - `bdlisa_entites` (vue) : l'asset détecte les noms de colonnes. Pour chaque attribut (niveau, etat, nature, …), s'il n'existe pas de colonne correspondante dans la table raw, la vue met **NULL** dans la colonne de la vue (ex. `niveau_eh`, `etat_eh`, …).  
   → En bronze, `code_eh` et `libelle_eh` sont renseignés ; niveau_eh, etat_eh, nature_eh, milieu_eh, theme_eh, origine_eh sont **NULL**.

3. **Silver**  
   `stg_tme_entites` lit la vue `bdlisa_entites` et fait des `LEFT JOIN` sur les tables Sandre `ref_*_eh` (ex. `ref_niveau_eh`) avec `base.niveau_eh = ref_niveau.code`.  
   Si `base.niveau_eh` est NULL, la jointure ne matche jamais → `libelle_niveau_eh` (et les autres libellés) restent **NULL**.  
   → En silver, même schéma : seul `code_eh` et `libelle_eh` sont remplis ; le reste des colonnes _eh est **NULL**.

4. **Gold**  
   Les marts (`stations_piezo_carte`, `hubeau_daily_chroniques`, `fct_monthly_chroniques`, `fct_yearly_stats`) utilisent `stg_tme_entites` ou un intermediate qui en dépend. Ils recopient ou agrègent `code_eh`, `libelle_eh`, `libelle_niveau_eh`, etc.  
   → Les colonnes qui sont NULL en silver restent **NULL** en gold.

**En résumé** : ce n'est pas un bug du pipeline. Les tables `ref_*_eh` en bronze sont bien remplies (code + libelle), mais comme le **fichier BDLISA** ne fournit pas les codes niveau/etat/nature/milieu/theme/origine, on n'a rien à joindre : la source ne les expose pas, donc bronze → silver → gold propagent des NULL pour ces champs.

---

## Colonnes TME dans le GeoPackage (layer 0)

Le **layer 0** du GeoPackage BDLISA V3 Métropole n’expose en pratique que **code** et **libellé** (→ `code_eh`, `libelle_eh` dans `bdlisa_entites` / `stg_tme_entites`). Les attributs **niveau, etat, nature, milieu, theme, origine** (et leurs libellés Sandre) sont **absents** de ce layer, donc restent **NULL** dans `stg_tme_entites`.

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
