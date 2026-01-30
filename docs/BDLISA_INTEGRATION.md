# Intégration TME (Entités Hydrogéologiques)

⚠️ **Note**: BDLISA et les nomenclatures Sandre ont été retirés du pipeline. Seul le référentiel TME est actuellement intégré.

## Référentiel TME

Le pipeline intègre uniquement les **attributs TME (Tableau Multi-Échelles)** qui contiennent les codes et descriptions des entités hydrogéologiques.

### Source des données

**Asset Dagster** : `tme_entites_hydrogeo`

Le pipeline charge le fichier TME depuis les sources suivantes (par ordre de priorité) :

1. **Fichier local TME.csv** (prioritaire) :
   - `TME.csv` à la racine du projet
   - `D:/BDLISA_V3_NATIONAL-csv(1)/CSV/TME.csv`
   - Variable d'environnement `BDLISA_CSV_DIR` (dossier contenant `TME.csv`)

2. **ZIP national BDLISA** (fallback) :
   - URL : `https://reseau.eaufrance.fr/geotraitements/bdlisa/files/telechargement/BDLISA_V3/BDLISA_V3_METRO-gpkg.zip`
   - Extrait le fichier CSV du ZIP si présent

### Tables créées

| Couche | Table | Description |
|--------|-------|-------------|
| **Bronze** | `bronze.tme_entites_hydrogeo` | Données brutes TME (codes EH + attributs) |
| **Silver** | `silver.stg_tme_entites` | TME nettoyé et typé |

### Colonnes TME

Les tables contiennent les colonnes suivantes :

- `code_eh` : Code de l'entité hydrogéologique
- `libelle_eh` : Libellé de l'entité (si disponible)
- `niveau_eh` : Niveau hiérarchique (1=National, 2=Régional, 3=Local)
- `etat_eh` : État de l'entité
- `nature_eh` : Nature de l'entité
- `milieu_eh` : Type de milieu
- `theme_eh` : Thème géologique
- `origine_eh` : Potentialités aquifères

⚠️ **Note**: Les colonnes `libelle_*_eh` et `geometry` peuvent être NULL selon la source de données disponible.

## Utilisation dans le pipeline

Le référentiel TME est utilisé dans :

1. **`int_station_era5_mapping`** (gold) : Enrichit les stations piézométriques avec les métadonnées TME
2. **`hubeau_daily_chroniques`** (gold) : Inclut les colonnes TME pour chaque observation
3. **`dim_piezo_stations`** (gold) : Métadonnées des stations enrichies avec TME

## Vérification

Pour vérifier le chargement du TME :

```sql
-- Compter les entités TME chargées
SELECT COUNT(*) FROM bronze.tme_entites_hydrogeo;

-- Voir un échantillon
SELECT code_eh, libelle_eh, niveau_eh, nature_eh 
FROM bronze.tme_entites_hydrogeo 
LIMIT 10;

-- Vérifier l'utilisation dans gold
SELECT COUNT(DISTINCT code_eh) as nb_entites
FROM gold.hubeau_daily_chroniques
WHERE code_eh IS NOT NULL;
```

## Référence

- [BDLISA – Accueil](https://bdlisa.eaufrance.fr/)
- [BDLISA – Téléchargement](https://bdlisa.eaufrance.fr/telechargement)

## Évolution future

L'intégration complète de BDLISA (géométries) et des nomenclatures Sandre (libellés) pourra être réactivée ultérieurement selon les besoins.
