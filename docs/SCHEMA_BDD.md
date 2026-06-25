# Schéma Base de Données

Structure des tables PostgreSQL du pipeline Hub'Eau.

## Architecture

```
Hub'Eau APIs ──┐
               ├──▶ DLT ──▶ bronze.* ──▶ dbt ──▶ silver.* ──▶ dbt ──▶ gold.*
ERA5 API ──────┘
```

## Schémas

| Schéma | Gestion | Contenu |
|--------|---------|---------|
| `bronze` | DLT + assets Dagster | Tables brutes (`*_raw`) + TME (`tme_entites_hydrogeo`) |
| `silver` | dbt staging | Tables nettoyées (`stg_*`) |
| `silver_rejects` | dbt rejects | Lignes filtrées (exceptions) avec `rejection_reason` — audit, qualité |
| `gold` | dbt (intermediate + marts) + assets Dagster (`indices`) + seeds dbt | Tables transformées (`int_*` + marts), indices standardisés (réf. fixe) et référentiels |

## Optimisations TimescaleDB

Les tables suivantes sont converties en **Hypertables** (PK incluant la colonne temps, puis `create_hypertable`) :
- **Silver** : `stg_era5_timeseries`
- **Marts** : `hubeau_daily_chroniques`, `hydro_daily_chroniques`

**Compression** (chunks anciens compressés) : `stg_era5_timeseries` (90 j), `hubeau_daily_chroniques` (365 j), `hydro_daily_chroniques` (365 j).

## PostGIS

- **Géométries** : `make_point(longitude, latitude)` → `geometry(Point, 4326)` (WGS84). Index **GIST** sur toutes les colonnes `geometry` / `geom`.
- **Distances** : utiliser `::geography` pour des mètres exacts : `ST_Distance(geom::geography, ...)`.
- **KNN** : l'opérateur `<->` s'appuie sur l'index GIST (ex. plus proche point ERA5 dans `int_station_era5_mapping`).

## Index (silver / gold)

- **Temps** : index **BRIN** sur les colonnes de temps (`date_mesure`, `date`, `time`, `era5_date`, `mois`, `annee`) pour les requêtes par plage.
- **Clés** : index B-tree sur `code_bss`, `code_site`, `code_station`, `(code_bss, date)`, etc.
- **Spatial** : index GIST sur `geometry` / `geom` (voir PostGIS ci-dessus).

---

## Tables Bronze (DLT + Assets Dagster)

Tables créées automatiquement par DLT et assets Dagster au premier run.

### Piézométrie

| Table | Description | Volume estimé |
|-------|-------------|---------------|
| `piezometry_stations_raw` | Stations BSS (piézométrie) | ~23k |
| `piezometry_chroniques_raw` | Mesures niveaux nappes | ~23M |

**Colonnes principales** :
- `piezometry_stations_raw` : `code_bss`, `x`, `y`, `nom_commune`, `code_departement`, etc.
- `piezometry_chroniques_raw` : `code_bss`, `date_mesure`, `niveau_nappe_eau`, `profondeur_nappe`, etc.

### Hydrométrie

| Table | Description | Volume estimé |
|-------|-------------|---------------|
| `hydrometry_sites_raw` | Sites hydrométriques | ~5k |
| `hydrometry_stations_raw` | Stations hydrométriques | ~5k |
| `hydrometry_obs_elab_raw` | Observations élaborées | ~15M |

**Colonnes principales** :
- `hydrometry_stations_raw` : `code_station`, `x`, `y`, `code_entite`, etc.
- `hydrometry_obs_elab_raw` : `code_entite`, `date_obs_elab`, `resultat_obs_elab`, etc.

### ERA5 (Copernicus)

| Table | Description | Volume estimé |
|-------|-------------|---------------|
| `era5_france_timeseries` | Time series extraites (Direct-to-Timeseries) | ~300M |

**Colonnes principales** :
- `era5_france_timeseries` : `time`, `latitude`, `longitude`, `temperature_2m`, `total_precipitation`, `potential_evaporation`

### Référentiel TME (Asset Dagster)

| Table | Description | Volume |
|-------|-------------|--------|
| `tme_entites_hydrogeo` | Entités hydrogéologiques (TME) | ~2k |

**Source** : Asset Dagster `tme_entites_hydrogeo` (TME.csv local prioritaire, sinon ZIP national)

**Colonnes** : `code_eh`, `libelle_eh`, `niveau_eh`, `etat_eh`, `nature_eh`, `milieu_eh`, `theme_eh`, `origine_eh`

### Métadonnées DLT

| Table | Description |
|-------|-------------|
| `_dlt_loads` | Historique des chargements |
| `_dlt_pipeline_state` | État des pipelines |

---

## Tables Silver (dbt staging)

Tables nettoyées et standardisées depuis bronze.

| Table | Description | Source | Filtres |
|-------|-------------|--------|---------|
| `stg_piezo_chroniques` | Chroniques piézo nettoyées | `bronze.piezometry_chroniques_raw` | NULL filtrés |
| `stg_piezo_stations` | Stations piézo nettoyées | `bronze.piezometry_stations_raw` | Coordonnées non-nulles |
| `stg_hydrometry_stations` | Stations hydro nettoyées | `bronze.hydrometry_stations_raw` | Coordonnées non-nulles |
| `stg_hydrometry_sites` | Sites hydro nettoyés | `bronze.hydrometry_sites_raw` | - |
| `stg_hydrometry_obs_elab` | Observations hydro nettoyées | `bronze.hydrometry_obs_elab_raw` | Observations non-nulles |
| `stg_era5_timeseries` | Time series ERA5 nettoyées | `bronze.era5_france_timeseries` | Observations non-nulles |
| `stg_tme_entites` | TME nettoyé | `bronze.tme_entites_hydrogeo` | Typage et normalisation minimale |

**Transformations appliquées** :
- Type casting (VARCHAR → NUMERIC, DATE, etc.)
- Renommage colonnes (standardisation)
- Filtrage des valeurs NULL
- Nettoyage léger (trim)
- Création de géométries PostGIS (`geometry` / `geom`)

---

## Tables de rejet (silver_rejects)

**Bonnes pratiques** : les lignes exclues en silver (mesure nulle, clé manquante, etc.) ne sont pas supprimées sans trace. Elles sont écrites dans des tables **rejet** dans le schéma `silver_rejects`, avec une colonne `rejection_reason` pour l'audit et la qualité.

| Table | Source | Motifs de rejet (exemples) |
|-------|--------|----------------------------|
| `stg_piezo_chroniques_rejected` | `piezometry_chroniques_raw` | `DATE_MESURE_NULL`, `CODE_BSS_NULL`, `NIVEAU_NAPPE_NULL`, `PROFONDEUR_NAPPE_NULL` |
| `stg_hydrometry_stations_rejected` | `hydrometry_stations_raw` | `CODE_SITE_NOT_IN_SITES`, `CODE_STATION_NULL`, `COORDS_NULL` |
| `stg_hydrometry_obs_elab_rejected` | `hydrometry_obs_elab_raw` | `CODE_SITE_NOT_IN_SITES`, `DATE_OBS_ELAB_NULL`, `CODE_SITE_NULL`, `GRANDEUR_HYDRO_NULL`, `RESULTAT_OBS_NULL` |

Voir `src/dbt_hubeau/models/rejects/README.md` pour les requêtes utiles.

---

## Tables Gold (dbt intermediate + marts)

Tables transformées et prêtes pour l'analyse.

### Intermediate (Piézométrie + Hydrométrie)

| Table | Description | Source |
|-------|-------------|--------|
| `int_daily_measurements` | Mesures quotidiennes agrégées (piézo) | `silver.stg_piezo_chroniques` |
| `int_station_era5_mapping` | Mapping stations piézo → grille ERA5 + métadonnées TME | `silver.stg_piezo_stations` + `silver.stg_tme_entites` |
| `int_era5_grid_points` | Points de grille ERA5 uniques (pour jointure spatiale) | `silver.stg_era5_timeseries` |
| `int_era5_for_all_stations` | ERA5 filtré pour les points de grille utilisés par toutes les stations (piézo + hydro) | `silver.stg_era5_timeseries` + mappings |
| `int_hydro_daily_measurements` | Mesures quotidiennes agrégées (hydrométrie) | `silver.stg_hydrometry_obs_elab` |
| `int_hydro_station_era5_mapping` | Mapping stations hydrométriques → grille ERA5 | `silver.stg_hydrometry_stations` |

**Détails** :
- `int_daily_measurements` : Agrégation par `code_bss` et `date_mesure` (AVG)
- `int_station_era5_mapping` : Mapping spatial + jointure avec TME
- `int_era5_for_all_stations` : Filtrage ERA5 sur l'union des points piézo + hydro (une seule table au lieu de deux)
- `int_hydro_daily_measurements` : Agrégation par `code_station`, `date_obs_elab`, `grandeur_hydro_elab`
- `int_hydro_station_era5_mapping` : Mapping spatial + métadonnées station/site

### Marts (Piézométrie)

#### `hubeau_daily_chroniques`

**Table fact principale** : Combine piézométrie + météo ERA5 + métadonnées TME.

| Colonne | Type | Description |
|---------|------|-------------|
| `code_bss` | VARCHAR | ID station BSS |
| `date` | DATE | Date mesure |
| **Observations** | | |
| `niveau_nappe_eau` | NUMERIC | Niveau nappe (m) - **NON NULL** |
| `profondeur_nappe` | NUMERIC | Profondeur (m) - **NON NULL** |
| `temperature_2m` | NUMERIC | Température ERA5 (°C) - **NON NULL** |
| `total_precipitation` | NUMERIC | Précipitations ERA5 (mm) - **NON NULL** |
| `potential_evaporation` | NUMERIC | Évaporation ERA5 (mm) - **NON NULL** |
| **Métadonnées station** | | |
| `codes_bdlisa` | VARCHAR | Codes TME |
| `code_commune_insee` | VARCHAR | Code INSEE |
| `nom_commune` | VARCHAR | Nom commune |
| `altitude_station` | NUMERIC | Altitude (m) |
| `code_departement` | VARCHAR | Code département |
| `nom_departement` | VARCHAR | Nom département |
| **Métadonnées TME** | | |
| `code_eh` | VARCHAR | Code entité hydrogéologique |
| `libelle_eh` | VARCHAR | Libellé EH |
| `niveau_eh` | VARCHAR | Niveau EH |
| `etat_eh` | VARCHAR | État EH |
| `nature_eh` | VARCHAR | Nature EH |
| `milieu_eh` | VARCHAR | Milieu EH |
| `theme_eh` | VARCHAR | Thème EH |
| `origine_eh` | VARCHAR | Origine EH |
| **Coordonnées** | | |
| `station_latitude` | NUMERIC | Latitude station réelle |
| `station_longitude` | NUMERIC | Longitude station réelle |
| `era5_latitude` | NUMERIC | Latitude point grille ERA5 |
| `era5_longitude` | NUMERIC | Longitude point grille ERA5 |

**Optimisations** :
- **Hypertable (1 an)** + Compression active
- Index sur `(code_bss, date)`, `(date)`, `(code_departement)`, `(code_eh)`

#### `fct_monthly_chroniques`

**Granularité** : Station x Mois

Agrégats mensuels :
- Moyennes, Min, Max, Écart-type
- Variations : vs mois précédent, vs année précédente
- Moyennes mobiles : 3 mois, 12 mois
- Table plain avec `delete+insert` incrémental (25 mois lookback)

#### `fct_yearly_stats`

**Granularité** : Station x Année

Agrégats annuels :
- Moyennes annuelles, Bilan hydrique
- Percentiles historiques
- Classification annuelle : `TRES_BAS`, `BAS`, `NORMAL`, `HAUT`, `TRES_HAUT`
- Table plain avec `delete+insert` incrémental

#### `dim_piezo_stations`

**Granularité** : Station

Table dimensionnelle enrichie :
- Statistiques globales (Date début/fin, nb mesures)
- Indicateurs techniques (niveau moyen, amplitude, tendance)
- Niveau d'alerte : `NORMAL`, `VIGILANCE`, `ALERTE`
- Qualité de tendance : `FIABLE`, `INDICATIVE`, `FAIBLE`, `NON_CALCULEE`

#### `stations_piezo_carte`

**Granularité** : Station

Mart "prêt carte" pour Superset :
- Une ligne par station avec géométrie PostGIS
- Libellés TME enrichis
- Indicateurs d'alerte et tendance
- **Optimisé pour les visualisations cartographiques**

### Marts (Hydrométrie)

#### `hydro_daily_chroniques`

**Table fact principale hydrométrie** : combine observations hydrométriques + météo ERA5.

- **Granularité** : Station × Jour × Grandeur
- Colonnes principales : `code_station`, `code_site`, `date`, `grandeur_hydro_elab`, `resultat_obs_elab`
- Métadonnées station/site + météo ERA5 intégrées
- **Hypertable (1 an)** + Compression active

#### `fct_monthly_hydro`

**Granularité** : Station × Mois × Grandeur

- Agrégats mensuels (moyenne, min, max, stddev)
- Moyennes mobiles 3/12 mois
- Variations vs mois précédent et vs année précédente
- Table plain avec `delete+insert` incrémental (25 mois lookback)

#### `fct_yearly_hydro`

**Granularité** : Station × Année × Grandeur

- Agrégats annuels + percentiles historiques
- Classification annuelle : `TRES_BAS`, `BAS`, `NORMAL`, `HAUT`, `TRES_HAUT`

#### `dim_hydro_stations`

**Granularité** : Station

Table dimensionnelle enrichie :
- Métadonnées stations hydrométriques
- Géométrie PostGIS
- Statut station (`ACTIVE` / `FERMEE`)
- Statistiques globales et indicateurs (grandeur principale)

#### `stations_hydro_carte`

**Granularité** : Station

Mart "prêt carte" pour Superset :
- Une ligne par station avec géométrie PostGIS
- Indicateurs hydrométriques principaux
- **Optimisé pour les visualisations cartographiques**

### Dimensions transverses

#### `dim_date`

**Granularité** : Jour

Dimension temps construite à partir des faits piézo et hydro :
- `year`, `quarter`, `month`, `week`, `day_of_year`, `iso_day_of_week`
- Flag `is_weekend`

#### `dim_geography`

Dimension géographique consolidée depuis les stations piézo et hydro :
- `code_commune`, `nom_commune`
- `code_departement`, `nom_departement`
- `code_region`, `nom_region`

---

## Tables Gold — Indices standardisés (assets Dagster, groupe `indices`)

Ces tables **ne sont pas produites par dbt** mais par des **assets Dagster** Python
(`src/hubeau_pipeline/assets/*_index_assets.py`, `reference_stats_assets.py`, groupe
`indices`). Toute la méthode scientifique est centralisée dans un seul module pur
`src/hubeau_pipeline/ml/indices.py` (fonctions `compute_reference_grid`,
`grid_to_zscore`, `classify_value`).

### Objectif

Fournir l'**indice piézométrique/hydrologique standardisé** (IPS/SPLI pour les nappes,
SSFI pour les débits) — la position du niveau d'un mois donné par rapport à sa
**normale saisonnière**, exprimée en z-score et classée en 7 classes BSH/Météo-France.
C'est la donnée affichée par l'app (Observatoire, carte, fiche station, secteurs) et le
bulletin « Météo des nappes ». **L'app ne recalcule plus ces indices : elle lit ces
tables.**

### Méthodologie (commune aux 3 tables)

| Étape | Détail |
|-------|--------|
| **Référence** | Fenêtre **fixe** `REF_PERIOD = (1991, 2020)` (normale climatologique WMO/BRGM). Pas de fenêtre glissante. |
| **Repli par station** | Échelle de qualité via `_select_reference_window` : `normale` (1991–2020, ≥15 ans) → `adaptee` (meilleure fenêtre 30 ans alignée décennie, ≥15 ans) → `provisoire` (historique complet, <15 ans). Le `flag` porte cette qualité. |
| **Grille** | Par mois calendaire (1–12) : percentiles empiriques 1→99 (`PCTL_GRID`) sur les valeurs de la fenêtre. Mois à `< MIN_PER_MONTH = 10` obs → interpolé depuis le mois voisin (circulaire) ; aucun mois exploitable → grille `NULL` ⇒ indice `UNKNOWN` (**jamais de grille fabriquée**). |
| **Z-score** | `grid_to_zscore` : rang-percentile de la valeur dans la grille → `clip(0.001, 0.999)` → `norm.ppf` → arrondi 3 décimales. CDF empirique projetée sur la loi normale standard. |
| **Classes (7)** | Seuils z `[-1.75, -1.28, -0.84, 0.84, 1.28, 1.75]` → `EXTREMEMENT_BAS, TRES_BAS, BAS, NORMAL, HAUT, TRES_HAUT, EXTREMEMENT_HAUT` (+ `UNKNOWN`). Équivalents en percentiles : `[4.01, 10.03, 20.05, 79.95, 89.97, 95.99]`. |
| **Source piézo** | `gold.fct_monthly_chroniques.niveau_moyen` (m NGF), par `code_bss`. |
| **Source hydro** | `gold.fct_monthly_hydro.resultat_moyen` (débit), par `code_station`, `positive_only=true` (débits ≤ 0 écartés). |

> ⚠️ **Cohérence cross-repo** : la même fonction `compute_reference_grid` est utilisée
> par les 3 assets, et son portage `dashboard/utils/reference.py` (`value_to_zscore`)
> dans `time-serie-explo` est **identique** (même grille, mêmes clips, mêmes seuils).
> Entrepôt et app ne peuvent donc pas diverger sur la méthode.

#### `station_reference_stats`

**Granularité** : (type, station, mois calendaire) — 12 lignes par station.
**Objectif** : la **grille de référence fixe** réutilisable (normale saisonnière figée).
**Asset** : `station_reference_stats` — **pas de recalcul nocturne**, rematérialisé
seulement à chaque décennie (1991–2020 → 2001–2030 en 2031).

| Colonne | Type | Description |
|---------|------|-------------|
| `type` | text | `piezo` / `hydro` |
| `code` | text | `code_bss` (piézo) ou `code_station` (hydro) |
| `month` | int | Mois calendaire 1–12 |
| `quantile_grid` | jsonb | 99 percentiles (m NGF ou débit), `NULL` si insuffisant |
| `baseline_start` / `baseline_end` | date | Bornes de la fenêtre de référence retenue |
| `flag` | text | `normale` / `adaptee` / `provisoire` |
| `n_years` | int | Nb d'années de la fenêtre |
| `computed_at` | timestamptz | Horodatage |

PK `(type, code, month)`. **Source** : `gold.fct_monthly_chroniques` + `gold.fct_monthly_hydro`.

#### `fct_monthly_index`

**Granularité** : (type, station, mois) — **série mensuelle complète, 1967 → mois courant**.
**Objectif** : l'**historique standardisé** d'une station (courbe SPLI/IPS ou SSFI, timeline secteurs).
**Asset** : `fct_monthly_index` (deps `station_reference_stats`) — **nocturne**. Re-score
chaque mois de l'historique contre la grille fixe.

| Colonne | Type | Description |
|---------|------|-------------|
| `type` | text | `piezo` / `hydro` |
| `code` | text | Code station |
| `month` | date | 1er du mois |
| `z` | double | Indice standardisé (z-score), `NULL` si pas de grille |
| `index_class` | text | Une des 7 classes (ou `UNKNOWN`) |
| `flag` | text | Qualité de la référence (`normale`/`adaptee`/`provisoire`) |
| `computed_at` | timestamptz | Horodatage |

PK `(type, code, month)`, index `(type, month)`. **Source** : mêmes faits mensuels.
**Consommée par l'app** : timeline secteurs, situation passée (`observatory_situation.py`,
`observatory_common.py`). *(Les endpoints fiche station `/spli` et `/ssfi` recalculent
encore à la volée et devraient à terme lire cette table.)*

#### `station_current_index`

**Granularité** : (type, station) — **1 ligne par station = dernier mois disponible**.
**Objectif** : l'indice **courant** pour la carte et les listes (état « aujourd'hui »).
**Asset** : `station_current_index` (deps `station_reference_stats`) — **nocturne**.
Classe uniquement le dernier mois.

| Colonne | Type | Description |
|---------|------|-------------|
| `code` / `type` | text | Station + domaine |
| `index_name` | text | `IPS` (piézo) ou `SSFI` (hydro) |
| `index_value` | double | z-score du dernier mois |
| `index_class` | text | Classe 7 niveaux (ou `UNKNOWN`) |
| `ref_month` | date | Mois classé |
| `baseline_start` / `baseline_end` | date | Fenêtre de référence utilisée |
| `computed_at` | timestamptz | Horodatage |

PK `(type, code)`, index sur `index_class`. **Source** : mêmes faits mensuels.
**Consommée par l'app** : carte, RightDrawer, KPI, liste Observatoire (`index_class`).

---

## Tables Gold — Référentiels & profils complémentaires

#### `ref_stations_meteeau_bsn` (seed dbt)

**Objectif** : réseau **officiel MétéEAU Nappes** du bulletin BRGM (450 indicateurs
ponctuels : 431 piézomètres + 19 sources karstiques suivies en débit). Permet de
restreindre l'agrégation par secteurs au réseau officiel (`network=meteeau`) pour coller
aux cartes BRGM. **Source** : `src/dbt_hubeau/seeds/ref_stations_meteeau_bsn.csv` (seed,
pas de calcul). Colonnes : `code_bss`, `code_bss_nouveau`, … **Consommée par l'app** :
`observatory_situation.py::_official_codes()`.

---

## Index Automatiques

dbt crée automatiquement les index suivants au premier run :

```sql
-- ERA5
CREATE INDEX IF NOT EXISTS idx_era5_lat_lon_time 
    ON bronze.era5_france_timeseries (latitude, longitude, time);
CREATE INDEX IF NOT EXISTS idx_era5_time 
    ON bronze.era5_france_timeseries (time);

-- Piézométrie
CREATE INDEX IF NOT EXISTS idx_piezo_chroniques_full 
    ON bronze.piezometry_chroniques_raw (code_bss, date_mesure);
CREATE INDEX IF NOT EXISTS idx_piezo_stations_coords 
    ON bronze.piezometry_stations_raw (x, y);
CREATE INDEX IF NOT EXISTS idx_piezo_stations_code_bss 
    ON bronze.piezometry_stations_raw (code_bss);
```

---

## Requêtes Courantes

### Volume des Tables

```sql
SELECT 
    schemaname, 
    tablename, 
    n_live_tup AS rows,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY schemaname, n_live_tup DESC;
```

### Dernière Donnée Disponible

```sql
SELECT MAX(date) AS derniere_date 
FROM gold.hubeau_daily_chroniques;
```

### Nombre de Stations par Département

```sql
SELECT 
    code_departement, 
    nom_departement,
    COUNT(DISTINCT code_bss) AS nb_stations,
    COUNT(*) AS nb_mesures
FROM gold.hubeau_daily_chroniques
GROUP BY code_departement, nom_departement
ORDER BY nb_mesures DESC;
```

### Données pour une Station

```sql
SELECT 
    date,
    niveau_nappe_eau,
    profondeur_nappe,
    temperature_2m,
    total_precipitation,
    potential_evaporation
FROM gold.hubeau_daily_chroniques
WHERE code_bss = 'BSS001XX0001'
ORDER BY date DESC
LIMIT 100;
```

### Statistiques par Entité Hydrogéologique

```sql
SELECT 
    code_eh,
    libelle_eh,
    COUNT(DISTINCT code_bss) AS nb_stations,
    COUNT(*) AS nb_mesures,
    MIN(date) AS date_debut,
    MAX(date) AS date_fin
FROM gold.hubeau_daily_chroniques
WHERE code_eh IS NOT NULL
GROUP BY code_eh, libelle_eh
ORDER BY nb_mesures DESC;
```

### Température Moyenne par Mois

```sql
SELECT 
    DATE_TRUNC('month', date) AS mois,
    AVG(temperature_2m) AS temp_moyenne_celsius,
    SUM(total_precipitation) AS precip_totale_mm
FROM gold.hubeau_daily_chroniques
WHERE date >= '2023-01-01'
GROUP BY DATE_TRUNC('month', date)
ORDER BY mois;
```

---

## Schémas Création

Les schémas sont créés automatiquement :
- `bronze` : Créé par DLT et assets Dagster au premier run
- `silver` : Créé par dbt au premier run
- `silver_rejects` : Créé par dbt au premier run
- `gold` : Créé par dbt au premier run
