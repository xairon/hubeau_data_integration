# Stockage ERA5

**Architecture : Direct-to-Timeseries**

Le pipeline n'archive pas les fichiers NetCDF en base. Les données ERA5-Land (résolution
native ~0.1°, servies par le Copernicus Climate Data Store) sont téléchargées, traitées en
mémoire / disque temporaire, puis insérées directement dans la table partitionnée
`bronze.era5_france_timeseries`.

## Vue d'ensemble

```
Copernicus CDS — ERA5-Land (~0.1°)
    │
    ▼
[Job Dagster : era5_meteo_job (historique) / era5_weekly_job (incrémental)]
    │ 1. Téléchargement NetCDF (tmp)
    │ 2. Extraction xarray (en mémoire)
    │ 3. Insertion par lots
    ▼
bronze.era5_france_timeseries (séries temporelles)
    │
    ▼
Couches dbt Silver / Gold
```

## Table : `bronze.era5_france_timeseries`

**Rôle** : Unique source de vérité pour les données météo historiques et récentes.

### Structure

| Colonne | Type | Description |
|---------|------|-------------|
| `id` | BIGSERIAL | Clé primaire |
| `time` | TIMESTAMP | Date/heure (00:00 UTC) |
| `latitude` | NUMERIC(6,3) | Latitude (grille 0.1°) |
| `longitude` | NUMERIC(6,3) | Longitude (grille 0.1°) |
| `temperature_2m` | NUMERIC(6,2) | Température à 2m (°C) |
| `total_precipitation` | NUMERIC(8,4) | Précipitations totales (mm) |
| `potential_evaporation` | NUMERIC(8,4) | Évaporation potentielle (mm) |
| `source_file_id` | TEXT | Traceabilité (ex: "era5_hist_2024_2025") |

### Optimisations
1. **Index spatio-temporels** : `idx_era5_time`, `idx_era5_location`.
2. **TimescaleDB** : table convertie en **hypertable** (chunk d'1 an) + **compression** des chunks anciens.

---

## Pipelines d'Ingestion

### 1. Job historique (`era5_meteo_job`)
- **But** : charger le backlog (1950 → présent), partitionné par blocs d'années.
- **Action** : télécharge le NetCDF, extrait, insère, supprime le fichier temporaire.
- **Idempotence** : supprime la plage existante (DELETE de l'overlap), puis réinsère.
- **Usage** : bootstrap et rattrapage manuel d'une période.

### 2. Job incrémental quotidien (`era5_weekly_job`)
- **But** : mettre à jour les données récentes (jusqu'à `aujourd'hui − ERA5_AVAILABILITY_LAG_DAYS`).
- **Logique « Smart Update »** :
    1. Vérifie la date max en base (`MAX(time)`).
    2. Dérive la fenêtre réelle manquante et ne télécharge que ce delta.
    3. Déclenché chaque jour à 3h00 UTC (schedule).

---

## Mapping spatial
Les stations (piézométriques et hydrométriques) sont rattachées au point de grille ERA5 le plus proche via **PostGIS KNN** (opérateur `<->`), dans `int_station_era5_mapping` et `int_hydro_station_era5_mapping`.

```sql
-- Algorithme de mapping (Nearest Neighbor)
SELECT ...
FROM stations s
CROSS JOIN LATERAL (
    SELECT latitude, longitude
    FROM era5_grid e
    ORDER BY s.geom <-> e.geom
    LIMIT 1
) e
```

**Résultat** : Chaque station est liée à son point météo le plus pertinent géographiquement.

---

## Marts climat par point de grille (module Climat junon)

- `gold.fct_era5_monthly_grid` — agrégats mensuels par cellule 0.1° (1950→présent, ~10,5 M lignes).
  `etp_totale`/`bilan_hydrique` en mm POSITIFS (PEV ERA5 négative inversée à l'agrégation).
  `temperature_moyenne`/`temperature_min`/`temperature_max` = moyenne/min/max des vraies
  statistiques journalières (source `stg_era5_daily_temp_stats`, cf. section suivante),
  agrégées LOCALEMENT à partir des 24 pas horaires bruts. **Cutover 2026-07-13** : ces
  3 colonnes ne dérivent plus de l'échantillon instantané 00:00 UTC de
  `bronze.era5_france_timeseries` — le biais froid nocturne (~2-4°C) décrit plus bas ne
  s'applique plus aux marts grille. Précipitation/ETP/bilan_hydrique restent dérivés de
  `stg_era5_timeseries` (pas de source journalière vraie disponible pour ces variables).

  > ⚠️ **Ne PAS conclure que l'ETP était biaisée *par le 00h UTC*** — erreur déjà commise. La
  > `potential_evaporation` d'ERA5 est un **flux d'accumulation** produit par le modèle ECMWF :
  > la valeur à 00:00 UTC **EST** le cumul journalier correct, ce n'est pas un échantillon
  > instantané. Le biais froid ne concernait que la *température*. (La PEV avait un tout autre
  > problème — son niveau — traité ci-dessous.)

### ⚠️ ETP : Hargreaves (2026-07-24), et NON la PEV d'ERA5

`etp_totale` est une **ET0 de référence calculée par la formule de Hargreaves (FAO-56)** à
partir des Tmin/Tmax/Tmoy journaliers vrais — possible seulement depuis le cutover
température. Le rayonnement extraterrestre `Ra` est calculé analytiquement (latitude +
jour de l'année), donc aucune donnée de vent/humidité/rayonnement n'est nécessaire.

**Pourquoi on a abandonné la `potential_evaporation` d'ERA5-Land.** Mesuré sur 30 888
mailles-mois (2015-2025, mêmes mailles) :

| | Hargreaves | PEV ERA5-Land | ratio |
|---|---|---|---|
| ETP annuelle | **818 mm** | **1 756 mm** | **×2,15** |
| Bilan P−ETP | **+146 mm/an** | **−793 mm/an** | — |

818 mm/an est cohérent avec l'ET0 de référence pour la France (littérature : 700-900 mm) ;
1 756 mm/an ne l'est pas, et mettait le pays en déficit hydrique permanent. La PEV d'ERA5
n'est **pas** une ET0 de référence FAO : c'est l'évaporation d'une surface sans stress
hydrique calculée avec la résistance aérodynamique du modèle, connue pour surestimer
largement l'ET0. Hargreaves est le repli recommandé par la FAO-56 en l'absence de
rayonnement/vent/humidité, et c'est la méthode employée par la littérature d'attribution
(World Weather Attribution utilise « ERA5 + Hargreaves »), ce qui rend notre SPEI
comparable aux publications.

La PEV brute reste exposée en `etp_pev_era5` pour traçabilité — **ne pas la consommer**.

> **Incohérence assumée grille ↔ station.** La chaîne *station*
> (`int_era5_for_all_stations` → `hubeau_daily_chroniques` / `hydro_daily_chroniques` /
> `fct_monthly_*`) continue d'exposer la **PEV brute** sous `potential_evaporation`. Elle
> sert de forçage aux modèles Pastas (TFN) : la changer invaliderait tous les calages
> existants. C'est une décision délibérée, pas un oubli. Conséquence : une « ETP » du module
> Climat (Hargreaves) et une « ETP » de la page Station (PEV) ne sont pas la même grandeur
> et diffèrent d'un facteur ~2.
> Note : `/observatory/era5/*` reconstruit sa `potential_evaporation` journalière depuis
> `etp_totale` (`-(etp_totale/nb_jours)`) — cet endpoint suit donc **Hargreaves**.

- `gold.fct_era5_climatology_grid` — normales 1991-2020 (gamma MoM + μ/σ) par cellule × mois ×
  fenêtre. Sert le **SPI** (gamma) et le **STI** (z-score). Modèle dbt.
- `gold.fct_era5_spei_climatology_grid` — paramètres de la **logistique généralisée (GLO)**
  pour le SPEI (`glo_alpha`, `glo_k`, `glo_xi`), référence 1991-2020, par cellule × mois
  calendaire × fenêtre. **Table gérée en Python, pas en dbt** : l'ajustement par L-moments a
  besoin des ~30 échantillons annuels ET de la fonction Γ, que PostgreSQL n'a pas.
  Les colonnes `ll_*` sont **obsolètes** (ancienne log-logistique, conservées sans être lues).
- `gold.fct_era5_indices_grid` — **SPI/STI/SPEI** (fenêtres 1/3/6/12) calculés par l'asset
  Python `fct_era5_indices_grid` (job `station_index_refresh`, nightly). Table vide →
  bootstrap complet 1950→présent.

### SPEI — pourquoi la GLO et non la log-logistique

Le choix initial (log-logistique 3 paramètres, Vicente-Serrano 2010) n'ajustait que **74,6 %**
des couples cellule × mois × fenêtre. L'instrumentation des motifs de rejet a montré que
**100 % des rejets** venaient de la garde `β ≤ 1` et **0 %** d'un manque de données. Or
`β = 1/τ₃` et |τ₃| < 1 toujours ⇒ `β ≤ 1` implique **τ₃ < 0** (asymétrie négative) : la
log-logistique est une loi à asymétrie *positive*, structurellement incapable de représenter
ces mailles. La **GLO** (`k = −τ₃`) accepte les deux signes → **100 % de couverture**.

La bascule est **purement additive** : la log-logistique est exactement la GLO reparamétrée
(`k = −1/β`), donc les valeurs sont identiques là où l'ancienne loi fonctionnait (vérifié :
écart max 0,000 sur 35 614 mailles). Voir `time-serie-explo/docs/superpowers/specs/2026-07-23-climat-spei-design.md` §2.0.

### ⚠️ Procédure de (re)construction du SPEI — ordre impératif

1. **Référence d'abord** — elle n'est **PAS** dans le job nightly (c'est une référence fixe
   1991-2020, inutile de la recalculer chaque nuit) :
   ```bash
   docker exec brgm-dlt-worker dagster asset materialize \
     --select fct_era5_spei_climatology_grid -m hubeau_pipeline.definitions   # ~2,5 min
   ```
2. **Puis les indices** :
   ```bash
   docker exec brgm-dlt-worker dagster asset materialize \
     --select fct_era5_indices_grid -m hubeau_pipeline.definitions
   ```
   ⚠️ Sur une table **déjà peuplée**, cet asset ne recalcule que les **3 derniers mois**
   (`latest_index_month()` non nul → branche nightly). Le `spei` resterait donc NULL sur tout
   l'historique. Un **backfill historique est obligatoire** après tout changement de méthode :
   boucler `_compute_range` par tranches de 5 ans de 1950 à aujourd'hui (upsert
   `ON CONFLICT DO UPDATE`, donc non destructif — spi/sti sont réécrits à l'identique).
   Coût mesuré : ~12 000 lignes/s, **~50 min** pour les 41,96 M lignes.
3. **Purger le cache junon** : `junon:obs_climat_*` (et `junon-redis-dev` pour l'env de dev),
   sinon l'app sert les anciennes valeurs jusqu'à 24 h.

---

## Voie complémentaire : statistiques journalières (mean/min/max)

**Même dataset que la timeseries** : `reanalysis-era5-land` (archive horaire brute), mais on
télécharge les **24 pas horaires** du jour (au lieu du seul 00:00 UTC) et on calcule
**localement** (`aggregate_hourly_to_daily`, groupby jour) la moyenne/min/max journalières
réelles de `2m_temperature` — sans le biais d'échantillonnage 00:00 UTC. C'est désormais la
source de `fct_era5_monthly_grid.temperature_*` (cf. section précédente).

> **Historique** : une 1ʳᵉ implémentation utilisait le produit dérivé
> `derived-era5-land-daily-statistics` (calcul côté CADS). Ce service post-traité est une file
> minuscule saturée (~43 h par ANNÉE demandée, backfill complet ~6 semaines). Bascule le
> 2026-07-10 vers l'archive brute (rapide : ~25 min/année). Équivalence vérifiée cellule-jour
> par cellule-jour contre le produit dérivé : Tn/Tx identiques à 0,0000 °C, moyenne à 0,01 °C
> (arrondi NUMERIC(6,2)). Voir `.superpowers/sdd/progress.md`.

- **Table bronze** : `bronze.era5_daily_temp_stats(time, latitude, longitude, t2m_mean,
  t2m_min, t2m_max, source_file_id, created_at)` — hypertable chunks 1 an, compression
  après 30 j, K→°C converti à l'insertion.
- **Silver** : `silver.stg_era5_daily_temp_stats` (append incrémental, dédup DISTINCT ON,
  arrondi 1 décimale), tests not_null/accepted_range/expression_is_true (`min≤mean≤max`).
  ⚠️ Le filtre incrémental est `time > MAX(time)` : un backfill d'années antérieures en
  bronze **n'entre pas** en silver sans reprocess forcé — voir la procédure de cutover ci-dessous.
- **Jobs Dagster** : `era5_daily_temp_historical_load` (partitionné 1 an, clés
  `"YYYY_YYYY"`, ex. `1950_1950`, 1 requête/mois brut horaire, mois téléchargés en parallèle
  `months_concurrency`) pour le backfill 1950→présent ; `era5_daily_temp_update_job`
  (smart update quotidien, schedule 03h30 UTC — jours dérivés de la fenêtre réelle, anti-cache
  CADS périmé).
- **Statut (2026-07-13)** : backfill **COMPLET** — 1950→2025, 76 années,
  319,9 M lignes en **bronze**, 0 incohérence min≤moy≤max. Débit archive brute : ~25 min/année.
  ⚠️ « Complet en bronze » n'implique **pas** « complet en silver » : le silver ne reçoit ces
  années que si le staging est reprocessé explicitement (procédure de cutover ci-dessous).
- **Cutover mart (2026-07-13, FAIT)** : `fct_era5_monthly_grid.temperature_*` dérive de
  `stg_era5_daily_temp_stats` (LEFT JOIN sur lat/lon/jour, pas de COALESCE de repli).
  Précipitation/ETP/bilan_hydrique/nb_jours/mois_complet restent dérivés de
  `stg_era5_timeseries`, inchangés. Procédure de rebuild détaillée ci-dessous.

### Procédure de cutover / rebuild du mart température (ordre IMPÉRATIF)

> ⚠️ **Étape silver OBLIGATOIRE AVANT le mart — piège vérifié.** Un backfill de
> `bronze.era5_daily_temp_stats` sur des années antérieures (1950→2025) **n'entre pas tout
> seul en silver**. Le modèle `stg_era5_daily_temp_stats` est incrémental (`append`) avec le
> filtre `AND time > (SELECT MAX(time) FROM {{ this }})`. Comme l'asset nightly peuple déjà le
> silver avec du récent (année en cours), `MAX(time)` ≈ aujourd'hui : toute ligne backfillée a
> `time < MAX(time)` et est **silencieusement ignorée** par l'`append` — **aucune erreur**,
> mais le silver (donc `fct_era5_monthly_grid`) n'a de température que sur les années déjà
> présentes, **NULL ailleurs**. Il faut donc forcer le reprocess du silver sur toute la plage
> backfillée AVANT de rebâtir le mart. Ce piège vaut pour **tout** futur backfill de température,
> pas seulement le cutover initial.

1. **Re-stage silver sur toute la plage backfillée** (l'une OU l'autre commande) :
   ```bash
   # Option A (recommandée) — full-refresh : reconstruit toute la table silver depuis bronze,
   # le filtre incrémental ne s'applique pas, aucun risque de conflit de PK.
   docker exec brgm-dlt-worker dbt run --full-refresh --select stg_era5_daily_temp_stats

   # Option B (celle appliquée le 2026-07-13) — TRUNCATE puis reprocess ciblé via la var projet.
   # Le TRUNCATE est requis : le modèle est en `append`, réinjecter depuis 1950 dans une table
   # non vide violerait la PK (latitude, longitude, time) sur l'overlap récent.
   docker exec -it brgm-postgres psql -U postgres -d postgres \
     -c "TRUNCATE silver.stg_era5_daily_temp_stats;"
   docker exec brgm-dlt-worker dbt run --select stg_era5_daily_temp_stats \
     --vars '{era5_daily_temp_reprocess_from_timestamp: "1950-01-01"}'
   ```
   > La var `era5_daily_temp_reprocess_from_timestamp` remplace le filtre `time > MAX(time)` par
   > `time >= <ts>::timestamp` (même mécanisme que `era5_reprocess_from_timestamp` sur le modèle
   > jumeau `stg_era5_timeseries`). Contrôle post-run : `SELECT MIN(time) FROM
   > silver.stg_era5_daily_temp_stats;` doit renvoyer 1950 (et non l'année en cours).
2. **Rebuild du mart mensuel** :
   ```bash
   docker exec brgm-dlt-worker dbt run --full-refresh --select fct_era5_monthly_grid
   ```
3. **Rebuild climatologie + re-bootstrap indices SPI/STI**, puis **ré-étiquetage junon**.

### Cutover marts STATION (2026-07-15, FAIT)

La température **niveau station** (`temperature_2m` des marts journaliers, `temperature_moyenne`
des marts mensuels/annuels) dérivait encore de l'instantané 00:00 UTC (`stg_era5_timeseries`),
alors que les marts grille étaient déjà passés aux vraies stats journalières → incohérence
produit (temp grille ≠ temp station, écart jusqu'à ~5°C l'été). Bascule effectuée :

- `int_era5_for_all_stations` : la température vient désormais d'un 2ᵉ LEFT JOIN sur
  `stg_era5_daily_temp_stats` (t2m_mean, même grain cellule 0.1° × jour), exposée sous le nom
  `temperature_2m` inchangé. Précipitation/ETP restent le pilote depuis `stg_era5_timeseries`.
  **Sémantique assumée** : une ligne mêle température = vraie moyenne journalière et précip/ETP =
  flux d'accumulation journaliers (chaque variable a sa représentation journalière correcte).
  Nuance : `temperature_min`/`max` mensuels station restent des MIN/MAX de moyennes journalières
  (pas de vrais Tn/Tx — cela imposerait d'ajouter t2m_min/max au grain station → piège ALTER
  hypertable, hors périmètre).

Séquence de rebuild (⚠️ **JAMAIS `--full-refresh` sur les 2 hypertables journalières** →
phantom hypertables ; utiliser la var de fenêtre) :
```bash
# 1. int (plain, sûr)
dbt run --full-refresh --select int_era5_for_all_stations
# 2. hypertables journalières : reprocess historique via la fenêtre élargie (couvre 1967→)
dbt run --select hubeau_daily_chroniques --vars '{"daily_recompute_window_days": "22000"}'
dbt run --select hydro_daily_chroniques  --vars '{"daily_recompute_window_days": "22000"}'
# 3. marts plain aval (agrègent depuis les hypertables)
dbt run --full-refresh --select fct_monthly_chroniques fct_monthly_hydro \
                                fct_yearly_stats fct_yearly_hydro dim_piezo_stations
```
Puis **flush Redis junon** (la température station remonte de +2-4°C l'été → cache obs à
invalider) + passe visuelle. IPS/SSFI non impacté (ne consomme pas la température).

---

## Maintenance

### Vérifier les Time Series
```sql
SELECT 
    source_file_id,
    COUNT(*) AS nb_lignes,
    MIN(time) AS date_debut,
    MAX(time) AS date_fin
FROM bronze.era5_france_timeseries
GROUP BY source_file_id
ORDER BY source_file_id DESC;
```

### Relancer une période
Depuis l'interface Dagster :
1. Job `era5_meteo_job`
2. Sélectionner la partition correspondant à la période à recharger
3. Lancer le run (le job supprime l'overlap puis réinsère ; un DELETE manuel est conseillé si les données sont corrompues).
