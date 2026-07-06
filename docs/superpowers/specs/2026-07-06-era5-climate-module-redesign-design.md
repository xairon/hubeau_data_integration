# Refonte du module Climat ERA5 — Design

**Date** : 2026-07-06
**Périmètre** : deux repos — `hubeau_data_integration` (pipeline) et `time-serie-explo` (junon)
**Statut** : validé (brainstorming 2026-07-06)

## Contexte & diagnostic

L'audit complet (pipeline + junon) a établi :

- **Données solides mais sous-exploitées** : ERA5-Land 3 variables (température 2 m, précipitations,
  ETP), grille 0.1° / 11 496 points France, quotidien, 1950→aujourd'hui (321 M lignes), à jour (lag 5 j).
- **Exposition Gold inadaptée à l'UI** : la seule vue « grille » (`gold.era5_grid`) est une vue non
  matérialisée sur les 321 M lignes du silver. Le backend junon compense par des précalculs de
  climatologie de ~71 s protégés par des locks, préchauffés au démarrage et re-warmés tous les 6 jours,
  plus un cache Redis 24 h / 7 j — fragile et lent au premier hit.
- **UI dispersée et peu lisible** : la météo n'existe que comme overlay de carte caché dans un tiroir
  (défaut = SPI en σ), deux variables implémentées mais non sélectionnables (`evaporation`, `anomaly`),
  contexte météo station réduit à des barres de pluie + 2 KPI, module « Météo des nappes » (MétéEAU)
  entièrement désactivé, confusion sémantique météo/nappes.
- **Code mort pipeline** : `sources/era5_source.py` (voie DLT abandonnée), `staging.era5_france_meteo_raw`,
  seed `ref_stations_meteeau_bsn` (baseline IPS sans consommateur), table orpheline
  `gold.int_pastas_station_profile`, commentaires « chunks de 2 ans » obsolètes.
- **Trous de tests** : `potential_evaporation` jamais testée ; aucun test sur les agrégats météo
  mensuels/annuels.

## Cadrage validé

- **Public** : experts hydro/climat (hydrogéologues, chargés d'études). Les indices standardisés
  (SPI/STI) restent au premier plan ; on optimise densité d'information et efficacité.
- **Finalité** : les trois usages, hiérarchisés — (1) contexte hydro intégré aux vues existantes,
  (2) exploration climat dans une page dédiée, (3) lecture sécheresse transverse.
- **Tâches à servir** : situation à date · contexte météo d'une station · analyse d'un point/zone
  dans le temps (1950→now) · comparaison de périodes.
- **Variables** : on garde les 3 existantes (temp, précip, ETP). Pas de backfill de nouvelles
  variables CDS dans cette refonte (réversible plus tard, ex. neige SWE).

## Principe d'architecture

**Déplacer l'intelligence du backend junon vers le pipeline.** Les agrégats mensuels par point de
grille, les normales 1991-2020 et les indices SPI/STI sont matérialisés dans Gold. Le backend junon
redevient des `SELECT` simples avec cache léger.

```
Pipeline (hubeau_data_integration)          junon (time-serie-explo)
─────────────────────────────────           ─────────────────────────────
stg_era5_timeseries (321M, existant)        Backend : SELECT simples,
  → fct_era5_monthly_grid    (~10,5M)         suppression warmers 71 s
  → fct_era5_climatology_grid (~138k)       Frontend : page « Climat »
  → SPI/STI via asset Python (scipy,          + overlay carte simplifié
    pattern identique aux assets IPS)         + section climat page station
```

## Lot 0 — Hygiène (pipeline)

Livrable indépendamment, gains immédiats :

- **Suppressions** : `src/hubeau_pipeline/sources/era5_source.py` ; table
  `staging.era5_france_meteo_raw` ; tests orphelins `tests/test_pastas_*.py` (importent
  `ml.pastas_wrapper` supprimé — cassent la collecte pytest) ; table orpheline
  `gold.int_pastas_station_profile` (DROP en base, plus de modèle dbt) ;
  commentaires « 2 ans » obsolètes dans `era5_jobs.py` et l'asset historique.
- **Conservé (correction d'audit)** : le seed `ref_stations_meteeau_bsn` est consommé par le
  backend junon (`api/routers/observatory_situation.py` — filtre réseau officiel MétéEAU) ;
  il n'est PAS supprimé.
- **Tests dbt ajoutés** : `accepted_range` sur `potential_evaporation` (daily marts et
  `int_era5_for_all_stations`, ex. [-5, 20] mm/j en warn) ; ranges sur agrégats météo mensuels/annuels
  (`temperature_moyenne` [-30, 40], `precipitation_totale` mensuelle [0, 1500] mm,
  `evaporation_moyenne` etc., tous en warn).
- **Symétrie** : ajouter `era5_distance_m` à `hubeau_daily_chroniques` (déjà présent côté hydro).

## Lot 1 — Marts climat par point de grille (pipeline)

### `gold.fct_era5_monthly_grid` (dbt)

- **Clé** : `(era5_latitude, era5_longitude, mois)` — 11 496 pts × ~912 mois ≈ 10,5 M lignes.
- **Colonnes** : `temperature_moyenne`, `temperature_min`, `temperature_max`,
  `precipitation_totale`, `etp_totale`, `bilan_hydrique` (= P − ETP), `nb_jours`,
  `mois_complet` (bool). **Convention de signe** : la PEV ERA5 est stockée négative
  (convention flux descendant) — `etp_totale = SUM(-potential_evaporation)` (mm positifs).
- **Les indices SPI/STI vivent dans une table séparée** `gold.fct_era5_indices_grid`
  (format long : `era5_latitude, era5_longitude, month, fenetre ∈ {1,3,6,12}, spi, sti`),
  possédée par l'asset Python — même séparation de propriété que `gold.fct_monthly_index`
  (IPS). Évite qu'un `delete+insert` ou full-refresh dbt du mart mensuel n'efface des
  colonnes calculées par Python.
- **Matérialisation** : table plain, incrémental `delete+insert`, lookback 2 mois,
  `incremental_predicates`. **PAS d'hypertable** (règle projet : mensuel = table plain).
- Source : `stg_era5_timeseries` (coordonnées déjà arrondies à 0.1° en staging — les doublons de
  précision sont traités en amont ; junon pourra retirer sa logique de fusion pondérée).

### `gold.fct_era5_climatology_grid` (dbt)

- **Clé** : `(era5_latitude, era5_longitude, mois_calendaire, fenetre)` avec fenêtre ∈ {1, 3, 6, 12}
  — 11 496 × 12 × 4 ≈ 552 k lignes.
- **Colonnes** : normales **1991-2020** des cumuls glissants de précipitations (moyenne, écart-type,
  paramètres gamma α/β par méthode des moments — même formule que `_mom_gamma` actuel de junon) et
  de la température (moyenne, écart-type), `nb_annees_valides`.
- **Matérialisation** : table, full rebuild (rapide), rafraîchie hebdomadairement (schedule existant
  du dimanche ou équivalent).

### Asset Python `era5_indices_refresh`

- Calcule SPI (gamma → normale, McKee 1993, avec probabilité de cumul nul `prob_zero` :
  H(x) = q + (1−q)·G(x)) et STI (z-score) par cellule × mois × fenêtre à partir de
  `fct_era5_monthly_grid` + `fct_era5_climatology_grid`, et upsert dans
  `gold.fct_era5_indices_grid`. scipy, vectorisé numpy, même pattern que les assets IPS
  (`monthly_index_assets.py` / `ml/indices.py`).
- **Incrémental** : recalcule les 3 derniers mois chaque nuit ; full refresh via config/var.
- **Orchestration** : ajouté à la chaîne sensor existante après `dbt_transform_job` (aux côtés de
  `station_index_refresh`).
- **Cas limites** : mois incomplet (`mois_complet = false`) → indices NULL ; cellule avec
  < 25 ans de données valides sur la fenêtre (seuil WMO) → indices NULL (classe « données
  insuffisantes » côté UI, pattern IPS).

### Bénéfice junon immédiat

Le SPI par station (aujourd'hui recalculé à la volée par le backend) devient une lecture directe :
station → cellule via `int_station_era5_mapping` → `fct_era5_indices_grid`. Suppression de
`_warm_era5_climatology`, des locks single-flight et du re-warm 6 jours dans `api/main.py`.

## Lot 2 — junon : page « Climat » + intégrations

### Navigation

Nouvelle entrée **« Climat »** (route `/climat`), à l'emplacement nav laissé par le module MétéEAU
désactivé. **MétéEAU (`MeteoNappesPage`) reste désactivé — hors périmètre de cette refonte.**

### Vue Situation (défaut)

- Carte plein écran : SPI (fenêtre 3 mois par défaut) / STI / bilan hydrique / température /
  précipitations / **ETP (enfin exposée)** ; sélecteur mois + fenêtre.
- **Bandeau de synthèse chiffré** : « X % du territoire en sécheresse (SPI < −1) », « pire mois de
  {mois} depuis {année} », zones les plus touchées — servi par une requête d'agrégat sur le mart
  (`/era5/situation-summary`).

### Vue Point/Zone

Clic cellule sur la carte ou recherche commune/département :

- Chronique mensuelle précipitations vs normale (barres + ligne normale), zoomable 1950→now.
- Courbes SPI/STI multi-fenêtres.
- **Tableau des épisodes de sécheresse** : séquences de mois consécutifs SPI < −1 (début, fin,
  durée, SPI min, déficit cumulé mm), triables — la valeur experte nouvelle.
- Export CSV du point.

### Vue Comparaison

- Zone + années sélectionnées → cumuls pluviométriques annuels superposés (une courbe par année vs
  normale).
- Petits multiples de cartes SPI d'un même mois sur N années (ex. juin 1976 / 2003 / 2022 / 2026).

### Backend (nouveaux endpoints, tous `SELECT` sur les nouveaux marts, cache Redis 24 h)

- `GET /era5/point-series?lat&lon` — séries mensuelles + indices du point.
- `GET /era5/episodes?lat&lon&window` — épisodes de sécheresse (calcul de séquences en SQL).
- `GET /era5/situation-summary?month&window` — agrégats territoire pour le bandeau.
- `GET /era5/compare?...` — données de comparaison de périodes.
- **Suppression** : warmers/locks de climatologie, calcul SPI à la volée (grille et station),
  logique de fusion des doublons de coordonnées.

### Carte Observatoire (existante)

- Overlay conservé mais **simplifié** : SPI, bilan hydrique, température, précipitations, ETP.
- Variable fantôme `anomaly` supprimée (STI la remplace) — code, couleurs, i18n, endpoint.
- Clic cellule → popup actuel + lien **« Analyser dans Climat → »** (pré-remplit la vue Point).

### Page Station

Section « Contexte climatique » enrichie : SPI local (lecture directe via mapping), cumuls glissants
3/6/12 mois vs normale, aux côtés des barres de précipitations existantes sur le graphe.

## Tests

- **dbt** : `not_null` + `dbt_utils.unique_combination_of_columns` sur les clés des 2 marts ;
  `accepted_range` SPI/STI [−4, 4] (warn), précip mensuelle [0, 1500] mm, cohérence
  `bilan_hydrique = precipitation_totale − etp_totale` (test SQL custom).
- **Python (pipeline)** : tests unitaires du calcul SPI avec golden values (même esprit que le
  contrat IPS cross-repo `GOLDEN_Z_TO_CLASS`).
- **junon** : tests d'endpoints avec fixtures ; tests composants des 3 vues (Situation, Point/Zone,
  Comparaison).

## Ordre de livraison

1. **Lot 0** — hygiène pipeline (~1 session)
2. **Lot 1** — marts + asset indices (~1-2 sessions)
3. **Lot 2** — junon, découpable par vue : Situation → Point/Zone → Comparaison (2-3 sessions)

Chaque lot est livrable et utile indépendamment ; le Lot 2 dépend du Lot 1.
