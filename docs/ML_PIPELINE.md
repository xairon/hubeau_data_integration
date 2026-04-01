# ML Pipeline — Modélisation Hydrogéologique & Embeddings

> **Date** : 2026-04-01
> **Status** : Production
> **Schema** : `ml.*` (PostgreSQL + pgvector)
> **Pastas** : 1.10.1

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Pastas — Modélisation TFN](#2-pastas--modélisation-tfn)
   - [2.1 IRF Features](#21-irf-features--paramètres-de-réponse-impulsionnelle)
   - [2.2 Full Re-fit](#22-full-re-fit--décomposition--bilan-hydrique)
   - [2.3 Signatures](#23-signatures--empreinte-statistique-des-nappes)
   - [2.4 SGI](#24-sgi--standardized-groundwater-index)
3. [Embeddings — Représentation Vectorielle](#3-embeddings--représentation-vectorielle)
   - [3.1 SoftCLT (multivarié)](#31-softclt--espace-multivarié)
   - [3.2 MiniRocket+PCA (univarié)](#32-minirocketpca--espace-univarié)
   - [3.3 Preprocessing](#33-preprocessing-des-données)
   - [3.4 Clustering](#34-clustering-umap--hdbscan)
4. [Stockage PostgreSQL](#4-stockage-postgresql)
5. [Assets & Jobs Dagster](#5-assets--jobs-dagster)
6. [Structure des fichiers](#6-structure-des-fichiers)
7. [Dépendances](#7-dépendances)
8. [Commandes utiles](#8-commandes-utiles)
9. [Historique des choix techniques](#9-historique-des-choix-techniques)

---

## 1. Vue d'ensemble

Le pipeline ML produit deux familles de résultats complémentaires :

### A. Pastas — Modélisation physique simplifiée (Transfer Function Noise)

Modélise la relation **pluie → nappe** pour chaque piézomètre via un modèle Pastas TFN (Collenteur et al., 2019). Produit :

| Table | Contenu | Stations | Volume |
|-------|---------|----------|--------|
| `ml.pastas_irf_features` | Paramètres IRF + métriques + diagnostics | 5 409 | 552 MB |
| `ml.pastas_model_timeseries` | Séries simulées + bilan hydrique journalier | 5 409 | 8.1 GB |
| `ml.pastas_groundwater_signatures` | 30 signatures + 7 stats néerlandaises | 3 979 | 1.7 MB |
| `ml.pastas_sgi` | Indice standardisé mensuel N(0,1) | 3 178 | 99 MB |

### B. Embeddings — Apprentissage de représentations

Encode le comportement temporel de chaque station en vecteurs 320D pour la recherche de similarité et le clustering :

| Espace | Encodeur | Input | GPU | Ce qu'il capture |
|--------|----------|-------|-----|------------------|
| **Multi** (4D) | SoftCLT (TS2Vec + loss contrastive soft) | Cible + 3 ERA5 | Oui | Couplage hydro-climat |
| **Uni** (1D) | MiniRocket + PCA | Cible seule | Non | Forme intrinsèque du signal |

### Couverture des stations

| Pipeline | Stations | Critère d'éligibilité |
|----------|----------|-----------------------|
| Pastas IRF | 5 409 | ≥ 10 ans span, 3 vars non-nulles, fenêtre 365j ≤ 10% NaN |
| Pastas signatures | 3 979 | ≥ 2 ans d'observations (730 jours) |
| Pastas SGI | 3 178 | ≥ 5 ans d'observations (1 825 jours) |
| Embeddings (piézo) | ~2 936 | ≥ 540 jours |
| Embeddings (hydro) | ~2 535 | ≥ 540 jours |

### Flux de données

```
gold.hubeau_daily_chroniques (piézo quotidien + ERA5)
    │
    ├─→ Pastas IRF Features (fit modèle TFN, extraction paramètres)
    │       └─→ ml.pastas_irf_features
    │
    ├─→ Pastas Full Re-fit (décomposition + bilan hydrique)
    │       ├─→ ml.pastas_irf_features (UPDATE: métriques enrichies)
    │       └─→ ml.pastas_model_timeseries (INSERT: séries quotidiennes)
    │
    ├─→ Pastas Signatures (30 sigs + 7 Dutch, pas de modèle)
    │       └─→ ml.pastas_groundwater_signatures
    │
    ├─→ Pastas SGI (indice standardisé mensuel, pas de modèle)
    │       └─→ ml.pastas_sgi
    │
    ├─→ SoftCLT Encoder (multi, GPU)
    │       └─→ ml.piezo_station_embeddings + ml.piezo_window_embeddings
    │
    └─→ MiniRocket+PCA Encoder (uni, CPU)
            └─→ ml.piezo_station_embeddings + ml.piezo_window_embeddings

gold.hydro_daily_chroniques (hydro quotidien + ERA5)
    │
    ├─→ SoftCLT Encoder (multi, GPU)
    │       └─→ ml.hydro_station_embeddings + ml.hydro_window_embeddings
    │
    └─→ MiniRocket+PCA Encoder (uni, CPU)
            └─→ ml.hydro_station_embeddings + ml.hydro_window_embeddings
```

---

## 2. Pastas — Modélisation TFN

### Principe général

Pastas (Collenteur et al., 2019) est une librairie Python de modélisation des nappes par fonctions de transfert (Transfer Function Noise, TFN). Le modèle simule le niveau piézométrique comme la **convolution de la recharge avec une fonction de réponse impulsionnelle** (IRF) de type Gamma :

```
h(t) = h₀ + ∫ θ(τ) · R(t-τ) dτ + n(t)
```

Où :
- `h(t)` : niveau piézométrique simulé
- `h₀` : niveau de base (constante)
- `θ(τ)` : fonction de réponse impulsionnelle (IRF Gamma)
- `R(t)` : recharge = f(précipitation, évapotranspiration) via FlexModel
- `n(t)` : bruit résiduel

Le modèle Pastas utilisé ici est un `RechargeModel` avec :
- **IRF** : distribution Gamma (`ps.Gamma()`) — standard pour la réponse des aquifères
- **Recharge** : FlexModel (`ps.rch.FlexModel()`) — décompose automatiquement P - ET en recharge, ruissellement, stockage
- **Solveur** : LeastSquares (Levenberg-Marquardt)

**Référence méthodologique** : BRGM TEMPO (Bichot & Pinault, 2007, BRGM/RP-55348-FR)

---

### 2.1 IRF Features — Paramètres de réponse impulsionnelle

**Code** : `src/hubeau_pipeline/assets/pastas_assets.py` + `src/hubeau_pipeline/ml/pastas_wrapper.py`

**Table** : `ml.pastas_irf_features`

#### Ce qui est calculé

Pour chaque station éligible, un modèle Pastas est fitté et les paramètres suivants sont extraits :

**Paramètres IRF (Gamma)**

| Colonne | Signification | Unité | Interprétation |
|---------|---------------|-------|----------------|
| `recharge_a` | Paramètre d'échelle de la Gamma | jours | Étalement temporel de la réponse |
| `recharge_n` | Paramètre de forme de la Gamma | - | n=1 → exponentielle, n>1 → pic retardé |
| `recharge_scale` | Représentation alternative de l'échelle | jours | Identique à recharge_a (Pastas naming) |

**Features dérivées de l'IRF**

| Colonne | Signification | Calcul | Utilité |
|---------|---------------|--------|---------|
| `tmax_days` | Temps au pic de la réponse | `a × (n-1)` si n>1, sinon `model.get_response_tmax()` | **Inertie de l'aquifère** : faible (~10j) = réactif/superficiel, élevé (~300j+) = inertiel/profond |
| `cutoff_95_days` | Temps pour 95% de la réponse cumulative | `searchsorted(cumsum(abs(IRF)), 0.95 × total)` | **Mémoire de l'aquifère** : durée de l'effet d'un événement pluvieux |
| `gain` | Intégrale de la réponse (amplitude totale) | `sum(abs(block_response))` | **Sensibilité** : réponse totale de la nappe à 1mm de recharge |
| `mean_response_time` | Barycentre de l'IRF | `average(t, weights=abs(IRF))` | Temps de réponse moyen pondéré |

**Métriques de qualité du fit**

| Colonne | Signification | Plage | Interprétation |
|---------|---------------|-------|----------------|
| `nash` | Nash-Sutcliffe (= R²) | [-∞, 1] | >0.7 très bon, >0.5 bon, <0 pire que la moyenne |
| `evp` | Variance expliquée (%) | [0, 100] | nash × 100 |
| `rmse` | Root Mean Square Error | m NGF | Erreur typique en mètres |
| `r2` | Coefficient de détermination | [0, 1] | Identique à nash pour cette formulation |
| `n_observations` | Nombre d'observations non-NaN | - | Densité du signal utilisé |

**Métadonnées**

| Colonne | Contenu |
|---------|---------|
| `series_start`, `series_end` | Bornes temporelles de la série |
| `series_length_days` | Span total en jours |
| `nan_fraction` | Fraction de valeurs manquantes |
| `fit_success` | Booléen (True/False) |
| `fitted_at` | Timestamp d'exécution |
| `pastas_version` | Version de Pastas utilisée |

#### Critères d'éligibilité

1. **Pré-filtre SQL** : `MIN_YEARS = 10` ans de span, avec au moins 1 valeur non-nulle pour chaque variable (niveau_nappe_eau, total_precipitation, potential_evaporation)
2. **Filtre qualité Python** (`_check_station_quality()`) :
   - Resample quotidien + interpolation linéaire des lacunes ≤ 60 jours (`INTERP_LIMIT`)
   - Trim aux premières/dernières observations valides
   - Exige au moins une fenêtre de 365 jours (`WINDOW_SIZE`) avec ≤ 10% NaN (`NAN_THRESHOLD`)

#### Traitement

- **Batch** : 200 stations/batch (`BATCH_SIZE`)
- **Parallélisme** : 8 workers joblib/loky (`N_JOBS`)
- **Persistance** : UPSERT (INSERT ... ON CONFLICT DO UPDATE) vers `ml.pastas_irf_features`
- **Gestion d'erreurs** : les fits échoués retournent `fit_success=False` sans crasher le batch

#### Applications

- **Cartographie de l'inertie** : `tmax_days` révèle le temps de réponse de chaque aquifère → identifier les nappes à réponse rapide (vulnérables aux sécheresses courtes) vs lente (vulnérables aux sécheresses prolongées)
- **Classification des aquifères** : les paramètres (a, n, gain, tmax) regroupent naturellement les aquifères par type (alluvial rapide vs calcaire profond)
- **Prédiction** : les stations avec nash > 0.5 peuvent être simulées en prévision météo → anticipation des niveaux futurs
- **Détection d'anomalies** : résidus anormalement grands signalent un comportement inhabituel (pompage, fuite, changement structural)

---

### 2.2 Full Re-fit — Décomposition + Bilan hydrique

**Code** : `src/hubeau_pipeline/assets/pastas_refit_asset.py`

**Tables** : `ml.pastas_irf_features` (UPDATE) + `ml.pastas_model_timeseries` (INSERT)

Cet asset re-fitte le même modèle Pastas sur les stations déjà ajustées avec succès, et extrait des informations supplémentaires en une seule passe.

#### Métriques enrichies (UPDATE `ml.pastas_irf_features`)

| Colonne | Signification | Utilité |
|---------|---------------|---------|
| `kge` | Kling-Gupta Efficiency | Métrique moderne qui corrige les biais de Nash (corrélation + biais + variabilité) |
| `mae` | Mean Absolute Error (m) | Erreur moyenne en valeur absolue |
| `aic` | Critère d'information d'Akaike | Comparaison de modèles (pénalise la surparamétrisation) |
| `bic` | Critère d'information bayésien | Idem mais pénalise plus fortement les paramètres |
| `pearsonr` | Corrélation de Pearson | Corrélation linéaire obs/sim |

**Tests diagnostiques sur les résidus**

| Colonne | Test | Hypothèse nulle | Seuil |
|---------|------|-----------------|-------|
| `shapiro_pvalue` | Shapiro-Wilk | Résidus normalement distribués | p > 0.05 |
| `dagostino_pvalue` | D'Agostino-Pearson | Résidus normalement distribués | p > 0.05 |
| `runs_test_pvalue` | Runs test | Résidus aléatoires (pas de pattern) | p > 0.05 |
| `ljung_box_pvalue` | Ljung-Box | Pas d'autocorrélation résiduelle | p > 0.05 |
| `durbin_watson_stat` | Durbin-Watson | Pas de corrélation sérielle | ~2.0 = OK |

**Réponse impulsionnelle complète**

| Colonne | Type | Contenu |
|---------|------|---------|
| `block_response` | `DOUBLE PRECISION[]` | Courbe IRF complète (array PostgreSQL) |
| `block_response_length` | `INTEGER` | Longueur de l'array |

#### Séries temporelles (INSERT `ml.pastas_model_timeseries`)

Table de 51.7M lignes (5 409 stations × quotidien, 1967→2026) :

| Colonne | Signification | Source Pastas |
|---------|---------------|---------------|
| `simulated` | Niveau simulé par le modèle (m NGF) | `model.simulate()` |
| `residuals` | Observé - simulé (NULL si pas d'obs ce jour) | `model.residuals()` |
| `recharge_contribution` | Contribution du stress de recharge au niveau | `model.get_contribution("recharge")` |
| `wb_recharge` | Flux de recharge effectif (mm/j) | `FlexModel.get_water_balance()` |
| `wb_actual_evaporation` | Évapotranspiration réelle (mm/j) | idem |
| `wb_surface_runoff` | Ruissellement de surface (mm/j) | idem |
| `wb_effective_precip` | Précipitation efficace (mm/j) | idem |
| `wb_root_zone_storage` | Stockage zone racinaire (mm) | idem |

Les colonnes `wb_*` proviennent du FlexModel qui décompose automatiquement le bilan en 5 composantes. Les noms de colonnes FlexModel sont normalisés via `_normalize_wb_columns()` pour correspondre au schéma fixe.

**Persistance** : la table utilise PostgreSQL `COPY` (protocole binaire) pour l'insertion en masse — beaucoup plus rapide qu'un INSERT row-by-row pour 51M lignes. Les données existantes d'une station sont supprimées avant insertion (DELETE + COPY par batch).

#### Applications

- **Comblement de lacunes** : les valeurs `simulated` fournissent une estimation continue du niveau, même en absence d'observation
- **Bilan hydrique journalier** : `wb_recharge` quantifie la recharge effective de chaque aquifère jour par jour
- **Analyse des composantes** : distinguer la part de recharge, ruissellement, évaporation et stockage dans le cycle de l'eau
- **Détection d'anomalies** : résidus (obs - sim) anormalement grands signalent un événement non capté par le modèle (pompage, contamination, rupture structurelle)
- **Courbe IRF** : `block_response` permet de visualiser et comparer la forme complète de la réponse entre aquifères

---

### 2.3 Signatures — Empreinte statistique des nappes

**Code** : `src/hubeau_pipeline/assets/pastas_signatures_asset.py` + `src/hubeau_pipeline/ml/pastas_signatures.py`

**Table** : `ml.pastas_groundwater_signatures`

#### Ce qui est calculé

Les signatures caractérisent le **comportement** d'une nappe sans modèle physique — c'est une empreinte statistique calculée directement sur la série brute de niveau. Aucun fit Pastas n'est requis.

**30 signatures hydrogéologiques** (via `ps.stats.signatures.summary()`) :

| Catégorie | Signatures | Ce qu'elles capturent |
|-----------|------------|----------------------|
| **Variabilité** | `cv_period_mean`, `cv_date_min`, `cv_date_max`, `cv_fall_rate`, `cv_rise_rate` | Dispersion du signal et variabilité saisonnière |
| **Saisonnalité** | `parde_seasonality`, `avg_seasonal_fluctuation`, `interannual_variation` | Force et régularité des cycles annuels |
| **Pulsations** | `low_pulse_count`, `high_pulse_count`, `low_pulse_duration`, `high_pulse_duration` | Fréquence et durée des épisodes extrêmes |
| **Taux de variation** | `rise_rate`, `fall_rate`, `reversals_avg`, `reversals_cv` | Vitesse de montée/descente, fréquence des inversions |
| **Prédictibilité** | `colwell_contingency`, `colwell_constancy` | Stabilité et prévisibilité temporelle |
| **Récession/Récupération** | `recession_constant`, `recovery_constant` | Constantes de vidange et de recharge (jours⁻¹) |
| **Courbe de durée** | `duration_curve_slope`, `duration_curve_ratio` | Distribution des niveaux : pente log-log et ratio extrêmes/normal |
| **Complexité** | `richards_pathlength`, `baselevel_index`, `baselevel_stability`, `magnitude`, `autocorr_time` | Complexité du signal, niveau de base, mémoire temporelle |
| **Timing** | `date_min`, `date_max` | Jour normalisé du minimum/maximum annuel |

**7 statistiques néerlandaises** (via `ps.stats.{gg, ghg, glg, gvg, q_ghg, q_glg, q_gvg}()`) :

| Colonne | Nom complet | Signification |
|---------|-------------|---------------|
| `gg` | Gemiddelde grondwaterstand | Niveau moyen de la nappe |
| `ghg` | Gemiddeld Hoogste Grondwaterstand | Moyenne des 3 plus hauts niveaux hivernaux sur 8 ans |
| `glg` | Gemiddeld Laagste Grondwaterstand | Moyenne des 3 plus bas niveaux estivaux sur 8 ans |
| `gvg` | Gemiddelde Voorjaars Grondwaterstand | Niveau moyen de printemps (mars-avril) |
| `q_ghg` | Percentile GHG | Position du GHG dans la distribution historique |
| `q_glg` | Percentile GLG | Position du GLG dans la distribution historique |
| `q_gvg` | Percentile GVG | Position du GVG dans la distribution historique |

#### Critères d'éligibilité

- `MIN_DAYS = 730` (≥ 2 ans d'observations de `niveau_nappe_eau`)
- Pas d'interpolation — opère sur les séries brutes

#### Traitement

- **Batch** : 500 stations/batch
- **Parallélisme** : 8 workers joblib/loky
- **Persistance** : UPSERT vers `ml.pastas_groundwater_signatures`
- Les signatures échouées individuellement retournent NULL sans affecter les autres

#### Applications

- **Clustering de stations par comportement** : les 37 métriques forment un espace de features pour regrouper les nappes par dynamique similaire (complémentaire aux embeddings)
- **Comparaison entre aquifères** : l'empreinte signature permet de comparer deux nappes indépendamment de leur altitude absolue
- **Détection de stations atypiques** : outliers dans l'espace des signatures
- **Caractérisation rapide** : profil synthétique d'une station en 37 métriques (pour fiches station, dashboards)
- **Stats néerlandaises** : GHG/GLG/GVG sont les standards européens pour la caractérisation des niveaux de nappe et la cartographie d'aptitude des sols

---

### 2.4 SGI — Standardized Groundwater Index

**Code** : `src/hubeau_pipeline/assets/pastas_sgi_asset.py`

**Table** : `ml.pastas_sgi`

#### Ce qui est calculé

Le SGI (Bloomfield & Marchant, 2013) est un indice mensuel normalisé N(0,1) qui standardise le niveau piézométrique de chaque station, similaire au SPI (Standardized Precipitation Index) pour les précipitations.

| Colonne | Type | Contenu |
|---------|------|---------|
| `code_bss` | TEXT | Station |
| `date` | DATE | Fin de mois |
| `sgi` | DOUBLE | Indice standardisé |

**Calcul** :
1. `ps.stats.sgi(gwl, timescale_months=1)` : normalise la série quotidienne
2. Resample mensuel (`resample("ME").mean()`) : agrège au mois
3. Drop NaN

#### Interprétation

| Valeur SGI | Signification |
|------------|---------------|
| < -2 | Sécheresse souterraine **exceptionnelle** |
| -2 à -1 | Sécheresse **modérée** |
| -1 à +1 | **Normal** |
| +1 à +2 | Hautes eaux **modérées** |
| > +2 | Hautes eaux **exceptionnelles** |

#### Critères d'éligibilité

- `MIN_DAYS = 1825` (≥ 5 ans d'observations) — nécessaire pour une standardisation significative

#### Applications

- **Carte de sécheresse souterraine** en temps réel : SGI agrégé par département/région
- **Suivi temporel** : courbes SGI pour identifier les épisodes de sécheresse et leur durée
- **Corrélation avec les indices atmosphériques** : comparer SGI vs SPI/SPEI pour quantifier le décalage nappe/pluie
- **Alerte précoce** : SGI < -1 sur plusieurs mois consécutifs = signal de sécheresse en développement
- **Comparaison inter-stations** : l'indice normalisé rend les stations comparables indépendamment de l'échelle absolue

---

## 3. Embeddings — Représentation Vectorielle

> **Note (2026-04-01)** : les assets et jobs ML embeddings sont désactivés du pipeline automatique Dagster (retirés de `all_assets` et `all_jobs`). Les données restent en base et le code reste importable pour usage manuel.

Chaque station hydrologique est encodée dans **deux espaces complémentaires** de 320 dimensions :

### 3.1 SoftCLT — Espace multivarié

**Modèle** : TS2Vec (Yue et al., AAAI 2022) avec la loss SoftCLT (Seunghan, 2023) — apprentissage contrastif hiérarchique soft.

**Code** : `src/hubeau_pipeline/ml/latent_space/encoder.py` (`SoftCLTEncoder`)

**Principe** :
1. Les séries multivariate (T, 4) de chaque station sont découpées en fenêtres glissantes (365j, stride 90j)
2. TS2Vec encode chaque fenêtre en un vecteur de 320 dimensions via un réseau de convolutions dilatées (depth=10, champ réceptif = 2¹⁰ = 1024 jours ≈ 2.8 ans)
3. La loss SoftCLT remplace la loss contrastive dure de TS2Vec par des labels soft :
   - **Instance CL** : similarité cosine entre représentations moyennes → poids soft (pas binaire positif/négatif)
   - **Temporal CL** : pondération sigmoid basée sur le décalage temporel (timesteps proches = plus similaires)
   - **Hiérarchique** : max-pooling 2x à chaque échelle, la loss opère à toutes les résolutions
4. L'embedding station = mean pooling des embeddings de toutes ses fenêtres

**Hyperparamètres** :

| Paramètre | Valeur | Justification |
|-----------|--------|---------------|
| `input_dims` | 4 | 1 variable cible + 3 ERA5 |
| `embedding_dim` | 320 | Standard TS2Vec |
| `hidden_dim` | 64 | Capacité suffisante pour 4 vars |
| `depth` | 10 | Champ réceptif 2^10 = 1024j (~2.8 ans) |
| `window_size` | 365j | 1 cycle hydrologique complet |
| `stride` | 90j | ~4 fenêtres/an |
| `batch_size` | 128 | Optimisé pour A6000 48GB VRAM |
| `n_epochs` | 200 (max) | Early stopping patience=20 |
| `max_train_length` | 1500 | Cap padding pour éviter OOM |
| `lr` | 1e-3 | AdamW default |

**Vendorisation** :
- `src/hubeau_pipeline/ml/ts2vec/` — TS2Vec core (encoder, dilated CNN, utils)
- `src/hubeau_pipeline/ml/softclt/` — SoftCLT loss, timelags, hard losses
- Le monkey-patch `_patch_softclt_loss()` remplace la loss dans TS2Vec au runtime

**Performance mesurée (RTX A6000)** :

| Opération | Durée | VRAM |
|-----------|-------|------|
| Training piézo (~2 936 stations) | ~33 min | 32-48 GB |
| Training hydro (~2 535 stations) | ~28 min | 32-48 GB |
| Nightly encoding (~3 000 stations) | ~5 min | ~2 GB |

Fallback CPU via `device="auto"` (~10x plus lent pour le training).

### 3.2 MiniRocket+PCA — Espace univarié

**Modèle** : MiniRocket (Dempster et al., 2021, via `aeon`) + StandardScaler + PCA.

**Code** : `src/hubeau_pipeline/ml/latent_space/rocket_encoder.py` (`RocketEncoder`)

**Principe** :
1. Les séries univariate (T, 1) sont découpées en fenêtres glissantes (365j, stride 90j)
2. MiniRocket applique 9 996 noyaux convolutifs aléatoires (PPV — proportion of positive values) sur chaque fenêtre
3. Les 9 996 features sont normalisées (StandardScaler) puis réduites à 320 dimensions via PCA
4. L'embedding station = mean pooling des embeddings de toutes ses fenêtres

**Avantages** :
- **Pas de GPU** : MiniRocket est CPU-only, très rapide
- **Déterministe** : avec `random_state=42`, pas besoin de training itératif
- **Pas de backpropagation** : les noyaux sont aléatoires (pas entraînés)

**Pipeline** :
```
Fenêtres (365j) → MiniRocket (9996 features) → StandardScaler → PCA (320d) → mean pool
```

**Hyperparamètres** :

| Paramètre | Valeur |
|-----------|--------|
| `embedding_dim` | 320 |
| `window_size` | 365j |
| `stride` | 90j |
| `random_state` | 42 |
| Fit MiniRocket | sur 5 000 fenêtres (subset) |
| Fit PCA | sur 30 000 fenêtres (subset) |

**Batching** : le transform MiniRocket est fait par lots de 20 000 fenêtres pour éviter les OOM sur les grands datasets.

---

### 3.3 Preprocessing des données

**Code** : `src/hubeau_pipeline/ml/latent_space/data.py`

4 loaders par domaine × espace :

| Loader | Domaine | Espace | Colonnes | Output |
|--------|---------|--------|----------|--------|
| `load_piezo_series()` | Piézo | Multi | `niveau_nappe_eau` + 3 ERA5 | `{code_bss: (T, 4)}` |
| `load_piezo_series_univariate()` | Piézo | Uni | `niveau_nappe_eau` | `{code_bss: (T, 1)}` |
| `load_hydro_series()` | Hydro | Multi | `resultat_obs_elab` + 3 ERA5 | `{code_station: (T, 4)}` |
| `load_hydro_series_univariate()` | Hydro | Uni | `resultat_obs_elab` | `{code_station: (T, 1)}` |

**Critères d'éligibilité** :
- **min_days = 540** (~1.5 ans) — garantit au moins 2 fenêtres de 365j
- Pas de filtre de récence — les stations inactives avec un long historique enrichissent l'apprentissage contrastif

**Interpolation** : `_interpolate_and_fill()` — interpolation linéaire par colonne pour les NaN internes, puis remplissage des NaN restants (bords) par 0.

**Normalisation** :
- **Multi (SoftCLT)** : `StandardScaler` **global** (fit sur toutes les stations concaténées, par feature). Pas de normalisation par station — préserve les magnitudes relatives (un aquifère profond à -50m et un superficiel à +2m restent distincts).
- **Uni (MiniRocket)** : le scaler est intégré dans le `RocketEncoder` (fit sur les features MiniRocket, pas sur les séries brutes).

---

### 3.4 Clustering UMAP + HDBSCAN

**Code** : `src/hubeau_pipeline/ml/latent_space/clustering.py`

Chaque (domaine, espace) est clusterisé avec **deux configurations** :

| Config | But | UMAP dims | min_cluster_size | min_samples |
|--------|-----|-----------|-----------------|-------------|
| **Wide** (default) | Groupes larges, vue macro | 15 (multi) / 5 (uni) | 25 (multi) / 15 (uni) | 10 (multi) / 5 (uni) |
| **Fine** | Sous-groupes fins | 10 (multi) / 5 (uni) | 10 | 5 (multi) / 3 (uni) |

Pipeline : embeddings 320d → UMAP cosine → HDBSCAN euclidien → labels.

Les UMAP 2D/3D pour la visualisation sont aussi calculés et stockés.

**Tuning (optionnel)** : Optuna TPE optimizer (80 trials, 300s timeout) qui maximise le DBCV avec pénalité de bruit. Activable via `tune=True` dans `cluster_and_update()`.

**Métriques de qualité** :
- **DBCV** (relative_validity_) : métrique native HDBSCAN pour clusters non-convexes
- **Silhouette** : cohérence intra/inter-cluster (exclut le bruit label=-1)
- **Davies-Bouldin** : ratio de dispersion intra/séparation inter (plus bas = mieux)
- **Calinski-Harabasz** : ratio de variance inter/intra (plus haut = mieux)
- **Noise ratio** : proportion de stations non assignées

---

## 4. Stockage PostgreSQL

### Schema `ml` — Tables Pastas

| Table | Clé primaire | Lignes | Taille |
|-------|-------------|--------|--------|
| `pastas_irf_features` | `code_bss` | 5 409 | 552 MB |
| `pastas_model_timeseries` | `(code_bss, date)` | 51.7M | 8.1 GB |
| `pastas_groundwater_signatures` | `code_bss` | 3 979 | 1.7 MB |
| `pastas_sgi` | `(code_bss, date)` | 958K | 99 MB |

**Index** :
- `pastas_model_timeseries` : BRIN sur `date` + B-tree sur `code_bss`
- `pastas_sgi` : BRIN sur `date` + B-tree sur `code_bss`

### Schema `ml` — Tables Embeddings

**Code** : `src/hubeau_pipeline/ml/latent_space/persistence.py`

Extensions : `pgvector` (installé dans `timescale/timescaledb-ha:pg16`)

| Table | Contenu | Clé primaire |
|-------|---------|-------------|
| `piezo_station_embeddings` | 1 embedding/station piézo | `(code_bss, space)` |
| `piezo_window_embeddings` | 1 embedding/fenêtre piézo | `(code_bss, window_start, space)` |
| `hydro_station_embeddings` | 1 embedding/station hydro | `(code_station, space)` |
| `hydro_window_embeddings` | 1 embedding/fenêtre hydro | `(code_station, window_start, space)` |
| `clustering_runs` | Historique des exécutions clustering | `id SERIAL` |
| `clustering_labels` | Labels + UMAP coords par exécution | `(run_id, station_id)` |

**Colonnes station_embeddings** :
```
{id_col}, space, embedding vector(320), cluster_id, model_version, n_days, n_windows,
updated_at, umap_2d_x, umap_2d_y, umap_3d_x, umap_3d_y, umap_3d_z
```

**Index** :
- **HNSW** sur `embedding` des station_embeddings (cosine, m=16, ef_construction=64) — similarité O(log n)
- **B-tree** sur `(id_col, window_start)` des window_embeddings — lookup par station

**Volumétrie estimée** :

| | Piézo | Hydro | Total |
|--|-------|-------|-------|
| Stations (×2 spaces) | ~2 936 × 2 | ~2 535 × 2 | ~10 942 |
| Fenêtres (×2 spaces) | ~208K × 2 | ~161K × 2 | ~738K |
| Stockage estimé | ~510 MB | ~390 MB | ~900 MB |

---

## 5. Assets & Jobs Dagster

### 5.1 Assets Pastas (4)

| Asset | Groupe | Dépendance | Durée | Déclenchement |
|-------|--------|-----------|-------|---------------|
| `ml_piezo_pastas_irf_features` | ml_piezo | `hubeau_daily_chroniques` | ~2h | Manuel |
| `ml_piezo_pastas_full_refit` | ml_piezo | `ml_piezo_pastas_irf_features` | ~4h | Manuel (après IRF) |
| `ml_piezo_groundwater_signatures` | ml_piezo | `hubeau_daily_chroniques` | ~1h15 | Manuel |
| `ml_piezo_sgi` | ml_piezo | `hubeau_daily_chroniques` | ~16 min | Manuel |

### 5.2 Jobs Pastas (4)

| Job | Asset exécuté |
|-----|---------------|
| `pastas_irf_features_job` | `ml_piezo_pastas_irf_features` |
| `pastas_full_refit_job` | `ml_piezo_pastas_full_refit` |
| `pastas_signatures_job` | `ml_piezo_groundwater_signatures` |
| `pastas_sgi_job` | `ml_piezo_sgi` |

**Chaîne d'exécution recommandée** :
```
pastas_irf_features_job → pastas_full_refit_job
                          (indépendants :)
                          pastas_signatures_job
                          pastas_sgi_job
```

`full_refit` dépend de `irf_features` (lit les stations avec `fit_success=true`). `signatures` et `sgi` sont indépendants (lisent directement Gold).

### 5.3 Assets Embeddings (12) — Désactivés

> Retirés de `all_assets` et `all_jobs` le 2026-04-01. Le code reste importable.

| Asset | Groupe | Espace | Type |
|-------|--------|--------|------|
| `ml_piezo_multi_model_train` | ml_piezo | multi | Training (GPU) |
| `ml_piezo_uni_model_train` | ml_piezo | uni | Training (CPU) |
| `ml_hydro_multi_model_train` | ml_hydro | multi | Training (GPU) |
| `ml_hydro_uni_model_train` | ml_hydro | uni | Training (CPU) |
| `ml_piezo_multi_embeddings_update` | ml_piezo | multi | Encoding |
| `ml_piezo_uni_embeddings_update` | ml_piezo | uni | Encoding |
| `ml_hydro_multi_embeddings_update` | ml_hydro | multi | Encoding |
| `ml_hydro_uni_embeddings_update` | ml_hydro | uni | Encoding |
| `ml_piezo_multi_clusters` | ml_piezo | multi | Clustering |
| `ml_piezo_uni_clusters` | ml_piezo | uni | Clustering |
| `ml_hydro_multi_clusters` | ml_hydro | multi | Clustering |
| `ml_hydro_uni_clusters` | ml_hydro | uni | Clustering |

### 5.4 Modèle versioning (Embeddings)

- **Training** : sauvegarde dans `/var/ml/models/{domain}_{space}_{YYYYmmdd_HHMM}/`
  - SoftCLT : `model.pt` + `scaler.pkl` + `stations.json`
  - MiniRocket : `rocket.pkl` + `pca.pkl` + `scaler.pkl` + `config.pkl`
- **Symlink** : `/var/ml/models/{domain}_{space}_latest` → version courante
- **Encoding** : utilise le modèle pointé par le symlink `_latest`

---

## 6. Structure des fichiers

```
src/hubeau_pipeline/
├── ml/
│   ├── __init__.py
│   ├── pastas_wrapper.py                 # Fit TFN + extraction features/timeseries
│   │   ├── fit_single_station()          # Fit + IRF params + qualité
│   │   └── fit_and_extract_all()         # Re-fit + décomposition + bilan hydrique
│   ├── pastas_signatures.py              # 30 signatures + 7 Dutch stats
│   │   └── compute_signatures_single()   # Calcul pour 1 station
│   ├── ts2vec/                           # Vendorisé — TS2Vec core
│   │   ├── ts2vec.py                     # Classe principale (fit/encode/save/load)
│   │   ├── encoder.py                    # DilatedConvEncoder (CNN backbone)
│   │   ├── dilated_conv.py               # Blocs de convolution dilatée
│   │   ├── losses.py                     # Loss originale TS2Vec (remplacée par SoftCLT)
│   │   └── utils.py                      # Padding, split utilitaires
│   ├── softclt/                          # Vendorisé — SoftCLT loss
│   │   ├── losses.py                     # hierarchical_contrastive_loss (soft, drop-in)
│   │   ├── timelags.py                   # Matrices de timelag sigmoid/gaussian
│   │   └── hard_losses.py                # inst_CL_hard + temp_CL_hard (fallback)
│   └── latent_space/                     # Pipeline d'embedding
│       ├── encoder.py                    # SoftCLTEncoder (multi, GPU)
│       ├── rocket_encoder.py             # RocketEncoder (uni, CPU, MiniRocket+PCA)
│       ├── data.py                       # Loaders depuis Gold tables
│       ├── persistence.py                # pgvector CRUD, schema init, clustering storage
│       ├── clustering.py                 # HDBSCAN + UMAP + métriques
│       └── tuning.py                     # Optuna hyperparameter optimization
├── assets/
│   ├── pastas_assets.py                  # Asset IRF features (fit + persist)
│   ├── pastas_refit_asset.py             # Asset full re-fit (décomposition + WB)
│   ├── pastas_sgi_asset.py               # Asset SGI (indice standardisé)
│   ├── pastas_signatures_asset.py        # Asset signatures (30 sigs + 7 Dutch)
│   └── ml_assets.py                      # 12 assets embeddings (désactivés)
├── jobs/
│   └── ml_jobs.py                        # 4 jobs Pastas + 8 jobs embeddings (désactivés)
```

---

## 7. Dépendances

### Pastas

| Package | Usage |
|---------|-------|
| `pastas` | Modélisation TFN, IRF, signatures, SGI |
| `joblib` | Parallélisme (loky backend) |
| `pandas`, `numpy` | Manipulation des séries temporelles |

### Embeddings

| Package | Usage | GPU ? |
|---------|-------|-------|
| `torch` | SoftCLT training + encoding | Oui (CUDA) |
| `aeon` | MiniRocket encoder pour univarié | Non |
| `scikit-learn` | StandardScaler, PCA, métriques clustering | Non |
| `hdbscan` | Clustering density-based | Non |
| `umap-learn` | Pré-réduction dimensionnelle, visualisation | Non |
| `pgvector` | Extension PostgreSQL pour vecteurs | Non |
| `joblib` | Sérialisation modèles (scaler, rocket, pca) | Non |
| `optuna` | Tuning hyperparamètres (optionnel) | Non |

### Docker

- Worker : NVIDIA Container Toolkit pour GPU, volume externe `brgm_ml_models` monté sur `/var/ml/models`
- PostgreSQL : `CREATE EXTENSION vector` + `CREATE SCHEMA ml` dans `init.sql`

---

## 8. Commandes utiles

### Pastas (manuel via Dagster UI ou CLI)

```bash
# Fit IRF Features (~2h)
docker exec brgm-dlt-worker dagster job execute -j pastas_irf_features_job

# Full re-fit: décomposition + bilan hydrique (~4h)
docker exec brgm-dlt-worker dagster job execute -j pastas_full_refit_job

# Signatures (30 sigs + 7 Dutch, ~1h15)
docker exec brgm-dlt-worker dagster job execute -j pastas_signatures_job

# SGI (indice standardisé mensuel, ~16min)
docker exec brgm-dlt-worker dagster job execute -j pastas_sgi_job
```

### Embeddings (manuel, désactivés du pipeline auto)

```bash
# Training multivarié piézo (GPU, ~30min)
docker exec brgm-dlt-worker dagster job execute -j ml_piezo_multi_train_job

# Training univarié piézo (CPU, ~5min)
docker exec brgm-dlt-worker dagster job execute -j ml_piezo_uni_train_job
```

### Requêtes SQL — Pastas

```sql
-- Distribution de la qualité des modèles
SELECT
    CASE
        WHEN nash >= 0.7 THEN 'EXCELLENT (>=0.7)'
        WHEN nash >= 0.5 THEN 'BON (0.5-0.7)'
        WHEN nash >= 0.2 THEN 'MOYEN (0.2-0.5)'
        WHEN nash >= 0 THEN 'FAIBLE (0-0.2)'
        ELSE 'MAUVAIS (<0)'
    END AS qualite,
    COUNT(*) AS n_stations,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM ml.pastas_irf_features
WHERE fit_success = true
GROUP BY 1 ORDER BY MIN(nash) DESC;

-- Stations les plus inertielles (tmax élevé = aquifère profond)
SELECT code_bss, tmax_days, cutoff_95_days, gain, nash
FROM ml.pastas_irf_features
WHERE fit_success = true AND nash >= 0.5
ORDER BY tmax_days DESC LIMIT 20;

-- Bilan hydrique mensuel moyen d'une station
SELECT
    date_trunc('month', date) AS mois,
    AVG(wb_recharge) AS recharge_moy,
    AVG(wb_actual_evaporation) AS etp_reelle_moy,
    AVG(wb_surface_runoff) AS ruissellement_moy,
    AVG(simulated) AS niveau_simule_moy
FROM ml.pastas_model_timeseries
WHERE code_bss = 'BSS001XXXX'
GROUP BY 1 ORDER BY 1;

-- Carte de sécheresse souterraine (dernier mois)
SELECT s.code_bss, s.sgi,
    CASE
        WHEN s.sgi < -2 THEN 'EXCEPTIONNELLE'
        WHEN s.sgi < -1 THEN 'MODEREE'
        WHEN s.sgi > 2 THEN 'HAUTES_EAUX_EXCEPT'
        WHEN s.sgi > 1 THEN 'HAUTES_EAUX'
        ELSE 'NORMAL'
    END AS etat
FROM ml.pastas_sgi s
WHERE s.date = (SELECT MAX(date) FROM ml.pastas_sgi);

-- Profil de signatures d'une station
SELECT * FROM ml.pastas_groundwater_signatures
WHERE code_bss = 'BSS001XXXX';

-- Comparaison des diagnostics résidus (stations fiables)
SELECT code_bss, nash, kge, durbin_watson_stat,
    CASE WHEN shapiro_pvalue > 0.05 THEN 'OK' ELSE 'NON-NORMAL' END AS normalite,
    CASE WHEN ljung_box_pvalue > 0.05 THEN 'OK' ELSE 'AUTOCORR' END AS independance
FROM ml.pastas_irf_features
WHERE fit_success = true AND nash >= 0.5
ORDER BY nash DESC;
```

### Requêtes SQL — Embeddings

```sql
-- Stations les plus similaires (cosine, HNSW)
SELECT code_bss, embedding <=> (
    SELECT embedding FROM ml.piezo_station_embeddings
    WHERE code_bss = 'BSS001XXXX' AND space = 'multi'
) AS distance
FROM ml.piezo_station_embeddings
WHERE code_bss != 'BSS001XXXX' AND space = 'multi'
ORDER BY distance LIMIT 10;

-- Distribution des clusters
SELECT cluster_id, COUNT(*) as n_stations
FROM ml.piezo_station_embeddings
WHERE space = 'multi' AND cluster_id >= 0
GROUP BY cluster_id ORDER BY n_stations DESC;

-- Trajectoire temporelle d'une station
SELECT window_start, window_end, embedding
FROM ml.piezo_window_embeddings
WHERE code_bss = 'BSS001XXXX' AND space = 'multi'
ORDER BY window_start;

-- Dernière exécution de clustering
SELECT * FROM ml.clustering_runs
WHERE domain = 'piezo' AND is_default = TRUE
ORDER BY created_at DESC LIMIT 1;
```

---

## 9. Historique des choix techniques

| Date | Changement | Raison |
|------|-----------|--------|
| 2026-03-10 | Design initial avec TS2Vec | Apprentissage contrastif hiérarchique, self-supervised |
| 2026-03-11 | Benchmark 5 méthodes (TS2Vec, SoftCLT, tsfresh, Moment, Chronos) | SoftCLT meilleur silhouette (0.69) |
| 2026-03-12 | SoftCLT intégré en production | Monkey-patch loss, même architecture TS2Vec |
| 2026-03-12 | Dual-space (multi + uni) | Perspectives complémentaires hydro-climat vs signal pur |
| 2026-03-15 | MiniRocket+PCA remplace SoftCLT pour univarié | SoftCLT OOM en uni (matrice temporelle O(T²)), MiniRocket plus rapide et sans GPU |
| 2026-03-15 | Batch MiniRocket transform (20K/batch) | Eviter OOM sur datasets > 100K fenêtres |
| 2026-03-30 | Pastas IRF Features v1 | Modélisation TFN avec Gamma IRF + FlexModel, extraction des paramètres hydrogéologiques |
| 2026-03-31 | Pastas Signatures + SGI + Full Re-fit | Pipeline Pastas v3 complet : 30 signatures, SGI mensuel, décomposition + bilan hydrique |
| 2026-04-01 | Embeddings désactivés du pipeline auto | Assets et jobs retirés de `all_assets`/`all_jobs`, sensor supprimé. Données en base intactes |
| 2026-04-01 | Fix double exécution sensor chain | `run_key` basé sur `date.today()` au lieu de `storage_id` pour dédupliquer |
