# Anomaly Detection — Résidus Pastas & Contexte Spatial

> **Date** : 2026-04-01
> **Status** : Design / Brainstorm
> **Prérequis** : Pipeline Pastas v3 (IRF, full refit, signatures, SGI) + Embeddings pgvector

---

## 1. L'idée

Le modèle Pastas ne connaît que la météo (P, ETP). Tout forçage non-climatique — pompage, connexion inter-aquifères, recharge artificielle, drainage — sort dans les **résidus** (obs − sim). En combinant l'analyse temporelle des résidus avec le **contexte spatial** des embeddings, on peut construire un détecteur d'anomalies explicable.

### Principe clé

> Si une station a des résidus anormaux mais que ses voisins de cluster n'en ont pas → **anomalie locale** (pompage, drain).
> Si tout le cluster est touché → **signal climatique** (pas une anomalie).

---

## 2. Ce qu'on a déjà

| Donnée | Table | Ce que ça apporte |
|--------|-------|-------------------|
| Résidus quotidiens (obs − sim) | `ml.pastas_model_timeseries` | Signal brut d'anomalie, 51.7M lignes |
| Diagnostics globaux (Ljung-Box, Runs test, Durbin-Watson) | `ml.pastas_irf_features` | Disent SI les résidus sont structurés, mais pas OÙ ni QUAND |
| Bilan hydrique (recharge, ruissellement, ETP réelle, stockage) | `ml.pastas_model_timeseries` | Comprendre quelle composante diverge |
| Nash, KGE, AIC/BIC | `ml.pastas_irf_features` | Filtrer les stations où le modèle est fiable (Nash ≥ 0.5) |
| Courbe IRF complète | `ml.pastas_irf_features.block_response` | Inertie de l'aquifère (contexte physique) |
| 37 signatures | `ml.pastas_groundwater_signatures` | Profil de vulnérabilité de la nappe |
| SGI mensuel | `ml.pastas_sgi` | Contexte sécheresse normalisé |
| Embeddings 320D + clusters | `ml.piezo_station_embeddings` | Voisinage comportemental (similarité) |
| Index HNSW cosine | `ml.piezo_station_embeddings` | Recherche des K plus proches en O(log n) |

---

## 3. Interprétation des résidus

Le modèle Pastas simule le niveau "naturel" — ce qui devrait se passer si seul le climat agissait. Le résidu = observé − simulé.

### Patterns et suspects

| Pattern résiduel | Suspect principal | Autres causes possibles |
|---|---|---|
| **Négatif persistant** (drift progressif) | Pompage croissant ou nouveau | Fuite vers autre aquifère, drainage agricole, imperméabilisation (urbanisation) |
| **Négatif saisonnier** (été uniquement) | Irrigation agricole | Pompage AEP saisonnier, évapotranspiration sous-estimée |
| **Positif persistant** | Recharge artificielle | Connexion avec cours d'eau non modélisée, retour d'irrigation |
| **Positif saisonnier** | Retour d'irrigation (hiver) | Crue de nappe, recharge par cours d'eau en crue |
| **Fort mais aléatoire** (pas de pattern) | Rien — bruit du modèle | Nash faible mais diagnostics propres (Ljung-Box OK, Runs test OK) |
| **Changement brutal** (rupture) | Événement ponctuel | Nouveau forage, colmatage, changement de capteur, travaux |

### Ce qui compte : direction + pattern + persistance, pas l'amplitude seule

Un résidu de -3m sur un aquifère profond (amplitude 20m) est moins significatif qu'un résidu de -0.5m sur un aquifère superficiel (amplitude 2m). Il faut normaliser par la variabilité naturelle de la station.

---

## 4. Ce qu'il manque : scores glissants temporels

Les diagnostics actuels sont **globaux** (un seul p-value pour toute la série, parfois 50+ ans). Pour un détecteur utile, il faut des **métriques par fenêtre temporelle** :

### Métriques proposées (fenêtre glissante, ex. 90 jours)

| Métrique | Calcul | Ce qu'elle détecte |
|----------|--------|-------------------|
| `residual_bias` | `mean(residuals)` sur la fenêtre | Biais persistant (positif ou négatif) |
| `residual_trend` | `slope(linreg(residuals))` | Drift progressif (pompage croissant) |
| `persistence_days` | Nb jours consécutifs de même signe | Durée de l'anomalie (plus long = plus suspect) |
| `seasonal_anomaly` | Écart du résidu moyen de ce trimestre vs la normale saisonnière | Anomalie saisonnière (irrigation) |
| `residual_stddev` | `stddev(residuals)` | Instabilité (changement de régime) |

Ces métriques sont calculables directement sur `ml.pastas_model_timeseries` (indexé BRIN sur date + B-tree sur code_bss).

---

## 5. La killer feature : contexte spatial via embeddings

### Pourquoi c'est nécessaire

Un biais résiduel de -1m ne veut rien dire tout seul. Mais si les 10 stations les plus similaires (même cluster, même dynamique hydro-climatique) montrent un biais de +0.1m pendant la même période, alors le -1m est très significatif.

### Comment le construire

1. Pour chaque station flaggée, récupérer les **K plus proches voisins** via pgvector HNSW (déjà indexé, O(log n))
2. Calculer les mêmes métriques de résidus sur les voisins
3. Calculer un **z-score spatial** : `(station_bias - median_voisins) / std_voisins`
4. z-score élevé → anomalie locale. z-score faible → signal régional (climat)

### Avantages

- **Explicable** : "Cette station est 2.3σ sous la médiane de son cluster de 15 stations similaires"
- **Robuste** : insensible aux biais systématiques du modèle Pastas (si le modèle est biaisé, toutes les stations du cluster le sont aussi → z-score = 0)
- **Pas besoin de données de pompage** : on détecte l'effet sans connaître la cause

---

## 6. Les signatures comme profil de vulnérabilité

Les 37 signatures de `pastas_groundwater_signatures` enrichissent l'explication :

- `recession_constant` élevée + `autocorr_time` élevé → aquifère inertiel → si les résidus divergent malgré ça, c'est d'autant plus suspect (il faut un forçage fort pour faire bouger un aquifère inertiel)
- `low_pulse_duration` anormalement longue vs le profil historique → le comportement change
- `parde_seasonality` + saisonnalité des résidus → distinguer anomalie saisonnière (irrigation) vs annuelle (pompage AEP)
- Stats néerlandaises (GHG/GLG) → la "norme" historique pour cadrer l'amplitude de l'anomalie

---

## 7. Architecture cible : asset `ml_anomaly_scores`

### Table de sortie : `ml.pastas_anomaly_scores`

```sql
CREATE TABLE ml.pastas_anomaly_scores (
    code_bss            TEXT NOT NULL,
    window_start        DATE NOT NULL,
    window_end          DATE NOT NULL,

    -- Scores bruts (calculés sur la fenêtre)
    residual_bias       DOUBLE PRECISION,   -- mean(residuals)
    residual_trend      DOUBLE PRECISION,   -- slope(residuals)
    persistence_days    INTEGER,            -- nb jours consécutifs même signe
    seasonal_anomaly    DOUBLE PRECISION,   -- écart vs norme saisonnière
    residual_stddev     DOUBLE PRECISION,   -- variabilité des résidus

    -- Contexte spatial (embeddings)
    cluster_id          INTEGER,            -- cluster HDBSCAN de la station
    n_neighbors         INTEGER,            -- nb voisins utilisés pour le z-score
    cluster_median_bias DOUBLE PRECISION,   -- biais médian des voisins
    cluster_std_bias    DOUBLE PRECISION,   -- std des voisins
    spatial_z_score     DOUBLE PRECISION,   -- (station - median) / std

    -- Classification
    anomaly_type        TEXT,               -- voir ci-dessous
    confidence          DOUBLE PRECISION,   -- [0, 1]

    -- Explainability
    explanation         TEXT,               -- phrase générée

    PRIMARY KEY (code_bss, window_start)
);
```

### Types d'anomalie

| `anomaly_type` | Critères | Signification |
|---|---|---|
| `PUMPING_SUSPECT` | bias < 0, spatial_z < -2, persistence > 60j | Prélèvement probable |
| `IRRIGATION_SEASONAL` | bias < 0 saisonnier (été), spatial_z < -1.5 | Pompage irrigation |
| `ARTIFICIAL_RECHARGE` | bias > 0, spatial_z > 2, persistence > 60j | Recharge artificielle ou connexion |
| `REGIME_CHANGE` | trend significatif, stddev en hausse | Changement progressif de régime |
| `POINT_EVENT` | rupture brutale dans les résidus | Événement ponctuel (forage, travaux) |
| `NORMAL` | spatial_z ∈ [-1.5, 1.5] ou diagnostics propres | Pas d'anomalie détectée |

### Score de confiance

```
confidence = f(
    abs(spatial_z_score),      -- plus éloigné du cluster = plus confiant
    persistence_days / 365,    -- plus persistant = plus confiant
    nash_station,              -- meilleur modèle = plus confiant
    n_neighbors,               -- plus de voisins = plus robuste
    1 - noise_ratio_cluster    -- cluster bien défini = plus fiable
)
```

### Texte d'explication (généré)

Exemples :
- *"Résidus moyens de -2.3 m sur 180 jours (mars-sept 2025), alors que les 12 stations similaires du cluster #7 montrent +0.1 m (z-score = -3.2). Profil aquifère : inertiel (tmax=95j), saisonnalité forte. Suspect : pompage irrigation."*
- *"Résidus de +0.8 m persistants depuis 14 mois. Les voisins montrent +0.1 m (z-score = +2.1). Possible recharge artificielle ou connexion avec cours d'eau non modélisée."*
- *"Résidus de -1.5 m mais cluster entier montre -1.2 m (z-score = -0.4). Signal climatique régional, pas d'anomalie locale."*

---

## 8. Exploration dans Superset

### Carte interactive

- Source : `stations_piezo_carte` LEFT JOIN `anomaly_scores` (dernière fenêtre)
- Markers colorés par `anomaly_type`, taille par `confidence`
- Filtres : département, type d'anomalie, seuil de confiance

### Drill-down station

- Graphe 1 : obs vs sim (2 courbes) + résidus en barres overlay
- Graphe 2 : résidus de la station vs bande interquartile des voisins de cluster
- Graphe 3 : `residual_bias` glissant + seuils d'anomalie
- Encart : profil signatures (radar chart des 6-8 signatures principales)

### Tableau de bord opérationnel

- Top 20 stations par `confidence` × `abs(spatial_z_score)`
- Timeline : combien de stations flaggées par mois (tendance nationale)
- Histogramme : distribution des types d'anomalie par département

---

## 9. Pré-filtrage : ne scorer que les stations fiables

Le détecteur n'a de sens que sur les stations où le modèle Pastas est fiable. Filtres recommandés :

- `fit_success = true` (obligatoire)
- `nash >= 0.3` (seuil minimal, sinon les résidus = bruit)
- Nash ≥ 0.5 pour les anomalies à haute confiance
- Station présente dans les embeddings (pour le z-score spatial)
- Au moins 2 ans de données récentes (résidus exploitables)

---

## 10. Chaîne d'exécution

```
[Existant]
pastas_irf_features_job  →  pastas_full_refit_job  (résidus quotidiens)
                                    ↓
[Nouveau]
ml_anomaly_scores_job :
  1. Charger résidus depuis pastas_model_timeseries
  2. Calculer métriques glissantes (90j) par station
  3. Charger embeddings + clusters depuis piezo_station_embeddings
  4. Pour chaque station, K-NN via pgvector → z-score spatial
  5. Classifier anomaly_type + confidence
  6. Générer texte d'explication
  7. UPSERT dans ml.pastas_anomaly_scores

Dépendances : pastas_full_refit + embeddings (pour le z-score)
Fréquence : après chaque full refit, ou mensuel
Durée estimée : ~10-20 min (SQL + pgvector, pas de modèle à fitter)
```

---

## 11. Extensions futures

- **Détection de ruptures** (change-point detection) sur les résidus avec PELT ou Bayesian Online CPD → détecter le moment exact où un pompage commence
- **Cross-validation temporelle** : re-fitter Pastas sur la période avant l'anomalie détectée, voir si le modèle est meilleur (confirme que l'anomalie n'existait pas avant)
- **Intégration données BNPE** (Banque Nationale des Prélèvements en Eau) : valider les détections contre les volumes de pompage déclarés
- **Expansion hydro** : même approche sur `hydro_daily_chroniques` pour détecter les barrages, prélèvements en rivière, rejets industriels
- **Alerte automatique** : sensor Dagster qui détecte les nouvelles anomalies et envoie une notification
