# Superset – Objectif et tables disponibles

## Objectif à terme

**L'objectif est d'exploiter l'ensemble des données du pipeline dans Apache Superset**, avec :

- **Tableaux de bord** : chroniques piézométriques, hydrométriques, météo ERA5, indicateurs agrégés (gold).
- **Cartes** : visualisation des stations piézométrie et hydrométrie avec indicateurs.

Le pipeline (Bronze → Silver → Gold) et PostGIS sont dimensionnés pour alimenter Superset en tables et vues prêtes pour la BI et la cartographie.

---

## Tables gold disponibles (pré-jointes)

Pour éviter les jointures dans Superset, le pipeline produit des **marts gold** où géométrie, stations et métadonnées sont déjà jointes. Une seule table = un dataset prêt à l'emploi.

| Usage dans Superset | Table | Contenu (déjà joint) |
|---------------------|-------|----------------------|
| **Carte stations piézo** (points + alertes/tendances) | `gold.stations_piezo_carte` | 1 ligne / station : `geom`, `code_eh`, `libelle_eh`, `niveau_alerte`, `tendance_classification`, commune, département. **À privilégier** pour la carte « stations avec alerte ». |
| **Chroniques quotidiennes piézo** (séries + météo + TME) | `gold.hubeau_daily_chroniques` | 1 ligne / station / jour : niveau nappe, météo ERA5, `code_eh`, `libelle_eh`, `station_latitude`, `station_longitude`. Pas de colonne PostGIS ; utiliser lat/lon pour scatter ou filtrer par date. |
| **Agrégations mensuelles piézo** | `gold.fct_monthly_chroniques` | 1 ligne / station / mois : moyennes, min, max, variations, moyennes mobiles. |
| **Agrégations annuelles piézo** | `gold.fct_yearly_stats` | 1 ligne / station / année : bilans annuels, percentiles, classifications. |
| **Tendances saisonnières piézo** | `gold.agg_station_trends` | 1 ligne / station / saison : régressions linéaires, projections. |
| **Dimension stations piézo** | `gold.dim_piezo_stations` | 1 ligne / station : métadonnées enrichies + stats globales + tendances. |
| **Chroniques quotidiennes hydro** (séries + météo) | `gold.hydro_daily_chroniques` | 1 ligne / station / jour / grandeur : observations hydro + météo ERA5. |
| **Agrégations mensuelles hydro** | `gold.fct_monthly_hydro` | 1 ligne / station / mois / grandeur : stats mensuelles + moyennes mobiles. |
| **Agrégations annuelles hydro** | `gold.fct_yearly_hydro` | 1 ligne / station / année / grandeur : bilans annuels + classifications. |
| **Tendances saisonnières hydro** | `gold.agg_hydro_trends` | 1 ligne / station / saison / grandeur : régressions linéaires, projections. |
| **Dimension stations hydro** | `gold.dim_hydro_stations` | 1 ligne / station : métadonnées enrichies + stats hydrométriques. |
| **Carte stations hydro** (points + indicateurs) | `gold.stations_hydro_carte` | 1 ligne / station : géométrie + indicateurs principaux. |
| **Grille météo ERA5** | `gold.int_era5_grid_points` | Points de grille : `geom`, `era5_latitude`, `era5_longitude`. |

### Tables silver disponibles

| Table | Description |
|-------|-------------|
| `silver.stg_piezo_stations` | Stations piézométriques nettoyées avec géométrie |
| `silver.stg_piezo_chroniques` | Chroniques piézométriques nettoyées |
| `silver.stg_hydrometry_stations` | Stations hydrométriques nettoyées avec géométrie |
| `silver.stg_hydrometry_obs_elab` | Observations hydrométriques nettoyées |
| `silver.stg_era5_timeseries` | Données météo ERA5 nettoyées |
| `silver.stg_tme_entites` | Référentiel TME (entités hydrogéologiques) |

---

## Utilisation dans Superset

### Création de datasets

1. Dans Superset, aller dans **Data > Datasets**
2. Créer un nouveau dataset en sélectionnant une table gold ou silver
3. Pour les cartes, sélectionner la colonne **`geom`** ou **`geometry`** comme « Spatial Column »

### Graphiques cartographiques (deck.gl)

Les colonnes **`geom`** / **`geometry`** (PostGIS) sont reconnues automatiquement par Superset pour les graphiques de type carte (deck.gl).

Exemples de visualisations :

- **Scatterplot** : Stations piézo avec couleur selon `niveau_alerte`
- **Hexagones** : Densité de stations par zone géographique
- **Heatmap** : Variations de niveau de nappe par département
- **Timeseries** : Évolution du niveau moyen mensuel

### Performances

- Les index **GIST** sur les géométries (créés automatiquement par dbt) optimisent les requêtes spatiales
- Les **Hypertables TimescaleDB** accélèrent les requêtes temporelles
- Les tables gold sont pré-agrégées pour limiter les calculs côté Superset

---

## Configuration actuelle

- **Base Superset** : La connexion PostgreSQL est configurée automatiquement au démarrage
- **Tables exposées** : Les tables gold et silver listées ci-dessus sont accessibles
- **Redis** : Utilisé pour le cache des dashboards

---

## Bonnes pratiques

1. **Privilégier les tables gold** pour les dashboards (marts, dims) ; utiliser silver pour le détail brut
2. **PostGIS** : les colonnes `geometry`/`geom` sont reconnues par Superset pour les cartes deck.gl
3. **Performances** : les index automatiques (GiST, BRIN, B-tree) optimisent les requêtes
4. **Filtres** : utiliser les colonnes `code_departement`, `code_eh`, `niveau_alerte` pour filtrer efficacement

---

## Limites actuelles

⚠️ **Pas de référentiels géographiques** : Pas de calques pour les contours régions, départements ou zones hydrographiques.

---

## Évolution future

- Ajouter des référentiels géographiques (contours administratifs, zones hydro)
- Créer des vues matérialisées pour les dashboards complexes
