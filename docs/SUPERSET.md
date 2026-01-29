# Superset – Objectif et calques

## Objectif à terme

**L’objectif est d’exploiter l’ensemble des données du pipeline dans Apache Superset**, avec :

- **Tableaux de bord** : chroniques piézométriques, météo ERA5, indicateurs agrégés (gold).
- **Cartes et calques** : visualisation des entités hydrogéologiques (BDLISA), stations, grille ERA5, etc.

Le pipeline (Bronze → Silver → Gold) et PostGIS sont dimensionnés pour alimenter Superset en tables et vues prêtes pour la BI et la cartographie.

---

## Tables gold avec jointures déjà faites (geo + stations)

Pour éviter les jointures dans Superset, le pipeline produit des **marts gold** où géométrie, stations et entités BDLISA sont déjà jointes. Une seule table = un calque prêt à l’emploi.

| Usage dans Superset | Table | Contenu (déjà joint) |
|---------------------|-------|----------------------|
| **Carte stations piézo** (points + BDLISA + alerte/tendance) | `gold.stations_piezo_carte` | 1 ligne / station : `geom`, `code_eh`, `libelle_eh`, `niveau_alerte`, `tendance_classification`, commune, département. **À privilégier** pour la carte « stations avec entité hydro et alerte ». |
| **Chroniques quotidiennes** (séries + BDLISA + météo) | `gold.hubeau_daily_chroniques` | 1 ligne / station / jour : niveau, météo ERA5, `code_eh`, `libelle_eh`, `station_latitude`, `station_longitude`. Pas de colonne PostGIS ; utiliser lat/lon pour scatter ou filtrer par date. |
| **Polygones BDLISA** (nappes / unités aquifères) | `silver.stg_tme_entites` | 1 ligne / entité : `geometry` (polygones), `code_eh`, `libelle_eh`. Calque de fond « nappes ». |
| **Points stations hydrométriques** | `gold.dim_hydro_stations` | 1 ligne / site : `geometry` (points), code_site, etc. |
| **Grille météo ERA5** | `gold.int_era5_grid_points` | Points de grille : `geom`, `era5_latitude`, `era5_longitude`. |

En résumé : **pas de jointure à faire dans Superset** pour les calques principaux ; les tables gold exposent déjà géo + stations + BDLISA (ou météo). Les colonnes **geometry** / **geom** (PostGIS) servent pour les graphiques type **carte** (deck.gl).

---

## Configuration actuelle

- **Base Superset** : `docker/superset/datasources.yaml` déclare la connexion PostgreSQL et les tables gold/silver listées ci‑dessus (dont `stations_piezo_carte`, `hubeau_daily_chroniques`, `stg_tme_entites`, etc.).
- **Import au démarrage** : `docker-init.sh` exécute `superset import_datasources -p /app/datasources.yaml`.
- Dans Superset : créer un **dataset** par table, puis choisir la colonne **geom** / **geometry** comme « spatial column » pour les graphiques type carte (deck.gl).

---

## Bonnes pratiques

1. **Privilégier les tables gold** pour les dashboards (marts, dims) ; utiliser silver si besoin de détail brut (ex. BDLISA pour les polygones).
2. **PostGIS** : les colonnes `geometry`/`geom` sont reconnues par Superset pour les graphiques type carte (deck.gl) lorsque la source est PostgreSQL/PostGIS.
3. **Performances** : les index GiST sur les géométries (déjà créés par dbt) améliorent les requêtes spatiales et le rafraîchissement des cartes.
