# Sources de données Hub'Eau

Dernière vérification : 2024-09-30

Cette référence recense toutes les APIs Hub'Eau exploitées par le pipeline. Chaque section détaille les endpoints utilisés, la stratégie de partitionnement, les filtres appliqués, les champs d'intérêt et les liens vers la documentation officielle.

> **Rappel** : lors de toute évolution (nouvel endpoint, changement de paramètre), mettre à jour la date ci-dessus et les tableaux concernés.

---

## 1. Hydrométrie (v2)

- **Base URL** : `https://hubeau.eaufrance.fr/api/v2/hydrometrie`
- **Documentation officielle** : <https://hubeau.eaufrance.fr/page/api-hydrometrie>
- **Partition** : fenêtre glissante 30 jours (limitation Hub'Eau v2)

| Endpoint | Paramètres clés | Pagination | Notes |
| --- | --- | --- | --- |
| `referentiel/sites` | `dept` (boucle sur tous les départements), `size=5000` | Page | Fournit le référentiel de sites (métadonnées). |
| `referentiel/stations` | `dept`, `size=5000` | Page | Stations de mesure (code entité, coordonnées). |
| `observations_tr` | `date_debut_obs`, `date_fin_obs`, `size=1000`, `format=json` | Curseur (`supports_cursor=True`) | Limité aux 30 derniers jours par l'API. |
| `obs_elab` | `date_debut_obs_elab`, `date_fin_obs_elab`, `size=1000`, `format=json` | Curseur | Observations élaborées (débits/niveaux validés). |

**Fenêtre appliquée** : `[date_now - 30j, date_now]` recalculée à chaque run.

---

## 2. Piézométrie (v1)

- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/niveaux_nappes`
- **Documentation officielle** : <https://hubeau.eaufrance.fr/page/api-niveaux-nappes>
- **Partition** : quotidienne (`YYYY-MM-DD`)

| Endpoint | Paramètres clés | Pagination | Notes |
| --- | --- | --- | --- |
| `stations` | `dept`, `size=2000` | Page | Boucle départementale, champs BSS. |
| `chroniques_tr` | `date_debut_mesure`, `date_fin_mesure`, `size=1000` | Page | Mesures temps réel (utilisées pour la Bronze). |
| `chroniques` | `date_debut_mesure`, `date_fin_mesure`, `size=1000` | Page | Historique consolidé. |

**Fenêtre appliquée** : `[partition, partition + 1 jour)`.

---

## 3. Température des cours d'eau (v1)

- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/temperature`
- **Documentation officielle** : <https://hubeau.eaufrance.fr/page/api-temperature-cours-eau>
- **Partition** : quotidienne (`YYYY-MM-DD`)

| Endpoint | Paramètres clés | Pagination | Notes |
| --- | --- | --- | --- |
| `station` | `dept`, `size=5000` | Page | Référentiel stations température. |
| `chronique` | `date_debut_obs`, `date_fin_obs`, `size=1000` | Page | Observations temporelles, alignées sur la partition. |

---

## 4. ONDE – Observatoire national des étiages (v1)

- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/ecoulement`
- **Documentation officielle** : <https://hubeau.eaufrance.fr/page/api-ecoulement>
- **Partition** : mensuelle (`YYYY-MM`), centrée sur la saison des campagnes (mai–octobre)

| Endpoint | Paramètres clés | Pagination | Notes |
| --- | --- | --- | --- |
| `stations` | `dept`, `size=2000` | Page | Stations ONDE. |
| `observations` | `date_debut_obs`, `date_fin_obs`, `size=1000` | Page | Observations de campagne. |
| `campagnes` | `annee`, `mois` | Page | Métadonnées campagne (utilisées pour filtrage). |

---

## 5. Qualité des eaux de surface (v2)

- **Base URL** : `https://hubeau.eaufrance.fr/api/v2/qualite_rivieres`
- **Documentation officielle** : <https://hubeau.eaufrance.fr/page/api-qualite-rivieres>
- **Partition** : annuelle (`YYYY`)

| Endpoint | Paramètres clés | Pagination | Notes |
| --- | --- | --- | --- |
| `station_pc` | `dept`, `size=5000` | Page | Référentiel stations de prélèvement. |
| `operation_pc` | `dept`, `date_debut_prelevement`, `date_fin_prelevement`, `size=2000` | Page | Liste des opérations de prélèvement. |
| `condition_environnementale_pc` | `dept`, `date_debut_prelevement`, `date_fin_prelevement`, `size=2000` | Page | Conditions environnementales associées. |
| `analyse_pc` | `dept`, `date_debut_prelevement`, `date_fin_prelevement`, `size=2000` | Page | Analyses physico-chimiques détaillées. |

**Fenêtre appliquée** : `[YYYY-01-01, YYYY-12-31]`.

---

## 6. Qualité des eaux souterraines (v1)

- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_nappes`
- **Documentation officielle** : <https://hubeau.eaufrance.fr/page/api-qualite-nappes>
- **Partition** : annuelle (`YYYY`)

| Endpoint | Paramètres clés | Pagination | Notes |
| --- | --- | --- | --- |
| `stations` | `num_departement`, `size=5000` | Page | Référentiel ouvrages BSS. |
| `analyses` | `num_departement`, `date_debut_prelevement`, `date_fin_prelevement`, `size=1000` | Page | Résultats d'analyses chimiques. |

---

## 7. Hydrobiologie (v1)

- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/hydrobio`
- **Documentation officielle** : <https://hubeau.eaufrance.fr/page/api-hydrobiologie>
- **Partition** : annuelle (`YYYY`) avec `end_offset_days=1` pour inclure la borne supérieure

| Endpoint | Paramètres clés | Pagination | Notes |
| --- | --- | --- | --- |
| `stations_hydrobio` | `dept`, `size=2000` | Page | Stations hydrobiologiques. |
| `indices` | `dept`, `date_debut_prelevement`, `date_fin_prelevement`, `size=2000` | Page | Indices biologiques (IBGN, I2M2...). |
| `taxons` | `dept`, `date_debut_prelevement`, `date_fin_prelevement`, `size=2000` | Page | Dénombrements taxonomiques. |

---

## 8. Prélèvements (v1)

- **Base URL** : `https://hubeau.eaufrance.fr/api/v1/prelevements`
- **Documentation officielle** : <https://hubeau.eaufrance.fr/page/api-prelevements>
- **Partition** : annuelle (`YYYY`)

| Endpoint | Paramètres clés | Pagination | Notes |
| --- | --- | --- | --- |
| `referentiel/points_prelevement` | `dept`, `size=2000` | Page | Référentiel ouvrages (avec métadonnées). |
| `chroniques` | `dept`, `date_debut_prelevement`, `date_fin_prelevement`, `size=2000` | Page | Chroniques de volumes prélevés. |

---

## 9. Champs d'intérêt et normalisation

- Les codes station sont harmonisés via `HubeauIngestionService._get_entity_key_for_api` (ex : `code_entite`, `code_bss`, `code_station_hydrobio`).
- Les timestamps sont convertis en ISO 8601, les nombres restent tels que fournis (validation scientifique post-ingestion dans la couche Silver).
- Le pipeline ne filtre pas les variables analytiques – l'intégralité des colonnes Hub'Eau est conservée.

---

## 10. Points de contrôle qualité

- Vérifier périodiquement les tailles de page recommandées par Hub'Eau (les valeurs actuelles respectent les limites officielles).
- En cas de 204/404, confirmer sur le portail Hub'Eau s'il s'agit d'une maintenance planifiée.
- Documenter tout ajout de paramètre optionnel (ex : filtres par bassin, type d'ouvrage) dans ce fichier.

Ce référentiel constitue la base scientifique du projet. Gardez-le exhaustif et aligné avec les travaux en cours.
