## Comptes 2024 par endpoint (Hub'Eau) et pipeline

| Domaine | Endpoint | Fenêtre de dates | Compte API (2024) | Source/API de référence | Compte observé pipeline | Remarques |
|---|---|---|---:|---|---:|---|
| Température | `/temperature/chronique` | date_debut_mesure=2024-01-01, date_fin_mesure=2024-12-31 | 1 456 457 | [`lien`](https://hubeau.eaufrance.fr/api/v1/temperature/chronique?date_debut_mesure=2024-01-01&date_fin_mesure=2024-12-31&size=20) | (à compléter) | Pagination complète jusqu'à `next=None` |
| Qualité rivières | `/qualite_rivieres/station_pc` (stations actives) | date_debut_prelevement=2024-01-01, date_fin_prelevement=2024-12-31 | ~4 431 | (compte API direct utilisateur) | 4 417 (logs) | Écart faible dû au set MinIO ∩ API_2024 |
| Qualité rivières | `/qualite_rivieres/operation_pc` | date_debut_operation=2024-01-01, date_fin_operation=2024-12-31 | 38 330 | (compte API direct utilisateur) | (à compléter) | Slicer station×mois |
| Qualité rivières | `/qualite_rivieres/condition_environnementale_pc` | date_debut_operation=2024-01-01, date_fin_operation=2024-12-31 | 357 311 | (compte API direct utilisateur) | (à compléter) | Slicer station×mois |
| Qualité rivières | `/qualite_rivieres/analyse_pc` | date_debut_prelevement=2024-01-01, date_fin_prelevement=2024-12-31 | 10 069 396 | [`lien`](https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/analyse_pc?date_debut_prelevement=2024-01-01&date_fin_prelevement=2024-12-31&size=20) | (à compléter) | Slicer station×mois, pagination 20k |

Notes
- Les comptes API proviennent des champs `count` des endpoints Hub'Eau et/ou du comptage direct utilisateur.
- Les comptes observés pipeline se remplissent après exécution complète des assets correspondants sur la partition 2024.
- Si écart: vérifier stations actives (union MinIO ∪ API_2024), bornes dates inclusives, et pagination jusqu'à `next=None`.


