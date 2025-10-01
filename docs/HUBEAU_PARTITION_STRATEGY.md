# Gestion des partitions Hub'Eau

## 1. État actuel

- **Pipeline** : ingestion bronze par API Hub'Eau.
- **Partitions temporelles** : saisies manuellement (typiquement `jour -> jour+1`).
- **Limites** :
  - APIs à historique long (prélèvements, qualité, hydrobiologie) tronquées (`max_pages=10`) avec pertes silencieuses.
  - APIs événementielles (hydrobiologie, qualité) générant de nombreuses partitions vides en journalier.
  - `depth_limit` appliqué globalement au lieu d'être spécifique à chaque API.

## 2. Temporalités métier

| API | Type de données | Temporalité réelle | Partition recommandée |
| --- | --- | --- | --- |
| Hydrométrie v2 | Temps réel (30 jours) | Fenêtre max 30 jours | Forcer fenêtre glissante 30 j |
| Piézométrie | Temps réel + quotidiennes | Continu | Quotidien (TR) ou annuel (chroniques) |
| Prélèvements | Chroniques annuelles | Données par année | Annuel |
| Qualité eaux (surface, nappes) | Analyses ponctuelles | Campagnes espacées | Annuel |
| Hydrobiologie | Campagnes saisonnières | Printemps/été | Annuel |
| Température | Séries continues | Depuis 2000 | Annuel |
| ONDE | Campagnes mensuelles/été | Variables | Annuel ou mensuel |

## 3. Problèmes actuels

- Appels vides fréquents en journalier.
- Troncatures silencieuses avec `max_pages`.
- Difficile d'homogénéiser l'ingestion entre APIs.
- Exemple d'erreurs fréquentes : `Server error '500 Internal Server Error'` lors d'appels massifs sur ONDE.

## 4. Stratégie proposée

### Court terme

- Partitions **annuelles** par défaut pour toutes les APIs sauf :
  - Hydrométrie : fenêtre glissante 30 jours.
  - Piézométrie (temps réel) : partitions quotidiennes.
- Avantages : limite les troncatures, simplifie la configuration, rend la logique homogène.

### Moyen terme

1. Scanner les stations/points pour récupérer `date_debut_mesure` / `date_fin_mesure` (via référentiels).
2. Si indisponible, fallback via requêtes `size=1&sort=asc/desc` pour déterminer min/max.
3. Générer dynamiquement les partitions par station (`partition_builder`).

Résultats attendus : suppression des appels vides, partitions pertinentes, ingestion alignée sur la réalité.

### Long terme

- Supprimer `max_pages` et implémenter une pagination complète (scroll jusqu'à `data=[]`).
- Centraliser la logique métier dans des fichiers de configuration YAML.

## 5. Exemple de configuration YAML

```yaml
apis:
  prelevements:
    base_url: "https://hubeau.eaufrance.fr/api/v1/prelevements"
    stations_endpoint: "referentiel/points_prelevement"
    observations_endpoint: "chroniques"
    entity_key: "code_ouvrage"
    partition_strategy: "year"
    date_field: "date_prelevement"

  hydrobiologie:
    base_url: "https://hubeau.eaufrance.fr/api/v1/hydrobio"
    stations_endpoint: "stations_hydrobio"
    observations_endpoints: ["indices", "taxons"]
    entity_key: "code_station_hydrobio"
    partition_strategy: "year"
    date_field: "date_prelevement"

  hydrometry:
    base_url: "https://hubeau.eaufrance.fr/api/v2/hydrometrie"
    stations_endpoint: "referentiel/stations"
    observations_endpoints: ["observations_tr", "obs_elab"]
    entity_key: "code_entite"
    partition_strategy: "sliding_30d"
    date_field: "date_obs"
```

## 6. Exemple de builder Python

```python
def build_partitions(date_min: str, date_max: str, strategy: str = "year") -> list[tuple[str, str]]:
    """Construit des partitions temporelles selon la stratégie."""
    from datetime import datetime, timedelta

    start = datetime.fromisoformat(date_min)
    end = datetime.fromisoformat(date_max)
    partitions = []

    if strategy == "year":
        current = start.replace(month=1, day=1)
        while current < end:
            next_year = current.replace(year=current.year + 1)
            partitions.append((current.date().isoformat(), next_year.date().isoformat()))
            current = next_year

    elif strategy == "month":
        current = start.replace(day=1)
        while current < end:
            if current.month == 12:
                next_month = current.replace(year=current.year + 1, month=1, day=1)
            else:
                next_month = current.replace(month=current.month + 1, day=1)
            partitions.append((current.date().isoformat(), next_month.date().isoformat()))
            current = next_month

    elif strategy == "sliding_30d":
        current = end - timedelta(days=30)
        partitions.append((current.date().isoformat(), end.date().isoformat()))

    elif strategy == "day":
        current = start
        while current < end:
            next_day = current + timedelta(days=1)
            partitions.append((current.date().isoformat(), next_day.date().isoformat()))
            current = next_day

    return partitions
```

## 7. Prochaines étapes

1. Appliquer les partitions annuelles + règles spécifiques hydrométrie/piézométrie.
2. Développer le module `partition_builder` dynamique.
3. Définir un schéma YAML commun et déplacer la configuration métier.
