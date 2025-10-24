# Modes d'Ingestion Hub'Eau

## Les 3 Modes

| Mode | Description | Exemple |
|------|-------------|---------|
| **FULL** | Tout l'historique disponible | Installation initiale |
| **YEAR** | Une année spécifique | `year: 2024` |
| **INCREMENTAL** | Derniers N jours | `incremental_days: 7` |

## Configuration

### Via Dagster UI (Recommandé)

1. Ouvrir Dagster UI : http://localhost:8080
2. Sélectionner un asset
3. Cliquer "Materialize"
4. Cliquer "Open Launchpad"
5. Configurer le mode :

```yaml
ops:
  piezometry_chroniques_csv:
    config:
      mode: "year"
      year: 2024
```

### Via Partitions (Visuel)

Dagster affiche les partitions disponibles :

```
┌────────────────────────────────────┐
│ full | incremental | 2024 | 2023  │
└────────────────────────────────────┘
```

Cliquer sur la partition désirée → "Materialize"

### Via CLI

```bash
dagster asset materialize -m hubeau_pipeline \
  --select piezometry_chroniques_csv \
  --config '{"ops": {"piezometry_chroniques_csv": {"config": {"mode": "year", "year": 2024}}}}'
```

## Cas d'Usage

### Première Installation
```yaml
mode: "full"
```
Charge tout l'historique disponible.

### Mise à Jour Quotidienne
```yaml
mode: "incremental"
incremental_days: 2
```
Charge seulement les derniers 2 jours.

### Backfill Année
```yaml
mode: "year"
year: 2023
```
Charge uniquement l'année 2023.

### Backfill Multi-Années

Via CLI avec boucle :

```bash
for year in 2020 2021 2022 2023 2024; do
  dagster asset materialize -m hubeau_pipeline \
    --select piezometry_chroniques_csv \
    --config "{\"ops\": {\"piezometry_chroniques_csv\": {\"config\": {\"mode\": \"year\", \"year\": $year}}}}"
done
```

## Restrictions

**Stations/Référentiels** : FULL uniquement (pas de filtre temporel)

Exemples :
- `piezometry_stations_csv`
- `quality_rivers_stations_csv`
- `ecoulement_stations_csv`

**Chroniques/Analyses** : FULL, YEAR, INCREMENTAL supportés

Exemples :
- `piezometry_chroniques_csv`
- `quality_rivers_analyses_csv`
- `temperature_chroniques_csv`

## Comportement par Défaut

Si vous lancez un asset sans config (simple clic "Materialize"), les valeurs par défaut sont :

```python
mode = "full"           # Tout l'historique
year = None             # Pas utilisé
incremental_days = 2    # Pas utilisé en mode full
```

## Vérification

Dans les logs Dagster, vous verrez :

```
🚀 Ingestion: piezometry_chroniques
   Mode: year
   Année: 2024
```

ou

```
🚀 Ingestion: piezometry_chroniques
   Mode: incremental
   Derniers 7 jours
```

## Troubleshooting

### Erreur : "Je ne vois pas l'option de config dans l'UI"
→ Utilisez "Open Launchpad" ou "Materialize with config", pas juste "Materialize"

### Erreur : "Mode YEAR ne filtre rien"
→ Vérifiez que l'asset supporte les filtres date (chroniques/analyses uniquement)

### Erreur : "Mode YEAR necessite le parametre 'year'"
→ Vous avez mis `mode: "year"` mais oublié de spécifier `year: 2024`
