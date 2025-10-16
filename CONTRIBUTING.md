# Contributing Guide

Merci de contribuer au projet Hub'Eau Data Integration ! Ce guide vous aidera à démarrer.

## 📋 Table des Matières

- [Code de conduite](#code-de-conduite)
- [Comment contribuer](#comment-contribuer)
- [Structure du projet](#structure-du-projet)
- [Workflow de développement](#workflow-de-développement)
- [Standards de code](#standards-de-code)
- [Tests](#tests)
- [Documentation](#documentation)

---

## Code de conduite

- Soyez respectueux et constructif
- Posez des questions si quelque chose n'est pas clair
- Partagez vos connaissances
- Acceptez les critiques constructives

---

## Comment contribuer

### 🐛 Reporter un Bug

1. Vérifiez qu'il n'existe pas déjà dans les Issues
2. Créez une Issue avec le template `bug_report`
3. Incluez :
   - Description claire du problème
   - Steps to reproduce
   - Comportement attendu vs obtenu
   - Logs pertinents

### ✨ Proposer une Feature

1. Créez une Issue avec le template `feature_request`
2. Expliquez :
   - Le besoin / problème résolu
   - La solution proposée
   - Les alternatives considérées

### 🔧 Soumettre un Fix/Feature

1. Fork le projet
2. Créez une branche depuis `main`
3. Implémentez vos changements
4. Testez localement
5. Créez une Merge Request

---

## Structure du projet

```
hubeau_data_integration/
├── configs/hubeau/              # ✅ Configurations DLT (YAML)
│   └── *.yml                    # 1 fichier = 1 endpoint
│
├── src/
│   ├── dlt_pipeline/            # ✅ Pipeline DLT réutilisable
│   │   ├── hubeau_generic.py    # Source DLT Hub'Eau
│   │   ├── slicing.py           # Stratégies de découpage
│   │   └── http_client.py       # Client HTTP avec retry
│   │
│   └── hubeau_pipeline/         # ✅ Code Dagster
│       ├── assets/bronze/       # Assets = datasets
│       ├── jobs/                # Jobs = groupes d'assets
│       ├── schedules/           # Planification automatique
│       ├── resources.py         # Connexions DB, S3, etc.
│       └── definitions.py       # Point d'entrée Dagster
│
├── docker/                      # ✅ Dockerfiles
│   ├── orchestrator/            # Dagster UI + daemon
│   └── worker/                  # DLT worker (exécution)
│
├── docs/                        # ✅ Documentation
└── scripts/                     # ✅ Déploiement
```

---

## Workflow de développement

### 1. Setup Local

```bash
# Clone
git clone https://scm.univ-tours.fr/ringuet/hubeau_data_integration.git
cd hubeau_data_integration

# Config
cp .env.template .env
# Éditer .env avec vos credentials

# Launch
docker-compose up -d

# Accès
# Dagster UI: http://localhost:8080
# MinIO: http://localhost:9001
```

### 2. Créer une Branche

```bash
git checkout -b feature/nom-feature
# ou
git checkout -b fix/nom-bug
```

**Convention de nommage** :
- `feature/` : Nouvelle fonctionnalité
- `fix/` : Correction de bug
- `docs/` : Documentation uniquement
- `refactor/` : Refactoring sans changement fonctionnel
- `chore/` : Maintenance (deps, config, etc.)

### 3. Développer

#### Ajouter une nouvelle API Hub'Eau

1. **Créer config YAML** : `configs/hubeau/nouvelle_api.yml`

```yaml
source:
  name: "nouvelle_api"
  description: "Description de l'API"
  base_url: "https://hubeau.eaufrance.fr/api/v1/nouvelle"

resource:
  name: "nouvelle_api_data"
  endpoint: "/data"
  write_disposition: "merge"  # ou "replace"
  primary_key: "code_station"

slicing:
  strategy: "datetime"  # ou "global", "dept", "station_month_chunked"
  date_field: "date_debut_mesure"
  start_date: "2020-01-01"

destinations:
  filesystem:
    bucket_url: "s3://bronze"
    layout: "{table_name}/year={year}/{load_id}.{file_id}.{ext}"
```

2. **Créer asset Dagster** : `src/hubeau_pipeline/assets/bronze/dlt_assets.py`

```python
@asset(
    partitions_def=yearly_partitions,
    group_name="nouvelle_api",
    compute_kind="dlt",
)
def nouvelle_api_data(context: AssetExecutionContext) -> MaterializeResult:
    """
    Asset Nouvelle API : extraction via DLT
    """
    return ingest_dlt(context, "configs/hubeau/nouvelle_api.yml")
```

3. **Ajouter au job** : `src/hubeau_pipeline/jobs/dlt_jobs.py`

```python
nouvelle_api_job = define_asset_job(
    name="hubeau_nouvelle_api_job",
    selection=AssetSelection.groups("nouvelle_api"),
    description="Job Nouvelle API Hub'Eau"
)
```

4. **Tester** :

```bash
# Via UI
http://localhost:8080 → Jobs → hubeau_nouvelle_api_job → Launch Run

# Via CLI
dagster job execute -j hubeau_nouvelle_api_job
```

#### Modifier une configuration existante

1. Éditer le YAML : `configs/hubeau/api.yml`
2. Tester dans Dagster UI
3. Commit & Push

---

## Standards de code

### Python

- **Style** : PEP 8 (formaté avec Black)
- **Line length** : 120 caractères
- **Type hints** : Recommandés pour les fonctions publiques
- **Docstrings** : Google style

```python
def extract_stations(
    api_name: str,
    start_date: str,
    end_date: str
) -> List[Dict[str, Any]]:
    """
    Extract stations from Hub'Eau API.

    Args:
        api_name: Name of the API to query
        start_date: Start date (ISO format)
        end_date: End date (ISO format)

    Returns:
        List of station dictionaries

    Raises:
        HTTPError: If API request fails
    """
    ...
```

### Configuration YAML

- **Indentation** : 2 espaces
- **Quotes** : Doubles quotes pour les strings
- **Comments** : Expliquer les choix non-évidents

```yaml
# Good
resource:
  name: "temperature_chroniques"  # Nom table MinIO
  endpoint: "/chroniques"
  write_disposition: "merge"  # Mode merge pour updates incrémentaux
  primary_key: "code_station"

# Bad
resource:
  name: temperature_chroniques
  endpoint: /chroniques
  write_disposition: merge
  primary_key: code_station
```

### Commits

**Convention** : [Conventional Commits](https://www.conventionalcommits.org/)

```bash
# Format
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types** :
- `feat:` Nouvelle fonctionnalité
- `fix:` Correction de bug
- `docs:` Documentation uniquement
- `refactor:` Refactoring
- `chore:` Maintenance
- `test:` Ajout/modification de tests

**Exemples** :

```bash
feat(hydrometry): add obs_elab endpoint support

fix(dlt): handle pagination for large datasets

docs(readme): update installation instructions

refactor(slicing): extract datetime logic to separate function

chore(deps): upgrade dagster to 1.11.14
```

---

## Tests

### Tests Locaux (Sans Docker)

```bash
# Test wrapper Python
python test_local_simple.py

# Test avec Jupyter
jupyter notebook notebooks/test_hubeau_wrapper.ipynb
```

### Tests Dagster (Avec Docker)

```bash
# Tester un asset spécifique
dagster asset materialize -a temperature_stations_reference

# Tester un job complet
dagster job execute -j hubeau_temperature_job

# Dry-run (affiche le plan sans exécuter)
dagster asset materialize -a temperature_stations_reference --dry-run
```

### Vérifier une Configuration DLT

```python
from dlt_pipeline.hubeau_generic import validate_config

validate_config('configs/hubeau/nouvelle_api.yml')
```

---

## Documentation

### Code

- **Docstrings** : Toutes les fonctions publiques
- **Comments** : Expliquer le "pourquoi", pas le "quoi"
- **Type hints** : Recommandés

### Configuration YAML

- **Commentaires** : Expliquer les choix techniques

```yaml
slicing:
  strategy: "station_month_chunked"  # Nécessaire car API limite à 20K records/requête
  chunk_months: 6  # Optimisé pour stations actives (500 records/mois en moyenne)
```

### Documentation Markdown

Mettre à jour si votre changement :
- Ajoute/supprime une API
- Change l'architecture
- Modifie le workflow de dev
- Ajoute des dépendances

**Fichiers à considérer** :
- `README.md` : Vue d'ensemble
- `docs/TUTORIEL_DLT.md` : Guide DLT
- `docs/ARCHITECTURE.md` : Architecture technique

---

## Merge Requests

### Checklist avant MR

- [ ] Code testé localement
- [ ] Commits suivent Conventional Commits
- [ ] Documentation mise à jour si nécessaire
- [ ] Pas de credentials hardcodés
- [ ] Pas de fichiers temporaires/logs committed

### Template MR

```markdown
## Description

[Description claire du changement]

## Type de changement

- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Tests effectués

- [ ] Tests locaux
- [ ] Tests Dagster UI
- [ ] Tests CI/CD

## Checklist

- [ ] Code suit les standards du projet
- [ ] Documentation mise à jour
- [ ] Pas de breaking changes (ou documentés)
```

---

## Questions ?

- **Issues** : https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/issues
- **Documentation** : [docs/](docs/)
- **Contact** : [Programme JUNON](https://www.junon-cvl.fr/fr/contact)

---

**Merci de contribuer ! 🎉**
