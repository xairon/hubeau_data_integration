# Hub'Eau Ingestion Pipeline

> Pipeline scientifique Dagster pour collecter, documenter et fiabiliser les jeux de données hydrologiques Hub'Eau.

Ce dépôt fournit tout le nécessaire pour qu'un·e chercheur·e ou ingénieur·e data puisse **ingérer les APIs Hub'Eau**, vérifier les jeux de données et préparer les couches Bronze/Silver/Gold du BRGM. La documentation détaille les choix d'architecture, chaque source Hub'Eau référencée et les perspectives de recherche (couches analytiques SOSA, knowledge graph).

---

## 🧭 Objectifs

- Offrir un **client Hub'Eau asynchrone résilient** (httpx + tenacity) avec pagination par curseur et métriques d'observabilité.
- Orchestrer les **assets Dagster** Bronze avec des partitions adaptées (jour, mois, année) et une gouvernance claire.
- Assurer une **traçabilité complète des données** : stations, observations, métadonnées d'ingestion, audit MinIO/local.
- Proposer une **documentation exhaustive** pour comprendre les choix scientifiques, reproduire les expérimentations et préparer les futures couches analytiques (SOSA, knowledge graph BRGM).

---

## 🚀 Démarrage rapide (tutoriel complet)

### 1. Pré-requis

- Python 3.11+
- Docker + Docker Compose (pour exécuter Dagster/MinIO/Postgres/Neo4j en local)
- `make` (optionnel) pour lancer les commandes courantes
- Accès réseau au portail [Hub'Eau](https://hubeau.eaufrance.fr/page/apis) et, si besoin, aux proxys institutionnels

### 2. Cloner et configurer l'environnement

```bash
git clone https://github.com/<organisation>/brgm.git
cd brgm
cp env.example .env  # renseigner les mots de passe MinIO/Postgres/Neo4j
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> ℹ️ Le fichier `.env` est lu par `docker-compose.yml` et par les ressources Dagster (MinIO, TimescaleDB, Neo4j). Renseignez au minimum `MINIO_USER`, `MINIO_PASS`, `POSTGRES_PASSWORD`, `NEO4J_PASSWORD`.

### 3. Lancer la stack analytique complète

```bash
docker compose up -d
```

Services exposés :

| Service | URL | Description |
| --- | --- | --- |
| Dagster UI | http://localhost:8080 | Visualisation des assets, jobs, partitions |
| MinIO Console | http://localhost:9001 | Vérification des objets Bronze et métadonnées |
| TimescaleDB (Postgres) | localhost:5432 | Stockage relationnel (Silver) |
| PostGIS | localhost:5433 | Analyses spatiales avancées |
| Neo4j Browser | http://localhost:7474 | Préparation du knowledge graph (vision SOSA) |

### 4. Tester l'installation Python

```bash
pytest
```

La suite de tests couvre le client Hub'Eau, le service d'ingestion, la pagination par curseur et les scénarios de partitions vides.

### 5. Première ingestion depuis la CLI Dagster

Exemple : lancer l'asset piézométrie (partitions journalières) sur le 29 septembre 2024.

```bash
docker compose exec dagster_webserver \
  dagster asset materialize \
  --select hubeau_piezometry_bronze \
  --partition "2024-09-29"
```

Les métadonnées et les payloads bruts sont enregistrés dans MinIO (`s3://bronze/piezometry/2024-09-29/...`). Si MinIO est indisponible, un fallback automatique écrit sous `./data/hubeau_bronze`.

### 6. Inspecter les résultats

1. Ouvrez Dagster UI > Assets > `hubeau_piezometry_bronze` pour visualiser les métriques (`total_records_ingested`, durée, statut).
2. Vérifiez MinIO : dossier `piezometry/2024-09-29/` avec `ingestion_metadata.json`, `chroniques_data.json`, etc.
3. Analyse rapide dans un notebook :
   ```python
   import json, pathlib
   payload = json.loads(pathlib.Path("data/hubeau_bronze/piezometry/2024-09-29/chroniques_data.json").read_text())
   len(payload)
   ```

### 7. Programmer les collectes

- Les **jobs Dagster** groupent les assets par thématique (`hubeau_bronze_daily_job`, `hubeau_bronze_yearly_job`).
- Les **schedules** `bronze_daily_schedule`, `bronze_yearly_schedule` orchestrent respectivement les partitions quotidiennes et annuelles.
- Activez-les dans Dagster UI ou via le CLI (`dagster schedule start bronze_daily_schedule`).

---

## 🧱 Architecture résumée

```
Hub'Eau APIs ──▶ HubeauClient (httpx + retries) ──▶ HubeauIngestionService ──▶ MinIO (Bronze)
                          │                                   │
                          │                              Metadata JSON (audit)
                          ▼                                   ▼
               Dagster Assets & Jobs ─────────────────────▶ Schedules / Sensors
```

- **Client Hub'Eau** : `HubeauClient` applique un sémaphore global (10 requêtes simultanées) pour respecter les limites Hub'Eau, gère la pagination par curseur et journalise chaque tentative.
- **Service d'ingestion** : `HubeauIngestionService` collecte stations + observations, sérialise les métriques (`IngestionMetrics`), écrit les objets dans MinIO ou sur disque et retourne un statut explicite (`success`, `partial_success`, `no_data`, `error`).
- **Assets Dagster** : un asset par API majeure Hub'Eau. Les partitions sont adaptées à la nature des données :
  - Hydrométrie : asset non partitionné, toujours 30 derniers jours.
  - Piézométrie, température, ONDE : partitions quotidiennes.
  - Qualité eaux de surface/souterraine, hydrobiologie, prélèvements : partitions annuelles.
- **Stockage** : MinIO bucket `bronze` structuré `api_name/partition/`. Chaque run écrit `*_data.json`, `*_metadata.json` et `ingestion_metadata.json`.
- **Étapes suivantes** : transformation Silver (TimescaleDB/PostGIS) et modélisation Gold (Neo4j/SOSA) décrites dans `docs/`.

---

## 📚 Documentation complète

La connaissance fonctionnelle et technique est centralisée dans [`docs/`](docs/README.md) :

- `ARCHITECTURE_MODERNE.md` : diagramme logique, ressources Dagster, déploiements.
- `HUBEAU_PIPELINE.md` : fonctionnement détaillé du client, service d'ingestion, assets, tests.
- `DATA_SOURCES_COMPLETE.md` : fiche détaillée de chaque endpoint Hub'Eau (paramètres, partitions, liens officiels).
- `DATA_STORAGE_STRATEGY.md` : conventions MinIO, rétention, contrôles qualité.
- `SOSA_FUTURE_VISION.md` : feuille de route vers les couches analytiques et le knowledge graph.
- `CODE_REVIEW.md` : checklist scientifique/technique pour les PR.

Chaque document est tenu à jour après modification du code : les sections « Décisions d'architecture » listent le rationnel et les liens vers les tickets.

---

## 🛠️ Développement & personnalisation

### Variables d'environnement clés

| Variable | Rôle |
| --- | --- |
| `GLOBAL_HUBEAU_SEMAPHORE` | Limite de requêtes parallèles (par défaut 10) partagée entre toutes les APIs |
| `MINIO_ENDPOINT`, `MINIO_USER`, `MINIO_PASS`, `MINIO_BRONZE_BUCKET` | Cible MinIO/S3 pour les exports Bronze |
| `HUBEAU_LOCAL_CACHE` | Répertoire fallback si MinIO indisponible |
| `DAGSTER_HOME` | Configuration Dagster (workspace, schedules activées) |

### Ajouter une nouvelle API Hub'Eau

1. Déclarer la configuration dans `src/hubeau_pipeline/assets/bronze/hubeau_configs.py`.
2. Mettre à jour la table correspondante dans `docs/DATA_SOURCES_COMPLETE.md` (endpoints, paramètres).
3. Ajouter un asset Dagster dans `hubeau_assets.py` en choisissant la bonne `PartitionsDefinition`.
4. Ajouter le job/schedule si nécessaire et couvrir via tests (`tests/test_hubeau_client.py`).

### Bonnes pratiques

- Toujours lancer `pytest` avant de pousser une PR.
- Documenter le rationnel scientifique (filtrage, variables d'intérêt) directement dans la doc et les assets.
- Utiliser les tags Dagster (`api=hubeau`) pour réguler la concurrence lors d'exécutions multiples.

---

## 🩺 Observabilité & dépannage

- **Logs Dagster** : inspecter les steps `ingest_hubeau_api`. Les erreurs Hub'Eau sont surfacées dans les logs et renvoyées dans le payload (`errors`).
- **Alertes de partition vide** : un statut `no_data` signifie exécution réussie mais aucun enregistrement (fenêtre sans prélèvement). La métadonnée `ingestion_metadata.json` est toujours écrite.
- **Erreurs réseau** : vérifiez le fallback local. Si `data/hubeau_bronze/...` est rempli mais pas MinIO, la connexion S3 est en cause.
- **Respect du quota Hub'Eau** : ajustez `rate_limit_delay` et `max_retries` dans les configs API si les temps de réponse évoluent.

---

## 🤝 Contribution scientifique

1. Créez une branche (`feature/hydrobiologie-taxons`, `fix/hubeau-retries`).
2. Ajoutez tests + documentation.
3. Exécutez `pytest` et, si pertinent, un run Dagster sur un échantillon.
4. Ouvrez une PR avec un résumé technique/scientifique et mettez à jour les sections concernées dans `docs/`.

La checklist complète est disponible dans [`docs/CODE_REVIEW.md`](docs/CODE_REVIEW.md).

---

## 📬 Contact

Projet porté par l'équipe BRGM – Hub'Eau. Pour toute question ou besoin de calibration scientifique (nouvelles variables, protocoles de prélèvement), contacter `prenom.nom@brgm.fr`.

Bonnes analyses !
