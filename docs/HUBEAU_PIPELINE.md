# Pipeline Hub'Eau – Détails d'implémentation

Dernière vérification : 2024-09-30

Ce document décrit le fonctionnement interne du pipeline Hub'Eau : client HTTP, service d'ingestion, assets Dagster, jobs et tests. Il fournit également les paramètres clés à ajuster selon les études scientifiques.

---

## 1. Client Hub'Eau (`HubeauClient`)

Emplacement : `src/hubeau_pipeline/assets/bronze/hubeau_client.py`

### 1.1 Caractéristiques principales

- **Client httpx asynchrone** avec timeout configurable (`config.timeout`, défaut 60s).
- **Retry exponentiel avec jitter** (tenacity) sur les erreurs réseau ou HTTP (`HTTPError`, `TimeoutException`).
- **Sémaphore global** (`GLOBAL_HUBEAU_SEMAPHORE = 10`) pour plafonner les requêtes concurrentes sur l'ensemble des assets.
- **Pagination hybride** : support natif de la pagination page/offset et de la pagination par curseur (`supports_cursor=True`).
- **Validation Pydantic** des réponses (`HubeauApiResponse`, `HubeauStation`, `HubeauObservation`). Les listes vides sont autorisées pour représenter des fenêtres sans données.
- **Métriques d'ingestion** : chaque requête alimente `IngestionMetrics` (nombre de requêtes, volume retourné, durée cumulative) utilisé pour l'audit.

### 1.2 Configuration (`HubeauApiConfig`, `HubeauEndpointConfig`)

- Définies dans `hubeau_configs.py`.
- Paramètres clés : `page_size`, `max_pages`, `supports_cursor`, `temporal_params`, `spatial_params`, `requires_spatial_filter`, `rate_limit_delay` (délai fixe entre deux requêtes).
- Les endpoints exposent `end_offset_days` pour gérer des fenêtres fermées/exclusives (ex : hydrobiologie +1 jour).

### 1.3 Gestion des erreurs

- Toute erreur de page est encapsulée dans `HubeauPageFetchError` et fait échouer l'ingestion si `bubble_exceptions=True`.
- Les logs redigent les paramètres sensibles (`page`, `cursor`) mais conservent les filtres pour faciliter le debugging.

---

## 2. Service d'ingestion (`HubeauIngestionService`)

### 2.1 Responsabilités

- Préparer la configuration MinIO/S3 ou le fallback local (`./data/hubeau_bronze`).
- Récupérer séquentiellement **stations** puis **observations** pour chaque endpoint pertinent.
- Enregistrer systématiquement les métadonnées d'exécution, y compris pour les partitions sans données (`status = "no_data"`).
- Propager un statut `partial_success` en cas d'erreurs partielles (stations KO mais observations OK, etc.).

### 2.2 Structure de sortie

```json
{
  "execution_date": "2024-09-30T10:15:00",
  "partition_date": "2024-09-29",
  "api_name": "piezometry",
  "total_records_ingested": 12345,
  "results_by_endpoint": {
    "stations": {"records_count": 200, "data": [...]},
    "chroniques": {"records_count": 12145, "data": [...]}},
  "status": "success",
  "metrics": {"request_count": 12, "bytes_downloaded": 5_200_000},
  "errors": []
}
```

Ces structures sont écrites telles quelles dans `ingestion_metadata.json` pour assurer un audit complet.

### 2.3 Stockage

- **MinIO** : bucket `bronze`, clés `api/partition/`.
  - `ingestion_metadata.json`
  - `<endpoint>_data.json`
  - `<endpoint>_metadata.json`
- **Fallback local** : miroir de la structure MinIO sous `HUBEAU_LOCAL_CACHE` (défaut `./data/hubeau_bronze`).

---

## 3. Assets Dagster

Emplacement : `src/hubeau_pipeline/assets/bronze/hubeau_assets.py`

### 3.1 Partitionnement

| Asset | Partition | Fenêtre Hub'Eau | Remarques |
| --- | --- | --- | --- |
| `hubeau_hydrometry_bronze` | Pas de partition (fenêtre glissante 30 jours) | API hydrométrie v2 – limitation officielle Hub'Eau | Calcul automatique des 30 derniers jours à chaque run. |
| `hubeau_piezometry_bronze` | Quotidienne (`DailyPartitionsDefinition`) | Données temps réel + historique | Ignore les dates futures, partition clé format `YYYY-MM-DD`. |
| `hubeau_temperature_bronze` | Quotidienne | Temps réel stations thermométriques | Idem piézométrie. |
| `hubeau_onde_bronze` | Mensuelle (utilise `StaticPartitionsDefinition` avec `YYYY-MM`) | Campagnes estivales ONDE | Les partitions futures sont ignorées par sécurité. |
| `hubeau_water_quality_surface_bronze` | Annuelle (`StaticPartitionsDefinition`) | Campagnes physico-chimiques surface | Combinaison station + analyses. |
| `hubeau_water_quality_groundwater_bronze` | Annuelle | Analyses chimie nappes | Filtre département obligatoire. |
| `hubeau_hydrobiology_bronze` | Annuelle | Indices biologiques et taxons | Gestion des codes stations spécifiques. |
| `hubeau_prelevements_bronze` | Annuelle | Déclarations volumes prélevés | Agrégation par ouvrage. |

> Les partitions disponibles sont déclarées dans `YEARLY_PARTITIONS` et doivent être mises à jour chaque nouvelle campagne.

### 3.2 Jobs & Schedules

- `jobs/bronze_ingestion.py` : compose les assets Bronze en jobs (`hubeau_bronze_daily_job`, `hubeau_bronze_yearly_job`).
- `schedules/` :
  - `bronze_daily_schedule` (execution quotidienne à 04:00 UTC).
  - `bronze_yearly_schedule` (exécution annuelle début janvier pour l'année précédente).
- Les schedules utilisent les filtres Dagster pour matérialiser automatiquement les partitions manquantes.

### 3.3 Tags & Concurrence

Tous les assets Bronze portent `tags={"api": "hubeau"}`. Cela permet d'appliquer des règles de taux (`run_queue_config`) et d'éviter d'exécuter simultanément plus d'un asset Hub'Eau par instance Dagster si besoin.

---

## 4. Tests & assurance qualité

- `tests/test_hubeau_client.py` : couvre la pagination, l'extraction des curseurs, la gestion des erreurs HTTP.
- `tests/test_hubeau_ingestion_service.py` : vérifie la persistance MinIO/local, la production des métadonnées et les statuts (`no_data`, `partial_success`).
- Les fixtures sous `tests/fixtures/` répliquent des extraits JSON Hub'Eau officiels pour garantir la conformité.
- `pytest` est exécuté dans CI et doit passer sans accès réseau.

---

## 5. Paramétrages scientifiques

### 5.1 Filtres spatiaux

- Certains endpoints (`requires_spatial_filter=True`) imposent de passer par une boucle départementale (`dept`, `code_departement`). Le service applique automatiquement la liste complète des départements métropolitains + DROM.
- Ajuster la liste dans `HubeauIngestionService._get_french_departments()` si des études nécessitent des territoires additionnels.

### 5.2 Fenêtres temporelles

- Les paramètres `temporal_params` définissent les champs début/fin à envoyer.
- Pour les assets annuels, l'ingestion envoie `[YYYY-01-01, YYYY-12-31]` (ou `end_offset_days` si l'API attend une borne exclusive).
- Les assets quotidiens utilisent la partition comme fenêtre `[day, day+1)`.

### 5.3 Respect des limites Hub'Eau

- `rate_limit_delay` (par défaut 0.5s) est paramétrable par API.
- `max_retries` (3 tentatives) doit rester bas pour éviter les boucles longues en cas de maintenance Hub'Eau.
- Pour des études intensives, planifier les runs hors heures de bureau et prévenir l'équipe Hub'Eau si la charge augmente.

---

## 6. Extension du pipeline

1. **Ajouter un endpoint** : étendre `HubeauEndpointConfig` (ex : nouveaux paramètres) et mettre à jour `DATA_SOURCES_COMPLETE.md`.
2. **Nouveau type d'asset** : créer un asset Bronze/Silver/Gold dans `src/hubeau_pipeline/assets/` et déclarer son partitionnement.
3. **Transformation Silver** : utiliser `psycopg` via la ressource `pg` pour insérer les données Bronze dans TimescaleDB (procédure décrite dans `DATA_STORAGE_STRATEGY.md`).
4. **Analyses Gold** : voir `SOSA_FUTURE_VISION.md` pour aligner les entités sur l'ontologie SOSA et alimenter Neo4j.

---

## 7. Points de vigilance

- Surveiller régulièrement les changements de schéma des APIs Hub'Eau (nouvelles colonnes, champs dépréciés).
- Documenter toute modification dans la configuration (nouveau filtre, changement de `page_size`).
- Vérifier les volumes MinIO : un accroissement soudain peut indiquer une évolution du format Hub'Eau.

Ce document doit être relu à chaque ajout de fonctionnalité pour garantir l'adéquation entre la documentation et le code.
