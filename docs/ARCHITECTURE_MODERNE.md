# Architecture Hub'Eau – Vue d'ensemble

Dernière vérification : 2024-09-30

Ce document décrit l'architecture logicielle et infrastructurelle du pipeline Hub'Eau. Il doit permettre à un·e postdoc ou ingénieur·e de comprendre instantanément comment les briques interagissent et comment déployer l'ensemble dans différents contextes (local, laboratoire BRGM, cloud).

---

## 1. Composants principaux

| Couche | Description | Technologies |
| --- | --- | --- |
| **Orchestration** | Dagster orchestre les assets partitionnés, jobs et schedules. `src/hubeau_pipeline/definitions.py` agrège assets, jobs, schedules et resources. | Dagster 1.x |
| **Ingestion** | `HubeauClient` (httpx + tenacity) & `HubeauIngestionService` réalisent les appels Hub'Eau, gèrent la pagination, calculent les métriques et stockent les payloads. | Python 3.11, httpx, tenacity, Pydantic |
| **Stockage Bronze** | MinIO bucket `bronze`, structuré par API/partition. Fallback automatique vers `./data/hubeau_bronze` si MinIO indisponible. | MinIO/S3, JSON |
| **Stockage Silver** | Tables relationnelles (TimescaleDB/Postgres) alimentées par des assets Silver (non couverts dans ce dépôt, mais prévus). | TimescaleDB, Postgres |
| **Stockage Gold** | Knowledge graph (Neo4j) aligné sur l'ontologie SOSA/SSN. | Neo4j |
| **Observabilité** | Logs Dagster, métriques `IngestionMetrics` retournées par le service et stockées dans `ingestion_metadata.json`. | Dagster, JSON |

---

## 2. Flux de données

1. **Déclenchement** : un job Dagster (ex : `hubeau_bronze_daily_job`) exécute une liste d'assets Bronze.
2. **Partitionnement** : Dagster fournit la partition (jour/mois/année) et assure que les partitions futures sont ignorées.
3. **Ingestion** : `HubeauClient` enchaîne les requêtes avec sémaphore global (`GLOBAL_HUBEAU_SEMAPHORE=10`) et retries exponentiels. Les curseurs `next` sont extraits et rejoués jusqu'à épuisement des pages.
4. **Consolidation** : `HubeauIngestionService` fusionne stations et observations, calcule le total d'enregistrements, sérialise les métriques et remonte les erreurs éventuelles (`partial_success` si une portion échoue).
5. **Stockage** : écriture dans MinIO (`ingestion_metadata.json`, `*_data.json`, `*_metadata.json`). Si MinIO est indisponible, le service bascule sur le disque local.
6. **Consommation** : assets Silver/Gold et analyses scientifiques consomment les JSON Bronze pour produire des tables analytiques ou des graphes.

---

## 3. Ressources Dagster

`src/hubeau_pipeline/resources.py` expose les ressources partagées :

- `s3` : client MinIO initialisé via les variables d'environnement et injecté dans les assets Bronze.
- `http_client` (si besoin d'assets complémentaires) : pattern standard pour partager un client HTTP configuré.
- Ressources futures : connexions TimescaleDB, Neo4j, services d'alerting.

Les assets Bronze déclarent `required_resource_keys={"s3"}` pour bénéficier des connexions configurées dans `dagster_home`.

---

## 4. Déploiements

### 4.1 Environnement local (développement)

- **Docker Compose** (`docker-compose.yml`) lance : `dagster_webserver`, `dagster_daemon`, `postgres`, `timescaledb`, `minio`, `neo4j`.
- Le réseau `brgm` connecte toutes les briques. Les volumes locaux persistent les données (`./dagster_home`, `./data`).
- Utiliser cet environnement pour valider les jobs, écrire des tests et préparer les PR.

### 4.2 Serveurs internes BRGM

- Déploiement recommandé via Docker Compose ou Kubernetes (Helm Dagster + StatefulSets MinIO/Postgres/Neo4j).
- Configurer les accès réseau sortants (firewall) vers `https://hubeau.eaufrance.fr`.
- Mettre en place des sauvegardes MinIO (versioning) et Postgres (dump quotidien).

### 4.3 Cloud / HPC

- Possibilité de remplacer MinIO par un bucket S3 natif (AWS, OVH, Scaleway). Le service d'ingestion supporte n'importe quel endpoint compatible S3.
- Les assets Dagster peuvent être exécutés via `dagster-cloud` ou `dagster-k8s`. Prévoir un sémaphore global identique pour éviter de saturer Hub'Eau.

---

## 5. Décisions d'architecture clés

| Décision | Raison | Impact |
| --- | --- | --- |
| **Sémaphore global (10 requêtes)** | Hub'Eau limite implicitement la charge simultanée. Mutualiser la limite évite de saturer l'API lorsque plusieurs assets tournent. | Concurrency maîtrisée, moins de 429/500. |
| **Fallback local automatique** | Les environnements de recherche n'ont pas toujours MinIO/S3. Assurer une persistance locale évite toute perte de données. | Données toujours présentes, même offline. |
| **Partition annuelle pour campagnes** | Les APIs `qualite`, `hydrobiologie`, `prelevements` sont structurées autour de campagnes/plans annuels. | Simplifie l'orchestration, aligné sur la gouvernance Hub'Eau. |
| **Statut explicite (`no_data`, `partial_success`)** | Distinguer une partition vide d'une ingestion échouée. | Observabilité claire, métriques robustes. |
| **Tests hors-ligne** | Les tests Pytest utilisent des fixtures locales pour éviter les appels réseau, garantissant la reproductibilité. | CI fiable sans dépendance externe. |

---

## 6. Sécurité & conformité

- Les identifiants MinIO/Postgres/Neo4j sont stockés dans `.env` (non committé) et injectés via variables d'environnement.
- Les exports JSON Bronze ne contiennent pas de données personnelles. Vérifier néanmoins les obligations de diffusion (licence Hub'Eau CC-BY).
- Prévoir un chiffrement au repos (MinIO SSE) sur les environnements de production.

---

## 7. Checklist de mise en production

1. Vérifier la connectivité Hub'Eau (curl + tests limités) depuis l'environnement cible.
2. Configurer `GLOBAL_HUBEAU_SEMAPHORE` selon la politique d'usage (10 recommandé).
3. Créer le bucket MinIO/S3 (`bronze`) ou adapter `MINIO_BRONZE_BUCKET`.
4. Définir les schedules Dagster (daily, yearly) et activer la surveillance (Dagster sensors/alerting).
5. Mettre en place les sauvegardes MinIO/Postgres/Neo4j.
6. Documenter dans `DATA_SOURCES_COMPLETE.md` toute restriction spécifique (maintenance Hub'Eau, quotas additionnels).

---

Cette architecture est conçue pour être extensible et reproductible. Toute évolution (nouvelle API, nouveau stockage) doit être décrite ici pour maintenir une vision partagée du pipeline.
