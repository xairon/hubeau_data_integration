# Stratégie de stockage & gouvernance des données

Dernière vérification : 2024-09-30

Cette note décrit la structure de stockage Bronze, la politique de rétention et les contrôles qualité associés. Elle s'applique aux environnements locaux et aux déploiements sur l'infrastructure BRGM.

---

## 1. Buckets & répertoires

### 1.1 MinIO / S3 (défaut)

- **Bucket** : `bronze` (configurable via `MINIO_BRONZE_BUCKET`).
- **Structure** : `s3://<bucket>/<api>/<partition>/`.
  - `ingestion_metadata.json` : résumé global de la partition (statut, métriques, erreurs).
  - `<endpoint>_data.json` : payload brut pour chaque endpoint (stations, observations, analyses...).
  - `<endpoint>_metadata.json` : informations techniques (nombre d'enregistrements, date d'exécution).
- **Versionning** : activer le versionning S3 pour tracer les corrections (recommandé en production).

### 1.2 Fallback local

- **Répertoire** : `HUBEAU_LOCAL_CACHE` (défaut `./data/hubeau_bronze`).
- **Structure miroir** : identique au bucket S3 pour faciliter les diff et rechargements.
- **Utilisation** : activé automatiquement si la connexion MinIO échoue. Les logs Dagster mentionnent le chemin de fallback.

---

## 2. Gouvernance des partitions

| Type d'asset | Partition | Exemple de chemin |
| --- | --- | --- |
| Hydrométrie (30 jours) | `recent_30days` | `bronze/hydrometry/recent_30days/ingestion_metadata.json` |
| Quotidien (`DailyPartitionsDefinition`) | `YYYY-MM-DD` | `bronze/piezometry/2024-09-29/chroniques_data.json` |
| Mensuel (`YYYY-MM`) | `YYYY-MM` | `bronze/onde/2024-08/observations_data.json` |
| Annuel | `YYYY` | `bronze/qualite_rivieres/2024/analyse_pc_data.json` |

> Les partitions futures sont automatiquement ignorées (statut `skipped_future_partition`). Une partition « vide » génère tout de même `ingestion_metadata.json` avec `status=no_data`.

---

## 3. Rétention & archivage

| Environnement | Rétention recommandée | Archivage |
| --- | --- | --- |
| Local développeur | 90 jours (purge manuelle des partitions anciennes) | Export ponctuel vers disque externe/Serveur NAS. |
| Plateforme BRGM | 2 ans minimum pour les données Bronze | Sauvegarde quotidienne du bucket (MinIO client `mc mirror`). |
| Cloud | Selon politique institutionnelle | Utiliser lifecycle policies S3 (transition vers Glacier/Deep Archive après 1 an). |

Les couches Silver/Gold héritent des politiques de TimescaleDB/Neo4j (dump quotidien + snapshots hebdomadaires).

---

## 4. Contrôles qualité

1. **Complétude** : vérifiez que chaque ingestion produit `ingestion_metadata.json`. L'absence du fichier signale une erreur non gérée.
2. **Volumes** : suivre `total_records_ingested` pour détecter des variations anormales (rupture de série).
3. **Erreurs** : toute erreur est listée dans le champ `errors` de `ingestion_metadata.json` et dans les logs Dagster. Documenter la résolution.
4. **Schéma** : lors de changements Hub'Eau (nouvelles colonnes), mettre à jour les transformations Silver/Gold et documenter dans `DATA_SOURCES_COMPLETE.md`.

---

## 5. Rechargements & corrections

1. **Identifier la partition** à rejouer (ex : `piezometry`, `2024-09-10`).
2. **Purger** la partition dans MinIO (`mc rm --recursive --force s3/bronze/piezometry/2024-09-10`).
3. **Relancer** l'asset concerné via Dagster (`materialize --partition ...`).
4. **Documenter** l'opération dans le journal scientifique (raison, impact).

---

## 6. Exposition des données

- **Partage interne** : utiliser MinIO Console (droits lecture seule) ou synchroniser le bucket vers un NAS interne.
- **Diffusion publique** : prévoir une étape d'anonymisation/filtrage selon les licences Hub'Eau et les contraintes de diffusion.
- **API interne** : future exposition via FastAPI/TimescaleDB (voir `SOSA_FUTURE_VISION.md`).

---

Le respect de cette stratégie garantit la traçabilité scientifique et facilite les audits des travaux Hub'Eau.
