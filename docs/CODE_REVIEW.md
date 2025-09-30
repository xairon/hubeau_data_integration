# Code Review - Hub'Eau Data Integration Pipeline

## 🔍 Synthèse

Cette revue met en évidence les points bloquants et les améliorations prioritaires pour rendre le pipeline plus robuste et aligné sur les bonnes pratiques observées dans les projets de référence du domaine (ex. `cl-hubeau`). Les sections suivantes sont classées par criticité.

## 🚨 Problèmes critiques (à corriger avant mise en production)

1. **Variables d'environnement non résolues dans les ressources Dagster**
   Les connexions PostgreSQL, Neo4j et S3 utilisent des littéraux `"{{ env.VAR }}"` dans la configuration `configured`. Dagster n'interprète pas cette syntaxe, ce qui construit des DSN invalides (`postgresql://postgres:{{ env.PG_PASSWORD }}@...`) et empêche toute connexion aux bases. Il faut remplacer ces chaînes par des `EnvVar` ou gérer la lecture d'environnement dans la fonction ressource.【F:src/hubeau_pipeline/resources.py†L70-L87】

2. **Recursion asynchrone sous sémaphore ⇒ risque de deadlock**  
   Dans `fetch_chunk`, lorsqu'une requête échoue on rappelle récursivement la même coroutine tout en conservant le jeton du sémaphore (`async with semaphore`). La récursion tente de réacquérir le sémaphore et se bloque. Il faut libérer le jeton avant la récursion (ex. découper la logique en fonction auxiliaire) ou utiliser une file itérative.【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L392-L436】

3. **Paramétrage erroné des endpoints hydrométrie**  
   Les endpoints `observations_tr` et `obs_elab` sont marqués `requires_spatial_filter=True` et injectent `code_departement`. L'API hydrométrie n'accepte pourtant pas ce filtre : elle requiert `code_entite`/`code_station`. Le code ne passe donc jamais par la branche "entity codes" et produit des requêtes invalides. Il faut retirer `requires_spatial_filter` et s'aligner sur l'approche par entités (cf. stratégie `cl-hubeau`).【F:src/hubeau_pipeline/assets/bronze/hubeau_configs.py†L52-L72】【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L365-L455】

4. **Sensibilité excessive aux indisponibilités MinIO**  
   `HubeauIngestionService` instancie un client MinIO au démarrage et lève une exception fatale si le service n'est pas accessible. Cela bloque tout développement local ou exécution de tests sans MinIO. Prévoir un fallback (mock, stockage disque) ou déléguer la connexion à une ressource Dagster injectable qui gère la tolérance aux pannes.【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L541-L565】

5. **Sensors branchés sur un job inexistant**  
   Les capteurs `error_detection_sensor` et `hubeau_freshness_sensor` importent `hubeau_daily_job`, qui n'est défini nulle part dans le package `jobs`. Le chargement des définitions Dagster échoue donc. Corriger l'import en ciblant un job réel (`hubeau_bronze_job` ?) ou créer le job manquant.【F:src/hubeau_pipeline/sensors/error_detection.py†L6-L36】【F:src/hubeau_pipeline/sensors/data_freshness.py†L6-L32】

6. **Incohérence des buckets S3**
   Les ressources configurent un bucket `bronze`, mais l'ingestion écrit dans `hubeau-bronze`. Les jobs BDLISA utilisent encore un bucket `bdlisa-bronze`. Cette divergence complique le provisioning et peut masquer des erreurs de permissions. Harmoniser les noms ou rendre le bucket configurable via ressource.【F:src/hubeau_pipeline/resources.py†L48-L87】【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L541-L655】

7. **Résumé d'ingestion orphelin et assets non chaînés**
   `hubeau_ingestion_summary` n'a aucune dépendance sur les autres assets bronze et renvoie un message statique. Il est pourtant sélectionné par plusieurs jobs ; il ne synthétise donc rien et retourne toujours `total_records_ingested = 0`, ce qui peut faire croire à une ingestion vide. Il faut en faire un asset multi-dépendant ou supprimer sa sélection des jobs de production.【F:src/hubeau_pipeline/assets/bronze/hubeau_assets.py†L74-L113】【F:src/hubeau_pipeline/jobs/bronze_ingestion.py†L9-L74】

8. **Jobs Dagster incomplets pour les capteurs**
   Les sensors continuent d'importer `hubeau_daily_job` qui n'existe pas, mais la définition `Definitions` charge `all_jobs` contenant les jobs bronze. Dagster échoue donc dès le chargement des sensors. Mettre à jour les sensors pour pointer vers un job réel (`hubeau_bronze_job`) ou supprimer leur enregistrement tant qu'ils ne sont pas prêts.【F:src/hubeau_pipeline/sensors/data_freshness.py†L1-L35】【F:src/hubeau_pipeline/sensors/error_detection.py†L1-L35】【F:src/hubeau_pipeline/definitions.py†L1-L24】

## ⚠️ Problèmes majeurs

1. **Sélection d'entités incomplète**  
   Pour plusieurs APIs, la liste des codes dérivée des stations ne couvre pas toutes les clés (`code_station_hydrobio`, `bss_id`, etc.). S'inspirer des extracteurs dédiés de `cl-hubeau` permettrait de structurer des méthodes par API et de valider systématiquement les paramètres.【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L592-L655】

2. **Manque d'injection de dépendances**  
   Les services HTTP, PostgreSQL, MinIO… sont instanciés directement dans les assets au lieu d'utiliser les ressources Dagster. Outre la testabilité limitée, cela duplique la configuration et empêche la mutualisation des clients (contrairement à l'approche `BaseHubeauSession` partagée dans `cl-hubeau`). Envisager une couche de services réutilisables alimentés par les ressources.【F:src/hubeau_pipeline/assets/bronze/hubeau_assets.py†L17-L63】【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L541-L655】

3. **Surparamétrage statique des limites**
   Les valeurs `page_size`, `max_pages` et `depth_limit` sont codées en dur et parfois incohérentes (ex. limite à 100 000 mais `max_pages`=50 ⇒ 50 000 max). `cl-hubeau` interroge la profondeur réelle de l'API via les en-têtes et adapte la pagination. Introduire une logique qui s'appuie sur les métadonnées (`count`, `next`) éviterait des pertes de données.【F:src/hubeau_pipeline/assets/bronze/hubeau_configs.py†L34-L189】【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L200-L273】

4. **Gestion d'erreur minimaliste**
   Beaucoup de `except Exception` se contentent de logguer sans remonter l'erreur ni taguer l'asset comme `FAILURE`, ce qui peut masquer des pertes de données. Mettre en place des exceptions typées et un reporting structuré (ex. via `DagsterEventMetadata`) pour suivre les tentatives ratées.【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L200-L447】

5. **Résumé de partition future**
   Les assets bronze sont partitionnés à partir du `2024-09-01`. En environnement de production on attend un backfill historique ; avec cette configuration il est impossible de rejouer les partitions précédant septembre 2024. Démarrer la partition à une date configurable (via settings Dagster) ou fournir une `start_date` réaliste.【F:src/hubeau_pipeline/assets/bronze/hubeau_assets.py†L14-L70】

## 💡 Améliorations recommandées

1. **Factoriser un client Hub'Eau commun**  
   `cl-hubeau` propose une classe `BaseHubeauSession` mutualisant retries, pagination et validation. Reprendre ce principe (en asynchrone si nécessaire) permettrait de réduire la duplication entre `utils.get_with_backoff` et `HubeauClient` et de faciliter l'ajout d'APIs.【F:src/hubeau_pipeline/utils.py†L1-L116】【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L1-L219】

2. **Utiliser des modèles Pydantic par endpoint**  
   Au lieu d'un modèle générique `HubeauObservation`, définir des modèles spécifiques par API (hydrométrie, piézométrie, etc.) pour bénéficier de la validation de schéma et documenter les champs. `cl-hubeau` expose des DataFrames typed ; on peut obtenir un résultat similaire avec Pydantic et `TypedDict`.

3. **Tests automatisés et données de fixtures**  
   Aucun test n'est présent alors que la complexité métier est élevée. Ajouter des tests unitaires pour la pagination, la validation des paramètres et la sérialisation MinIO (en s'inspirant de la couverture de `cl-hubeau`) sécuriserait les refontes.

4. **Observabilité Dagster**  
   Exploiter `context.log.event` et `context.add_output_metadata` pour publier les métriques (`metrics.dict()`) directement dans Dagster plutôt que via de simples logs. Cela facilitera les dashboards et alertes.

## ✅ Priorisation proposée

1. Corriger les points bloquants (ressources, sémaphores, configuration hydrométrie, MinIO, sensors, buckets).
2. Revoir la gestion des dépendances et la configuration dynamique des endpoints.
3. Introduire une librairie cliente mutualisée + tests inspirés de `cl-hubeau`.
4. Déployer la télémétrie et la documentation technique associée.

En suivant ces étapes, on rapproche le pipeline d'une architecture "state of the art" tout en capitalisant sur les bonnes pratiques déjà éprouvées dans l'écosystème Hub'Eau.

## 🔬 Audit des assets, jobs et capteurs

- **Couverture des assets bronze.** Tous les assets `hubeau_*_bronze` appellent `ingest_hubeau_api`, mais la configuration est reconstruite à chaque appel et aucune validation n'est faite avant d'indexer `configs[api_name]`. Une faute de frappe dans le nom de l'asset provoquerait un `KeyError` non géré. Centraliser les configs dans une ressource ou ajouter une validation explicite éviterait des crashs silencieux.【F:src/hubeau_pipeline/assets/bronze/hubeau_assets.py†L17-L73】
- **Jobs alignés mais non testés.** Les jobs bronze sélectionnent bien les assets correspondants, mais aucun test ni `asset_checks` ne garantit que la sélection reflète la réalité métier (ex. `hubeau_summary_job` n'a qu'un asset placeholder). Ajouter des tests d'intégration Dagster comme dans `cl-hubeau` sécuriserait les refactorings.【F:src/hubeau_pipeline/jobs/bronze_ingestion.py†L9-L89】【F:tests/test_hubeau_ingestion_service.py†L1-L79】
- **Capteurs bloquants.** Tant que `hubeau_daily_job` n'est pas recréé, il vaut mieux désactiver les sensors pour éviter que Dagster crashe en import. `cl-hubeau` laisse les sensors dans un module optionnel non chargé par défaut ; reproduire ce pattern simplifie la maintenance.【F:src/hubeau_pipeline/sensors/data_freshness.py†L1-L35】【F:src/hubeau_pipeline/sensors/error_detection.py†L1-L35】

## 🧭 Paramètres API & conformité fonctionnelle

- **Hydrométrie – filtres invalides.** Les endpoints `observations_tr` et `obs_elab` sont configurés avec `requires_spatial_filter=True` et injectent `code_departement`. L'API officielle attend `code_entite`/`code_station` : l'appel actuel génère des requêtes vides ou 400. `cl-hubeau` segmente par `code_entite` ; il faut aligner la configuration et la logique de chunking.【F:src/hubeau_pipeline/assets/bronze/hubeau_configs.py†L24-L73】【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L300-L387】
- **Qualité cours d'eau – surcharge départementale.** `operation_pc`, `condition_environnementale_pc` et `analyse_pc` imposent un filtre département alors que l'API tolère directement `code_station`. Cette contrainte multiplie les requêtes et dépasse vite les quotas. S'inspirer du découpage par station de `cl-hubeau` réduirait la charge tout en respectant la pagination native.【F:src/hubeau_pipeline/assets/bronze/hubeau_configs.py†L74-L143】【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L312-L455】
- **Qualité nappes – mauvais paramètre.** La configuration utilise `num_departement` alors que la documentation Hub'Eau référence `code_departement`. Les requêtes retournent donc des erreurs 400. Corriger la clé du paramètre spatial est indispensable.【F:src/hubeau_pipeline/assets/bronze/hubeau_configs.py†L144-L189】
- **Hydrobiologie – chunking incohérent.** L'endpoint `taxons` exige `code_station_hydrobio`, pourtant la configuration force un filtre département. La fonction `get_observations` bascule alors sur la branche spatiale et n'utilise jamais les codes station, ce qui perd des données. Il faut aligner `requires_spatial_filter=False` et alimenter `code_station_hydrobio` depuis les stations comme le fait `cl-hubeau`.【F:src/hubeau_pipeline/assets/bronze/hubeau_configs.py†L190-L243】【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L312-L455】
- **Prélèvements – fenêtre temporelle annuelle.** `ingest_api_data` construit une fenêtre `[annee, annee]` mais les chroniques sont pluriannuelles. Il faut couvrir l'année complète (ex. `annee_min=year`, `annee_max=year+1`) ou suivre l'approche `cl-hubeau` qui ré-ingère les séries complètes avant d'agréger.【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L251-L347】【F:src/hubeau_pipeline/assets/bronze/hubeau_configs.py†L244-L283】

## ⚙️ Chunking & parallélisation

- **Deadlock sur semaphores.** `fetch_chunk` rappele récursivement la coroutine depuis le bloc `async with semaphore`. La seconde tentative tente de réacquérir le même sémaphore et reste bloquée, empêchant l'ingestion complète. Il faut extraire la logique de retry hors du contexte ou utiliser une pile itérative comme dans `cl-hubeau` (`gather_with_concurrency`).【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L360-L437】
- **Pas de borne sur le fan-out.** La création de tâches `asyncio.gather` sur les départements n'impose aucune limite globale autre que le sémaphore local (jusqu'à 15 requêtes simultanées). Avec huit assets en parallèle, on peut dépasser largement les quotas Hub'Eau. `cl-hubeau` sérialise les appels par API et inspecte les en-têtes `X-RateLimit-Remaining`. Ajouter un ordonnanceur global ou utiliser `asyncio.Semaphore` partagé par service éviterait ces rafales.【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L312-L455】
- **Chunk adaptatif incomplet.** L'algorithme ne tient compte que du nombre de codes, pas de la taille de réponse. Si un chunk génère >50 000 lignes, la pagination s'arrête silencieusement (`max_pages=20`). `cl-hubeau` détecte `response.count` pour re-segmenter automatiquement ; reproduire cette logique empêcherait la perte de données.【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L200-L347】

## ⏱️ Respect des quotas & résilience

- **Pas de gestion explicite du 429.** `AsyncRetrying` ne cible que `httpx.HTTPError`/`Timeout`. Les réponses 429 Hub'Eau lèvent `HTTPStatusError` avec code 429 mais ne sont pas distinguées ni accompagnées d'un `Retry-After`. Ajouter un handler dédié (comme dans `cl-hubeau`) permettrait de patienter le bon délai et d'éviter le bannissement.【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L80-L199】
- **Rate limit statique.** `rate_limit_delay` est fixé à 0,5 s pour toutes les APIs, sans tenir compte des quotas spécifiques (hydrobiologie est limitée à ~3 req/s, hydrométrie à 10 req/s). Un délai unique n'empêche pas les dépassements quand plusieurs tasks tournent en parallèle. Externaliser les limites par endpoint (ou lire `Retry-After`) garantirait le respect des SLA.【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L80-L199】【F:src/hubeau_pipeline/assets/bronze/hubeau_configs.py†L24-L283】
- **Absence de backpressure MinIO.** Le service écrit toutes les données brutes dans des fichiers JSON uniques. Pour les API volumineuses (hydrométrie), cela dépasse la mémoire avant même la sauvegarde. Prévoir un streaming vers MinIO ou une écriture chunkée inspirée de `cl-hubeau` (qui produit des Parquet paginés) sécuriserait l'exécution.【F:src/hubeau_pipeline/assets/bronze/hubeau_client.py†L541-L655】
