# Guide de Migration : Infrastructure Hub'Eau Refactorisée

## 🎯 Vue d'ensemble des changements

Cette refonte majeure transforme l'infrastructure Hub'Eau avec 4 améliorations critiques :

1. **Séparation Orchestrator / Workers** : Architecture distribuée scalable
2. **Integration dagster-dlt** : Code 10x plus simple, observability automatique
3. **Data Quality** : Schema contracts et validators DLT
4. **Observability** : Prometheus + Grafana pour monitoring complet

---

## 📊 Comparaison Avant / Après

### Architecture

| Aspect | AVANT | APRÈS |
|--------|-------|-------|
| **Conteneurs** | 1 image monolithique (2GB) | 2 images (orchestrator 500MB + worker 2GB) |
| **Scaling** | Vertical seulement | Horizontal (1-N workers) |
| **Build time** | 10min (rebuild tout) | 3min orchestrator + 10min worker |
| **Deploy** | Downtime complet | Rolling updates workers |
| **Isolation** | Code métier peut crasher Dagster | Isolation complète |

### Code

| Aspect | AVANT (dlt_assets.py) | APRÈS (dlt_assets_refactored.py) |
|--------|----------------------|----------------------------------|
| **Lignes de code** | ~700 lignes par asset | ~50 lignes par asset |
| **Logging** | Manuel (context.log.info) | Automatique (dagster-dlt) |
| **Métriques** | Manuelles | Automatiques (Prometheus) |
| **Data lineage** | Pas de tracking | Automatique dans Dagster UI |
| **Retry logic** | Manuel | Automatique |
| **Schema evolution** | Non détecté | Détecté et logged |
| **Data quality** | Pas de validation | Validators + schema contracts |

### Observability

| Aspect | AVANT | APRÈS |
|--------|-------|-------|
| **Métriques** | Pas de Prometheus | ✅ 15+ métriques métier |
| **Dashboards** | Pas de Grafana | ✅ Dashboards temps réel |
| **Alerting** | Pas d'alertes | ✅ Prometheus Alertmanager |
| **Data quality** | Pas de tracking | ✅ Métriques de validation |
| **API monitoring** | Logs basiques | ✅ Latence, rate limiting, erreurs |

---

## 🚀 Migration Étape par Étape

### Étape 1 : Préparation (5 min)

```bash
# 1. Backup de l'ancien docker-compose
cp docker-compose.yml docker-compose.yml.backup

# 2. Vérifier que les nouvelles dépendances sont présentes
ls -la requirements-orchestrator.txt requirements-worker.txt

# 3. Créer les répertoires de monitoring
mkdir -p docker/monitoring/grafana/{dashboards,datasources}
```

### Étape 2 : Build des nouvelles images (15 min)

```bash
# Build orchestrator (léger, rapide)
docker build -f docker/orchestrator/Dockerfile -t hubeau-orchestrator:latest .

# Build worker (lourd, plus long)
docker build -f docker/worker/Dockerfile -t hubeau-worker:latest .

# Vérifier les tailles
docker images | grep hubeau
# hubeau-orchestrator:latest   ~500MB
# hubeau-worker:latest          ~2GB
```

### Étape 3 : Configuration Prometheus/Grafana (2 min)

```bash
# Les fichiers sont déjà créés :
# - docker/monitoring/prometheus.yml
# - docker/monitoring/grafana/datasources/prometheus.yml
# - docker/monitoring/grafana/dashboards/dashboard.yml

# Ajouter mot de passe Grafana dans .env
echo "GRAFANA_PASSWORD=your_secure_password" >> .env
```

### Étape 4 : Lancement de la nouvelle stack (5 min)

```bash
# Stop l'ancienne stack
docker-compose down

# Start la nouvelle stack
docker-compose up -d

# Vérifier que tous les conteneurs démarrent
docker-compose ps

# Vérifier les logs
docker-compose logs -f dagster_webserver
docker-compose logs -f dlt_worker
```

### Étape 5 : Vérification des services (5 min)

```bash
# 1. Dagster UI
open http://localhost:8080
# ✅ Vérifier que le workspace "hubeau_pipeline" est loadé

# 2. Prometheus
open http://localhost:9090
# ✅ Vérifier targets: Status > Targets
# ✅ Devrait voir: dagster-webserver, dlt-workers, minio, prometheus

# 3. Grafana
open http://localhost:3001
# Login: admin / <GRAFANA_PASSWORD>
# ✅ Vérifier datasource Prometheus est connecté

# 4. Métriques workers
curl http://localhost:9091/metrics
# ✅ Devrait voir les métriques Prometheus du worker
```

### Étape 6 : Test d'un asset refactorisé (10 min)

```bash
# Option A : Via Dagster UI
# 1. Aller sur http://localhost:8080
# 2. Naviguer vers Assets > piezometry_stations_refactored
# 3. Cliquer "Materialize"
# 4. Observer les métriques en temps réel

# Option B : Via CLI
docker exec dlt_worker dagster asset materialize -m hubeau_pipeline -a piezometry_stations_refactored
```

### Étape 7 : Migration progressive des assets (variable)

**Stratégie recommandée : Migration progressive**

1. **Garder les anciens assets** dans `dlt_assets.py`
2. **Créer de nouveaux assets** dans `dlt_assets_refactored.py`
3. **Migrer un asset à la fois** (commencer par les plus simples)
4. **Valider en production** avant de supprimer l'ancien

**Pattern de migration d'un asset :**

```python
# AVANT (dlt_assets.py)
@asset(name="piezometry_stations")
def piezometry_stations(context: AssetExecutionContext):
    # 100+ lignes de code manuel
    cfg = yaml.load(...)
    pipeline = dlt.pipeline(...)
    source = hubeau_rest_source(...)
    load_info = pipeline.run(source)
    # ... parsing, logging, métriques manuel

# APRÈS (dlt_assets_refactored.py)
@dlt_assets(
    name="piezometry_stations_v2",  # Nouveau nom pour coexistence
    dlt_source=hubeau_rest_source(...),
    dlt_pipeline=create_dlt_pipeline(...)
)
def piezometry_stations_v2_assets(context, dlt):
    yield from dlt.run(context=context)
```

---

## 📈 Métriques Prometheus Disponibles

### DLT Extraction
- `hubeau_dlt_records_extracted_total` : Nombre total de records extraits
- `hubeau_dlt_records_loaded_total` : Nombre total de records chargés
- `hubeau_dlt_extraction_duration_seconds` : Durée d'extraction
- `hubeau_dlt_load_duration_seconds` : Durée de chargement
- `hubeau_dlt_errors_total` : Nombre d'erreurs DLT

### API Hub'Eau
- `hubeau_api_requests_total` : Nombre total de requêtes
- `hubeau_api_rate_limit_hits_total` : Nombre de rate limits
- `hubeau_api_response_time_seconds` : Temps de réponse API

### Data Quality
- `hubeau_data_quality_check_failures_total` : Échecs de validation
- `hubeau_data_quality_invalid_records_total` : Records invalides

### Pipeline
- `hubeau_pipeline_runs_total` : Nombre d'exécutions
- `hubeau_pipeline_duration_seconds` : Durée d'exécution

### Storage
- `hubeau_minio_objects_written_total` : Objets écrits MinIO
- `hubeau_minio_bytes_written_total` : Bytes écrits MinIO

---

## 🔍 Exemples de Requêtes PromQL

### Dashboard suggestions

**1. Throughput d'extraction**
```promql
rate(hubeau_dlt_records_extracted_total[5m])
```

**2. Taux d'erreur**
```promql
rate(hubeau_dlt_errors_total[5m]) / rate(hubeau_api_requests_total[5m])
```

**3. Latence API P95**
```promql
histogram_quantile(0.95, rate(hubeau_api_response_time_seconds_bucket[5m]))
```

**4. Records invalides par source**
```promql
sum by (source, validation_rule) (hubeau_data_quality_invalid_records_total)
```

**5. Durée pipeline par partition**
```promql
hubeau_pipeline_duration_seconds{partition=~"2024.*"}
```

---

## 🛡️ Data Quality : Utilisation

### 1. Schema Contracts

```python
from hubeau_pipeline.data_quality import create_schema_contract

@dlt.source(
    schema_contract=create_schema_contract(
        mode="strict",        # "evolve" | "strict" | "freeze"
        freeze_columns=True,  # Refuse nouvelles colonnes
        freeze_tables=False   # Accepte nouvelles tables
    )
)
def my_source():
    ...
```

**Modes disponibles :**
- `evolve` : Accepte tous les changements (défaut)
- `strict` : Freeze types, accepte nouvelles colonnes
- `freeze` : Refuse tous les changements

### 2. Validators

```python
from hubeau_pipeline.data_quality import (
    validate_primary_keys,
    validate_coordinates,
    validate_date_range,
    validate_numeric_range
)

# Dans un transformer ou resource
for record in records:
    # Primary keys
    is_valid, error = validate_primary_keys(
        record,
        primary_keys=['code_bss', 'timestamp_mesure']
    )

    # Coordinates
    is_valid, error = validate_coordinates(
        record,
        lat_field='latitude',
        lon_field='longitude',
        allow_null=False
    )

    # Date range
    is_valid, error = validate_date_range(
        record,
        date_field='date_obs',
        min_date=datetime(2000, 1, 1),
        max_date=datetime.now()
    )

    # Numeric range
    is_valid, error = validate_numeric_range(
        record,
        field='niveau_nappe_ngf',
        min_value=-100,
        max_value=3000
    )
```

### 3. Transformers

```python
from hubeau_pipeline.data_quality import (
    normalize_dates,
    clean_numeric_fields,
    validate_and_flag_records,
    apply_standard_quality_checks
)

@dlt.resource
def my_resource():
    raw_data = extract_from_api()

    # Pipeline de transformation
    cleaned = normalize_dates(raw_data, fields=['date_obs'])
    cleaned = clean_numeric_fields(cleaned, fields=['resultat'])
    validated = validate_and_flag_records(
        cleaned,
        validator_func=validate_piezometry_record,
        drop_invalid=False  # Flag plutôt que drop
    )

    yield from validated

# OU version simplifiée :
@dlt.resource
def my_resource():
    raw_data = extract_from_api()

    yield from apply_standard_quality_checks(
        raw_data,
        source_name="piezometry",
        resource_name="chroniques",
        date_fields=["date_obs"],
        numeric_fields=["resultat"],
        validator_func=validate_piezometry_record
    )
```

---

## 🐛 Troubleshooting

### Problème : Worker ne démarre pas

```bash
# Vérifier les logs
docker-compose logs dlt_worker

# Causes possibles :
# 1. Port 4000 déjà utilisé
netstat -tlnp | grep 4000

# 2. Erreur dans worker_entrypoint.sh
docker exec dlt_worker bash -c "cat /app/scripts/worker_entrypoint.sh"

# 3. Problème de permissions
docker exec dlt_worker bash -c "ls -la /app/scripts/worker_entrypoint.sh"
```

### Problème : Dagster UI ne voit pas le workspace

```bash
# Vérifier workspace.yaml
docker exec dagster_webserver cat /app/dagster_home/workspace.yaml

# Vérifier connectivity GRPC
docker exec dagster_webserver nc -zv dlt_worker 4000

# Reload workspace
docker exec dagster_webserver dagster instance info
```

### Problème : Prometheus ne scrape pas les métriques

```bash
# Vérifier endpoint métriques
curl http://localhost:9091/metrics

# Vérifier config Prometheus
docker exec prometheus cat /etc/prometheus/prometheus.yml

# Vérifier targets dans Prometheus UI
open http://localhost:9090/targets
```

### Problème : Grafana ne se connecte pas à Prometheus

```bash
# Test connexion depuis Grafana container
docker exec grafana curl -f http://prometheus:9090/api/v1/query?query=up

# Vérifier datasource config
docker exec grafana cat /etc/grafana/provisioning/datasources/prometheus.yml
```

---

## 📚 Ressources

### Documentation
- **dagster-dlt** : https://docs.dagster.io/integrations/dlt
- **DLT Schema Contracts** : https://dlthub.com/docs/general-usage/schema-contracts
- **Prometheus Python Client** : https://github.com/prometheus/client_python
- **Grafana Provisioning** : https://grafana.com/docs/grafana/latest/administration/provisioning/

### Fichiers clés créés/modifiés
- `requirements-orchestrator.txt` : Dépendances orchestrator
- `requirements-worker.txt` : Dépendances worker
- `docker/orchestrator/Dockerfile` : Image orchestrator
- `docker/worker/Dockerfile` : Image worker
- `docker-compose.yml` : Stack complète refactorisée
- `dagster_home/workspace.yaml` : Config GRPC
- `docker/monitoring/prometheus.yml` : Config Prometheus
- `src/hubeau_pipeline/observability/metrics.py` : Instrumentation Prometheus
- `src/hubeau_pipeline/data_quality/validators.py` : Validators
- `src/hubeau_pipeline/data_quality/transformers.py` : Transformers DLT
- `src/hubeau_pipeline/assets/bronze/dlt_assets_refactored.py` : Exemple assets

---

## ✅ Checklist de Migration

### Pré-migration
- [ ] Backup `docker-compose.yml`
- [ ] Backup volumes Docker (`docker volume ls`)
- [ ] Documentation état actuel (assets, partitions, schedules)
- [ ] Test sur environnement de dev d'abord

### Build & Deploy
- [ ] Build orchestrator image
- [ ] Build worker image
- [ ] Vérifier tailles images
- [ ] Ajouter `GRAFANA_PASSWORD` dans `.env`
- [ ] `docker-compose down` (ancienne stack)
- [ ] `docker-compose up -d` (nouvelle stack)

### Vérification Services
- [ ] Dagster UI accessible (http://localhost:8080)
- [ ] Workspace loaded correctement
- [ ] Prometheus accessible (http://localhost:9090)
- [ ] Targets Prometheus tous "UP"
- [ ] Grafana accessible (http://localhost:3001)
- [ ] Datasource Prometheus connecté
- [ ] Métriques worker accessible (http://localhost:9091/metrics)

### Test Fonctionnel
- [ ] Materialiser un asset refactorisé
- [ ] Vérifier données dans MinIO
- [ ] Vérifier métriques Prometheus
- [ ] Vérifier logs structurés
- [ ] Tester partition Dagster
- [ ] Tester incremental loading

### Migration Assets
- [ ] Identifier assets à migrer (priorité)
- [ ] Migrer 1 asset pilote
- [ ] Valider en production
- [ ] Migrer assets restants progressivement
- [ ] Supprimer anciens assets (quand stable)

### Monitoring Setup
- [ ] Créer dashboards Grafana personnalisés
- [ ] Configurer alertes Prometheus
- [ ] Documenter runbooks
- [ ] Former l'équipe aux nouveaux dashboards

---

## 🎓 Formation Équipe

### Points clés à comprendre

1. **Architecture distribuée** : Orchestrator vs Workers
2. **dagster-dlt** : Comment ça simplifie le code
3. **Schema contracts** : Pourquoi et comment les utiliser
4. **Métriques Prometheus** : Quelles métriques regarder
5. **Grafana dashboards** : Où trouver l'info

### Exercices pratiques

1. **Migrer un asset simple** (ex: référentiel stations)
2. **Ajouter un validator custom** pour un nouveau champ
3. **Créer une alerte Prometheus** sur taux d'erreur
4. **Créer un dashboard Grafana** pour un nouveau source
5. **Debugger un pipeline** avec les métriques

---

## 🚦 Prochaines Étapes Recommandées

### Court terme (1-2 semaines)
1. Tester la stack complète en dev
2. Migrer 2-3 assets pilotes
3. Créer dashboards Grafana de base
4. Former l'équipe

### Moyen terme (1-2 mois)
1. Migrer tous les assets vers dagster-dlt
2. Configurer alerting Prometheus
3. Scale workers (passer à 2-3 replicas)
4. Optimiser based on métriques

### Long terme (3-6 mois)
1. CI/CD automatisé (GitHub Actions)
2. Tests automatisés (pytest + Great Expectations)
3. Secrets management (Vault)
4. Multi-environment (dev/staging/prod)

---

**Questions ? Problèmes ?**
Voir les logs détaillés avec `docker-compose logs -f <service_name>`
