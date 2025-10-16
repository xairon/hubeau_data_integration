# Observability Stack - Guide Complet

## 📊 Vue d'ensemble

L'observability permet de **comprendre ce qui se passe** dans ton pipeline en temps réel grâce à 3 piliers :

1. **Métriques** (Prometheus) : Compteurs, durées, gauges
2. **Dashboards** (Grafana) : Visualisation graphique
3. **Logs** (Structlog) : Events détaillés

---

## 🔧 Prometheus : C'est quoi ? Comment ça marche ?

### Concept de base

**Prometheus = Base de données optimisée pour séries temporelles**

Une série temporelle = une métrique qui évolue dans le temps :
```
Timestamp            | Métrique                    | Valeur
2025-01-15 10:00:00 | records_loaded{source=piezo} | 1000
2025-01-15 10:00:15 | records_loaded{source=piezo} | 1050
2025-01-15 10:00:30 | records_loaded{source=piezo} | 1100
```

### Architecture

```
┌──────────────────┐
│  DLT Worker      │  ← Ton application Python
│  (port 9091)     │
└────────┬─────────┘
         │ Expose /metrics (format texte)
         │
         ▼
  http://worker:9091/metrics
         │
         │ Retourne:
         │ hubeau_dlt_records_total{source="piezo"} 1000
         │ hubeau_api_response_time_seconds_sum 45.2
         │
         ▼
┌──────────────────┐
│  Prometheus      │  ← "Scrape" (lit) /metrics toutes les 15s
│  (port 9090)     │     Stocke dans time-series DB
└────────┬─────────┘
         │
         │ API PromQL
         │
         ▼
┌──────────────────┐
│  Grafana         │  ← Lit Prometheus et affiche graphiques
│  (port 3001)     │
└──────────────────┘
```

### Comment instrumenter ton code ?

#### 1. Importer les métriques
```python
from hubeau_pipeline.observability.metrics import (
    dlt_records_extracted_total,
    dlt_extraction_duration_seconds,
    hubeau_api_requests_total
)
```

#### 2. Incrémenter dans ton code
```python
def extract_piezometry_data():
    start_time = time.time()

    # Extract data
    records = fetch_from_api()  # 1000 records

    # Incrémenter counter
    dlt_records_extracted_total.labels(
        source="piezometry",
        resource="chroniques",
        partition="2024"
    ).inc(1000)

    # Observer duration
    duration = time.time() - start_time
    dlt_extraction_duration_seconds.labels(
        source="piezometry",
        resource="chroniques",
        partition="2024"
    ).observe(duration)

    return records
```

#### 3. Prometheus scrape automatiquement
Configuration dans `docker/monitoring/prometheus.yml` :
```yaml
scrape_configs:
  - job_name: 'dlt-workers'
    static_configs:
      - targets: ['dlt_worker:9091']
    scrape_interval: 15s
```

Prometheus va faire un `curl http://dlt_worker:9091/metrics` toutes les 15s.

#### 4. Consulter dans Prometheus UI
http://localhost:9090

Query examples :
```promql
# Valeur actuelle
hubeau_dlt_records_extracted_total{source="piezometry"}

# Taux par seconde (sur 5min)
rate(hubeau_dlt_records_extracted_total{source="piezometry"}[5m])

# Total sur 24h
increase(hubeau_dlt_records_extracted_total{source="piezometry"}[24h])
```

---

## 📈 Types de Métriques Prometheus

### 1. Counter (compteur, toujours croissant)

**Utilisation :** Compter des événements (requests, records, errors)

```python
from prometheus_client import Counter

# Définir
records_loaded = Counter(
    'hubeau_dlt_records_loaded_total',
    'Total records loaded',
    ['source', 'resource']  # Labels
)

# Utiliser
records_loaded.labels(source="piezometry", resource="chroniques").inc(1000)
```

**Queries utiles :**
```promql
# Taux par seconde
rate(hubeau_dlt_records_loaded_total[5m])

# Total augmentation sur 1h
increase(hubeau_dlt_records_loaded_total[1h])

# Par source
sum by (source) (hubeau_dlt_records_loaded_total)
```

### 2. Gauge (jauge, monte et descend)

**Utilisation :** Valeurs instantanées (CPU, RAM, workers actifs)

```python
from prometheus_client import Gauge

# Définir
active_workers = Gauge(
    'hubeau_active_workers',
    'Number of active workers'
)

# Utiliser
active_workers.set(5)     # Set à 5
active_workers.inc()      # 5 → 6
active_workers.dec(2)     # 6 → 4
```

**Queries utiles :**
```promql
# Valeur actuelle
hubeau_active_workers

# Max sur 1h
max_over_time(hubeau_active_workers[1h])

# Moyenne sur 1h
avg_over_time(hubeau_active_workers[1h])
```

### 3. Histogram (distribution de valeurs)

**Utilisation :** Latences, durées (permet de calculer percentiles)

```python
from prometheus_client import Histogram

# Définir
extraction_duration = Histogram(
    'hubeau_dlt_extraction_duration_seconds',
    'Extraction duration',
    ['source'],
    buckets=[1, 5, 10, 30, 60, 120, 300]  # Buckets en secondes
)

# Utiliser
extraction_duration.labels(source="piezometry").observe(45.2)  # 45.2 secondes
```

**Ce que Prometheus stocke :**
```
hubeau_dlt_extraction_duration_seconds_bucket{source="piezo",le="1"} 0
hubeau_dlt_extraction_duration_seconds_bucket{source="piezo",le="5"} 0
hubeau_dlt_extraction_duration_seconds_bucket{source="piezo",le="10"} 0
hubeau_dlt_extraction_duration_seconds_bucket{source="piezo",le="30"} 0
hubeau_dlt_extraction_duration_seconds_bucket{source="piezo",le="60"} 1  ← 45.2s < 60s
hubeau_dlt_extraction_duration_seconds_sum{source="piezo"} 45.2
hubeau_dlt_extraction_duration_seconds_count{source="piezo"} 1
```

**Queries utiles :**
```promql
# P95 (95e percentile)
histogram_quantile(0.95, rate(hubeau_dlt_extraction_duration_seconds_bucket[5m]))

# P50 (médiane)
histogram_quantile(0.50, rate(hubeau_dlt_extraction_duration_seconds_bucket[5m]))

# Moyenne
rate(hubeau_dlt_extraction_duration_seconds_sum[5m])
/
rate(hubeau_dlt_extraction_duration_seconds_count[5m])
```

---

## 🎯 Métriques Hub'Eau Disponibles

### DLT Extraction

```python
# Records extraits
hubeau_dlt_records_extracted_total{source, resource, partition}

# Records chargés
hubeau_dlt_records_loaded_total{source, resource, destination, partition}

# Durée extraction
hubeau_dlt_extraction_duration_seconds{source, resource, partition}

# Durée chargement
hubeau_dlt_load_duration_seconds{source, resource, destination, partition}

# Erreurs DLT
hubeau_dlt_errors_total{source, resource, error_type, partition}
```

### API Hub'Eau

```python
# Requests totales
hubeau_api_requests_total{endpoint, status_code}

# Rate limits
hubeau_api_rate_limit_hits_total{endpoint}

# Temps de réponse
hubeau_api_response_time_seconds{endpoint}
```

### Data Quality

```python
# Échecs validation
hubeau_data_quality_check_failures_total{source, resource, check_type, partition}

# Records invalides
hubeau_data_quality_invalid_records_total{source, resource, validation_rule, partition}
```

### Pipeline

```python
# Runs totaux
hubeau_pipeline_runs_total{pipeline_name, status}  # status: success | failure | retry

# Durée pipeline
hubeau_pipeline_duration_seconds{pipeline_name, partition}
```

### Storage

```python
# Objets écrits MinIO
hubeau_minio_objects_written_total{bucket, prefix}

# Bytes écrits
hubeau_minio_bytes_written_total{bucket, prefix}
```

### Dagster

```python
# Asset materializations
hubeau_dagster_asset_materializations_total{asset_key, partition}

# Durée materialization
hubeau_dagster_asset_materialization_duration_seconds{asset_key, partition}
```

### Incremental Loading

```python
# Dernier timestamp chargé
hubeau_incremental_last_timestamp{source, resource}

# Gap entre dernières données et maintenant (en jours)
hubeau_incremental_gap_days{source, resource}
```

---

## 📊 Grafana : Créer des Dashboards

### 1. Se connecter

http://localhost:3001
- User: `admin`
- Password: valeur de `GRAFANA_PASSWORD` dans `.env`

### 2. Vérifier datasource Prometheus

Configuration > Datasources > Prometheus
- URL: `http://prometheus:9090`
- Status: ✅ Connected

### 3. Créer un dashboard

#### Panel : Throughput d'extraction

1. Create > Dashboard > Add Panel
2. Query :
```promql
rate(hubeau_dlt_records_extracted_total{source="piezometry"}[5m])
```
3. Visualization : Time series (ligne)
4. Panel title : "Records extraits par seconde - Piézométrie"
5. Unit : ops/s
6. Save

#### Panel : Latence API P95

1. Add Panel
2. Query :
```promql
histogram_quantile(0.95,
  rate(hubeau_api_response_time_seconds_bucket[5m])
)
```
3. Visualization : Gauge
4. Panel title : "P95 Latence API Hub'Eau"
5. Unit : seconds (s)
6. Thresholds :
   - Green : < 2s
   - Yellow : 2-5s
   - Red : > 5s
7. Save

#### Panel : Taux d'erreur

1. Add Panel
2. Query :
```promql
sum(rate(hubeau_dlt_errors_total[5m]))
/
sum(rate(hubeau_api_requests_total[5m]))
* 100
```
3. Visualization : Stat
4. Panel title : "Taux d'erreur (%)"
5. Unit : percent (0-100)
6. Alert : Si > 5% → Email
7. Save

#### Panel : Records chargés par source

1. Add Panel
2. Query :
```promql
sum by (source) (hubeau_dlt_records_loaded_total)
```
3. Visualization : Bar chart
4. Panel title : "Records totaux par source"
5. Legend : {{source}}
6. Save

#### Panel : Data quality (records invalides)

1. Add Panel
2. Query :
```promql
sum by (validation_rule) (hubeau_data_quality_invalid_records_total)
```
3. Visualization : Pie chart
4. Panel title : "Records invalides (par règle)"
5. Save

### 4. Variables (pour filtrage dynamique)

Dashboard settings > Variables > Add variable

**Variable : source**
- Name: `source`
- Type: Query
- Query :
```promql
label_values(hubeau_dlt_records_extracted_total, source)
```
- Multi-value: ✅
- Include All: ✅

Utilisation dans panels :
```promql
rate(hubeau_dlt_records_extracted_total{source=~"$source"}[5m])
```

**Variable : partition**
- Name: `partition`
- Type: Query
- Query :
```promql
label_values(hubeau_dlt_records_extracted_total, partition)
```

### 5. Alertes

Panel > Alert tab > Create alert rule

**Exemple : High Error Rate**
```
Condition:
WHEN avg() OF query(A, 5m, now)
IS ABOVE 10

Query A:
sum(rate(hubeau_dlt_errors_total[5m]))

Send to: Email channel

Message:
Pipeline Hub'Eau - Taux d'erreur élevé
{{ $values.A }} erreurs/seconde détectées
```

---

## 🚨 Alerting avec Prometheus Alertmanager

### 1. Configuration Alertmanager

`docker/monitoring/alertmanager.yml` :
```yaml
global:
  smtp_smarthost: 'smtp.gmail.com:587'
  smtp_from: 'alerts@brgm.fr'
  smtp_auth_username: 'alerts@brgm.fr'
  smtp_auth_password: 'your_password'

route:
  receiver: 'email-team'
  group_by: ['alertname', 'source']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h

receivers:
  - name: 'email-team'
    email_configs:
      - to: 'team@brgm.fr'
        headers:
          Subject: '[ALERT] {{ .GroupLabels.alertname }}'

  - name: 'slack-channel'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'
        channel: '#alerts-hubeau'
        text: '{{ .CommonAnnotations.summary }}'
```

### 2. Définir alertes

`docker/monitoring/alerts.yml` :
```yaml
groups:
  - name: hubeau_pipeline
    interval: 30s
    rules:
      # Taux d'erreur élevé
      - alert: HighErrorRate
        expr: |
          sum(rate(hubeau_dlt_errors_total[5m]))
          /
          sum(rate(hubeau_api_requests_total[5m]))
          > 0.05
        for: 5m
        labels:
          severity: critical
          team: data-engineering
        annotations:
          summary: "Taux d'erreur élevé dans pipeline Hub'Eau"
          description: |
            Taux d'erreur: {{ $value | humanizePercentage }}
            Source: {{ $labels.source }}

      # Extraction lente
      - alert: SlowExtraction
        expr: |
          histogram_quantile(0.95,
            rate(hubeau_dlt_extraction_duration_seconds_bucket[5m])
          ) > 300
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Extraction lente (P95 > 5min)"
          description: "P95 latence: {{ $value }}s pour {{ $labels.source }}"

      # Rate limit atteint
      - alert: RateLimitHit
        expr: |
          rate(hubeau_api_rate_limit_hits_total[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Rate limit Hub'Eau atteint"
          description: "Endpoint: {{ $labels.endpoint }}"

      # Gap incremental trop grand
      - alert: IncrementalGapTooLarge
        expr: hubeau_incremental_gap_days > 7
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: "Gap incremental > 7 jours"
          description: |
            Source: {{ $labels.source }} / {{ $labels.resource }}
            Gap: {{ $value }} jours

      # Aucune donnée chargée (pipeline bloqué ?)
      - alert: NoDataLoaded
        expr: |
          rate(hubeau_dlt_records_loaded_total[1h]) == 0
        for: 2h
        labels:
          severity: critical
        annotations:
          summary: "Aucune donnée chargée depuis 2h"
          description: "Pipeline potentiellement bloqué"
```

### 3. Ajouter Alertmanager au docker-compose

```yaml
alertmanager:
  image: prom/alertmanager:latest
  ports:
    - "9093:9093"
  volumes:
    - ./docker/monitoring/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
    - alertmanager_data:/alertmanager
  command:
    - '--config.file=/etc/alertmanager/alertmanager.yml'
    - '--storage.path=/alertmanager'
  networks:
    - hubeau_network
```

Et mettre à jour `prometheus.yml` :
```yaml
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']

rule_files:
  - /etc/prometheus/alerts.yml
```

---

## 📖 PromQL : Requêtes Essentielles

### Basics

```promql
# Valeur actuelle
hubeau_dlt_records_extracted_total

# Filtrer par label
hubeau_dlt_records_extracted_total{source="piezometry"}

# Regex
hubeau_dlt_records_extracted_total{source=~"piezo.*"}

# Multiple labels
hubeau_dlt_records_extracted_total{source="piezometry",partition="2024"}
```

### Rates et Increases

```promql
# Taux par seconde (sur 5min)
rate(hubeau_dlt_records_extracted_total[5m])

# Augmentation totale (sur 1h)
increase(hubeau_dlt_records_extracted_total[1h])

# Dérivée (changement par seconde)
deriv(hubeau_dlt_records_extracted_total[5m])
```

### Aggregations

```promql
# Somme
sum(hubeau_dlt_records_extracted_total)

# Somme par label
sum by (source) (hubeau_dlt_records_extracted_total)

# Moyenne
avg(hubeau_dlt_extraction_duration_seconds_sum)

# Max
max(hubeau_api_response_time_seconds)

# Count (nombre de séries)
count(hubeau_dlt_records_extracted_total)
```

### Time Functions

```promql
# Max sur 1h
max_over_time(hubeau_active_workers[1h])

# Moyenne sur 24h
avg_over_time(hubeau_api_response_time_seconds[24h])

# Valeur il y a 1h
hubeau_dlt_records_extracted_total offset 1h

# Différence avec il y a 1h
hubeau_dlt_records_extracted_total
-
hubeau_dlt_records_extracted_total offset 1h
```

### Percentiles (Histograms)

```promql
# P50 (médiane)
histogram_quantile(0.50, rate(hubeau_dlt_extraction_duration_seconds_bucket[5m]))

# P95
histogram_quantile(0.95, rate(hubeau_dlt_extraction_duration_seconds_bucket[5m]))

# P99
histogram_quantile(0.99, rate(hubeau_dlt_extraction_duration_seconds_bucket[5m]))
```

### Math

```promql
# Ratio (taux d'erreur)
sum(rate(hubeau_dlt_errors_total[5m]))
/
sum(rate(hubeau_api_requests_total[5m]))

# Pourcentage
(sum(rate(hubeau_dlt_errors_total[5m]))
/
sum(rate(hubeau_api_requests_total[5m])))
* 100

# Moyenne pondérée
sum(hubeau_dlt_extraction_duration_seconds_sum)
/
sum(hubeau_dlt_extraction_duration_seconds_count)
```

---

## 🎓 Best Practices

### 1. Nommage des métriques

```
<namespace>_<component>_<name>_<unit>

hubeau_dlt_records_extracted_total       ✅
hubeau_api_response_time_seconds         ✅
hubeau_pipeline_runs_total               ✅

records_extracted                        ❌ (pas de namespace)
hubeau_duration                          ❌ (pas d'unité)
```

### 2. Labels

**GOOD (cardinalité faible) :**
```python
records_loaded.labels(
    source="piezometry",     # 10 sources max
    resource="chroniques",   # 50 resources max
    partition="2024"         # 50 partitions max
)
# Cardinalité totale: 10 × 50 × 50 = 25,000 séries → OK
```

**BAD (cardinalité élevée) :**
```python
records_loaded.labels(
    user_id="123456",        # Millions d'users → ❌
    timestamp="2024-01-15",  # Infini de timestamps → ❌
    record_id="abc123"       # Millions de records → ❌
)
# Cardinalité totale: millions → CRASH Prometheus
```

### 3. Retention

```yaml
# prometheus.yml
storage:
  tsdb:
    retention.time: 15d      # Garder 15 jours (default)
    retention.size: 50GB     # Ou max 50GB
```

### 4. Recording Rules (pré-calcul)

Pour queries lourdes utilisées fréquemment :

```yaml
# recording_rules.yml
groups:
  - name: hubeau_aggregations
    interval: 1m
    rules:
      # Pré-calculer le taux d'erreur
      - record: hubeau:error_rate:1m
        expr: |
          sum(rate(hubeau_dlt_errors_total[1m]))
          /
          sum(rate(hubeau_api_requests_total[1m]))

      # Pré-calculer P95 latence
      - record: hubeau:api_latency_p95:5m
        expr: |
          histogram_quantile(0.95,
            rate(hubeau_api_response_time_seconds_bucket[5m])
          )
```

Usage :
```promql
# Au lieu de :
sum(rate(hubeau_dlt_errors_total[1m])) / sum(rate(hubeau_api_requests_total[1m]))

# Utilise :
hubeau:error_rate:1m
```

---

## 🐛 Troubleshooting

### Prometheus ne scrape pas

```bash
# Vérifier targets
http://localhost:9090/targets

# Test manuel
curl http://localhost:9091/metrics

# Logs Prometheus
docker logs prometheus
```

### Grafana ne voit pas les métriques

```bash
# Test connexion Prometheus depuis Grafana
docker exec grafana curl http://prometheus:9090/api/v1/query?query=up

# Vérifier datasource
Configuration > Datasources > Prometheus > Test
```

### Métriques ne s'incrémentent pas

```python
# Vérifier que le code est exécuté
import logging
logger.info("Incrémentation métrique")
records_loaded.labels(...).inc(1000)

# Vérifier labels exacts
# BAD: labels différents = séries différentes
records_loaded.labels(source="piezo").inc(100)
records_loaded.labels(source="piezometry").inc(200)  # Série différente !

# GOOD: labels identiques
records_loaded.labels(source="piezometry").inc(100)
records_loaded.labels(source="piezometry").inc(200)  # Même série
```

---

## 📚 Ressources

- **Prometheus Doc** : https://prometheus.io/docs/
- **PromQL Basics** : https://prometheus.io/docs/prometheus/latest/querying/basics/
- **Grafana Tutorials** : https://grafana.com/tutorials/
- **Python Client** : https://github.com/prometheus/client_python
- **Best Practices** : https://prometheus.io/docs/practices/naming/

---

Enjoy monitoring ! 🚀
