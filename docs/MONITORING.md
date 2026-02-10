# Monitoring - Guide complet

## Vue d'ensemble de la stack

```
┌─────────────────────────────────────────────────────────────────┐
│                        TON NAVIGATEUR                           │
│                                                                 │
│   Grafana (:49507)          Netdata (:49506)                    │
│   Dashboards custom         Monitoring tout-en-un               │
│   Alertes configurables     Alertes par défaut                  │
└──────────┬──────────────────────────────────────────────────────┘
           │ requête PromQL
           ▼
┌──────────────────────┐
│   Prometheus (:49508)│   ← stocke les métriques (30 jours)
│   Base de métriques  │   ← scrape toutes les 15s
└──────┬─────────┬─────┘
       │         │
       ▼         ▼
┌──────────┐  ┌───────────────────┐
│ cAdvisor │  │ postgres_exporter │
│ (interne)│  │ (interne)         │
└──────┬───┘  └─────────┬─────────┘
       │                │
       ▼                ▼
┌──────────┐  ┌──────────────────┐
│  Docker  │  │   PostgreSQL     │
│  Engine  │  │   TimescaleDB    │
└──────────┘  └──────────────────┘
```

**En résumé** :
- **cAdvisor** lit les métriques Docker (CPU, RAM, réseau, disque par container)
- **postgres_exporter** lit les métriques PostgreSQL (connections, locks, cache, transactions)
- **Prometheus** scrape ces 2 exporters toutes les 15 secondes et stocke l'historique
- **Grafana** interroge Prometheus pour afficher des graphes et des alertes

---

## 1. Accès

| Service | URL | Login |
|---------|-----|-------|
| Grafana | http://localhost:49507 | admin / admin (changer au 1er login) |
| Prometheus | http://localhost:49508 | pas d'auth |
| Netdata | http://localhost:49506 | pas d'auth |

---

## 2. Prometheus - La base de métriques

### C'est quoi ?

Prometheus est une base de données de séries temporelles. Il va **chercher** (scrape) les métriques chez les exporters à intervalles réguliers. Contrairement à d'autres outils où les apps "poussent" les données, ici c'est Prometheus qui "tire" (pull).

### Interface Prometheus (http://localhost:49508)

L'interface est basique mais utile pour du debug :

#### Vérifier que les exporters sont UP

**Status → Targets** (http://localhost:49508/targets)

Tu verras 3 targets :
- `prometheus` (lui-même) → doit être **UP**
- `postgresql` (postgres_exporter) → doit être **UP**
- `cadvisor` → doit être **UP**

Si un target est **DOWN**, le service correspondant n'est pas joignable.

#### Tester une requête PromQL

Dans le champ "Expression" sur la page d'accueil :

```promql
# RAM utilisée par chaque container (en bytes)
container_memory_usage_bytes{name=~"brgm-.+"}

# CPU par container (taux sur 1 minute)
rate(container_cpu_usage_seconds_total{name=~"brgm-.+"}[1m])

# Nombre de connections PostgreSQL actives
pg_stat_activity_count{datname="postgres", state="active"}

# Cache hit ratio global de la base
pg_stat_database_blks_hit{datname="postgres"} / (pg_stat_database_blks_hit{datname="postgres"} + pg_stat_database_blks_read{datname="postgres"})
```

Clique **Execute** puis **Graph** pour voir l'évolution dans le temps.

### Configuration

Fichier : `docker/prometheus/prometheus.yml`

```yaml
global:
  scrape_interval: 15s      # Fréquence de collecte
  evaluation_interval: 15s  # Fréquence d'évaluation des règles

scrape_configs:
  - job_name: "prometheus"        # Se scrape lui-même
    static_configs:
      - targets: ["localhost:9090"]

  - job_name: "postgresql"        # Scrape postgres_exporter
    static_configs:
      - targets: ["postgres_exporter:9187"]

  - job_name: "cadvisor"          # Scrape cAdvisor
    static_configs:
      - targets: ["cadvisor:8080"]
```

**Rétention** : 30 jours (configurable via `--storage.tsdb.retention.time` dans docker-compose.yml)

---

## 3. Grafana - Les dashboards

### Premier login

1. Aller sur http://localhost:49507
2. Login : `admin` / `admin`
3. Grafana demande de changer le mot de passe → choisis-en un ou "Skip"

### Dashboards pré-installés

Deux dashboards sont déjà configurés et se chargent automatiquement :

#### Dashboard "Docker Containers - Hub'Eau"

**Accès** : Menu hamburger (☰) → Dashboards → Docker Containers - Hub'Eau

Ce qu'il montre :
- **CPU Usage per Container** : graphe temps réel du CPU de chaque container `brgm-*`
- **Memory Usage per Container** : RAM en bytes par container
- **Memory Usage vs Limit** : jauge en % (vert < 70%, jaune 70-90%, rouge > 90%)
  → **C'est LE panneau clé pour savoir si un job Dagster sature la RAM**
- **Network I/O** : trafic réseau entrant/sortant par container
- **Disk I/O** : lectures/écritures disque par container

#### Dashboard "PostgreSQL - Hub'Eau"

**Accès** : Menu hamburger (☰) → Dashboards → PostgreSQL - Hub'Eau

Ce qu'il montre :
- **Active Connections** : nombre de connexions actives (vert < 50, rouge > 90)
- **Total Connections** : toutes les connexions (actives + idle)
- **Cache Hit Ratio** : jauge (vert > 99%, jaune 90-99%, rouge < 90%)
- **Database Size** : taille totale de la base en bytes
- **Transactions per Second** : commits/s et rollbacks/s
- **Rows Operations/s** : fetched, inserted, updated, deleted par seconde
- **Locks** : nombre de locks par type (utile pour détecter des contentions)
- **Temp Bytes Written** : indicateur de queries lourdes qui débordent en disque
- **Deadlocks** : compteur cumulatif (doit rester à 0)
- **Buffers** : checkpoint vs backend (si "backend" est élevé, PostgreSQL manque de shared_buffers)

### Naviguer dans Grafana

#### Changer la période

En haut à droite, le sélecteur de temps :
- `Last 1 hour` : par défaut
- `Last 6 hours` : pour voir l'impact d'un job
- `Last 24 hours` / `Last 7 days` : pour les tendances
- **Custom range** : sélectionner une plage précise (ex: pendant un job Dagster)

#### Auto-refresh

Bouton à côté du sélecteur de temps :
- `Off` : pas de rafraîchissement
- `5s` / `10s` : mode "live" pendant un job
- `1m` : monitoring tranquille

#### Zoomer sur un pic

Cliquer-glisser sur un graphe pour zoomer sur une période précise. Utile pour isoler un pic de RAM pendant un job.

### Créer un nouveau panneau

1. Ouvrir un dashboard → **Edit** (icône crayon en haut)
2. **Add** → **Visualization**
3. Dans "Query", écrire une requête PromQL
4. Choisir le type de visualisation (Time series, Gauge, Stat, Table...)
5. **Apply**

### Importer un dashboard communautaire

Grafana a des milliers de dashboards pré-faits sur https://grafana.com/grafana/dashboards/

1. **Menu (☰) → Dashboards → New → Import**
2. Entrer l'ID du dashboard :
   - `893` : Docker + cAdvisor (très complet)
   - `9628` : PostgreSQL (très détaillé)
   - `14282` : cAdvisor full dashboard
3. **Load** → Sélectionner "Prometheus" comme datasource → **Import**

---

## 4. Cas d'usage concrets

### "Mon job Dagster est lent, c'est la RAM ?"

1. Ouvrir Grafana → **Docker Containers - Hub'Eau**
2. Mettre la période sur les dernières heures
3. Regarder **Memory Usage vs Limit** pour `brgm-dlt-worker`
   - **Rouge (> 90%)** → le worker manque de RAM, le job swap/ralentit
   - **Vert** → la RAM n'est pas le problème, chercher ailleurs (CPU, I/O, API lente)
4. Regarder le graphe **CPU Usage** pour `brgm-dlt-worker`
   - CPU à 100% constant → le job est CPU-bound
5. Regarder **Disk I/O** pour `brgm-postgres`
   - Beaucoup d'écritures → INSERT/UPDATE massifs en cours

### "Une requête SQL est lente"

1. Ouvrir Grafana → **PostgreSQL - Hub'Eau**
2. Vérifier **Cache Hit Ratio**
   - < 90% → PostgreSQL lit beaucoup depuis le disque (données froides ou RAM insuffisante)
3. Vérifier **Temp Bytes Written**
   - En hausse → une requête a débordé la `work_mem` et trie sur disque
4. Vérifier **Locks**
   - Beaucoup de `ExclusiveLock` → une table est verrouillée (probablement dbt rebuild)
5. Pour identifier la requête exacte, utiliser `pg_stat_statements` dans CloudBeaver :
   ```sql
   SELECT query, calls, total_exec_time, mean_exec_time, rows
   FROM pg_stat_statements
   ORDER BY total_exec_time DESC
   LIMIT 20;
   ```

### "Le bootstrap job tourne depuis des heures"

1. Grafana → **Docker Containers** → période "Last 6 hours"
2. Timeline du job :
   - **Phase DLT** (bronze) : gros réseau I/O (download API), CPU modéré
   - **Phase dbt** (silver/gold) : gros CPU + disk I/O (transformations SQL)
   - **Phase ERA5** : gros réseau I/O (download Copernicus)
3. Grafana → **PostgreSQL** :
   - **Rows Operations/s** : les `inserted/s` montrent le débit d'ingestion
   - **Transactions/s** : permet de voir si PostgreSQL suit le rythme

### "La base PostgreSQL grossit trop vite"

1. Grafana → **PostgreSQL** → **Database Size**
2. Mettre sur "Last 7 days" pour voir la tendance
3. Si croissance anormale, vérifier dans CloudBeaver :
   ```sql
   SELECT schemaname, tablename,
          pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as size
   FROM pg_tables
   WHERE schemaname IN ('bronze', 'silver', 'gold')
   ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
   LIMIT 20;
   ```

---

## 5. PromQL - Aide-mémoire

PromQL est le langage de requête de Prometheus. Les requêtes les plus utiles :

### Containers Docker

```promql
# RAM par container
container_memory_usage_bytes{name=~"brgm-.+"}

# RAM en % du limit
container_memory_usage_bytes{name=~"brgm-.+"} / container_spec_memory_limit_bytes{name=~"brgm-.+"}

# CPU par container (taux/seconde)
rate(container_cpu_usage_seconds_total{name=~"brgm-.+"}[1m])

# Réseau reçu par container
rate(container_network_receive_bytes_total{name=~"brgm-.+"}[5m])

# Disque écrit par container
rate(container_fs_writes_bytes_total{name=~"brgm-.+"}[5m])

# RAM du worker spécifiquement
container_memory_usage_bytes{name="brgm-dlt-worker"}
```

### PostgreSQL

```promql
# Connections actives
pg_stat_activity_count{datname="postgres", state="active"}

# Cache hit ratio
pg_stat_database_blks_hit{datname="postgres"} / (pg_stat_database_blks_hit{datname="postgres"} + pg_stat_database_blks_read{datname="postgres"})

# Transactions/s
rate(pg_stat_database_xact_commit{datname="postgres"}[1m])

# Rows insérées/s
rate(pg_stat_database_tup_inserted{datname="postgres"}[1m])

# Taille de la base
pg_database_size_bytes{datname="postgres"}

# Locks par type
pg_locks_count{datname="postgres"}

# Temp bytes écrits/s (queries lourdes)
rate(pg_stat_database_temp_bytes{datname="postgres"}[5m])

# Deadlocks
pg_stat_database_deadlocks{datname="postgres"}
```

### Syntaxe PromQL rapide

| Syntaxe | Description | Exemple |
|---------|-------------|---------|
| `metric_name` | Valeur instantanée | `pg_stat_activity_count` |
| `{label="value"}` | Filtre par label | `{datname="postgres"}` |
| `{name=~"regex"}` | Filtre par regex | `{name=~"brgm-.+"}` |
| `rate(metric[5m])` | Taux de variation par seconde sur 5 min | `rate(pg_stat_database_xact_commit[5m])` |
| `sum(metric)` | Somme de toutes les séries | `sum(pg_stat_activity_count)` |
| `avg(metric)` | Moyenne | `avg(container_cpu_usage_seconds_total)` |
| `max(metric)` | Maximum | `max(container_memory_usage_bytes)` |
| `topk(5, metric)` | Top 5 | `topk(5, container_memory_usage_bytes)` |

---

## 6. Alertes Grafana (optionnel)

Grafana peut envoyer des alertes par email, Slack, Discord, etc.

### Configurer un canal de notification

1. **Menu (☰) → Alerting → Contact points**
2. **Add contact point**
3. Choisir le type (Email, Slack, Discord, Webhook...)
4. Configurer et **Test** → **Save**

### Créer une alerte

Exemple : alerter quand le worker dépasse 90% de RAM.

1. Ouvrir le dashboard **Docker Containers**
2. Editer le panneau **Memory Usage vs Limit**
3. Onglet **Alert** → **Create alert rule from this panel**
4. Condition : `WHEN last() OF query IS ABOVE 0.9`
5. Evaluate every `1m` for `5m` (alerte si > 90% pendant 5 minutes)
6. Notification : choisir le contact point créé
7. **Save**

### Alertes utiles à créer

| Alerte | Condition | Seuil |
|--------|-----------|-------|
| Worker RAM saturée | `container_memory_usage / limit` pour brgm-dlt-worker | > 90% pendant 5 min |
| PostgreSQL RAM saturée | `container_memory_usage / limit` pour brgm-postgres | > 85% pendant 5 min |
| Cache hit ratio bas | `blks_hit / (hit + read)` | < 95% pendant 10 min |
| Trop de connections | `pg_stat_activity_count` total | > 80 pendant 2 min |
| Deadlock détecté | `increase(pg_stat_database_deadlocks[5m])` | > 0 |

---

## 7. Comparaison Netdata vs Grafana

| Aspect | Netdata | Grafana + Prometheus |
|--------|---------|---------------------|
| Setup | Zero config | Config nécessaire |
| Dashboards | Pré-faits, très nombreux | Custom, flexibles |
| Historique | Limité (RAM) | 30 jours sur disque |
| Alertes | Par défaut (parfois trop) | À configurer manuellement |
| Requêtes custom | Non | Oui (PromQL) |
| Corrélation | Limitée | Excellente (overlay de métriques) |
| Communauté dashboards | Non | 3000+ dashboards importables |
| Idéal pour | Monitoring temps réel "quick look" | Analyse post-mortem et alertes custom |

**Recommandation** : utiliser Netdata pour le monitoring live rapide, Grafana pour l'analyse détaillée et les alertes sur mesure.

---

## 8. Fichiers de configuration

```
docker/
├── prometheus/
│   └── prometheus.yml              # Scrape config (targets, intervalles)
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── datasource.yml      # Auto-configure Prometheus comme source
│   │   └── dashboards/
│   │       └── dashboards.yml      # Charge les dashboards au démarrage
│   └── dashboards/
│       ├── docker-containers.json  # Dashboard Docker pré-configuré
│       └── postgresql.json         # Dashboard PostgreSQL pré-configuré
└── netdata/
    ├── go.d/
    │   └── postgres.conf           # Connecteur PostgreSQL pour Netdata
    └── health.d/
        └── postgres.conf           # Override alertes (silence chunks TimescaleDB)
```

---

## 9. Troubleshooting

### Prometheus target DOWN

```bash
# Vérifier que le service tourne
docker compose ps

# Vérifier les logs
docker compose logs postgres_exporter
docker compose logs cadvisor
```

### Grafana "No data"

1. Vérifier que Prometheus est UP : http://localhost:49508/targets
2. Tester la requête directement dans Prometheus d'abord
3. Dans Grafana, vérifier que la datasource est "Prometheus" (pas un autre)
4. Vérifier la période sélectionnée (les données n'existent qu'après le démarrage de Prometheus)

### Métriques PostgreSQL manquantes

```bash
# Vérifier que pg_stat_statements est chargé
docker exec brgm-postgres psql -U postgres -c "SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements';"

# Si vide, redémarrer PostgreSQL (nécessaire après le 1er CREATE EXTENSION)
docker compose restart postgres
```

### Netdata alertes spam

Les alertes per-chunk TimescaleDB sont déjà désactivées via `docker/netdata/health.d/postgres.conf`.
Pour désactiver d'autres alertes, ajouter des overrides dans ce même fichier.
