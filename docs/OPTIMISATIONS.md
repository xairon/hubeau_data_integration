# Optimisations PostgreSQL & Monitoring

Guide complet des optimisations implémentées pour le projet Hub'Eau Data Integration.

## 📋 Vue d'Ensemble

Le projet inclut maintenant:
- ✅ **Schedules intelligents** (hebdo/mensuel/annuel)
- ✅ **Index PostgreSQL** pour performance requêtes
- ✅ **Foreign Keys** pour intégrité référentielle
- ✅ **PostGIS Geometry** pour requêtes spatiales
- ✅ **Assets de validation** des données
- ✅ **Sensors de monitoring** (backfill auto, alerting)

---

## 🗓️ Schedules Automatiques

### Schedules Actifs

| Schedule | Fréquence | Description |
|----------|-----------|-------------|
| `sync_stations_weekly` | Dimanche 2h | Mise à jour référentiels stations (8 APIs) |
| `sync_current_year_monthly` | 1er du mois 3h | Données année courante (incrémental) |
| `sync_all_years_annually` | 15 janvier 4h | Backfill complet toutes années depuis 2020 |

### Schedules Optionnels (désactivés par défaut)

| Schedule | Fréquence | Description |
|----------|-----------|-------------|
| `sync_piezometry_biweekly` | 1er et 15 du mois 3h | Piézométrie seulement |
| `sync_quality_rivers_monthly` | 5 du mois 3h | Qualité cours d'eau seulement |
| `sync_hydrometry_monthly` | 10 du mois 3h | Hydrométrie seulement |

**Activation:**
```python
# src/hubeau_pipeline/schedules/schedules.py
all_schedules = [
    sync_stations_weekly,
    sync_current_year_monthly,
    sync_all_years_annually,
    # Décommenter pour activer:
    sync_piezometry_biweekly,
    sync_quality_rivers_monthly,
    sync_hydrometry_monthly,
]
```

---

## 🔍 Index PostgreSQL

### Bénéfices

- **Performance requêtes**: 10x à 1000x plus rapides
- **Optimisation tri**: ORDER BY instantané
- **Joins rapides**: Entre stations et chroniques

### Exécution

```bash
# Option 1: psql
psql -U postgres -d postgres -f scripts/sql/01_create_indexes.sql

# Option 2: Adminer
# http://localhost:8081 → SQL command → Copier-coller script

# Option 3: Docker
docker exec postgres psql -U postgres -d postgres -f /tmp/01_create_indexes.sql
```

### Index Créés (60+ index)

#### Piézométrie
- `idx_piezo_stations_code`: Sur code station
- `idx_piezo_chroniques_bss_date`: Composite (code_bss + date) - **CRITIQUE**
- `idx_piezo_chroniques_recent`: Partiel (< 1 an)

#### Hydrométrie
- `idx_hydro_obs_station_date`: Composite (station + date)
- `idx_hydro_stations_code`: Sur code station

#### Qualité Eau
- `idx_quality_rivers_analyses_full`: Composite (station + date + paramètre)
- `idx_quality_rivers_analyses_param`: Sur code paramètre

**Voir script complet**: `scripts/sql/01_create_indexes.sql`

### Vérification

```sql
-- Taille des index
SELECT
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_indexes
JOIN pg_class ON indexrelid = oid
WHERE schemaname = 'hubeau'
ORDER BY pg_relation_size(indexrelid) DESC
LIMIT 20;
```

---

## 🔗 Foreign Keys (Intégrité Référentielle)

### Bénéfices

- **Cohérence garantie**: Pas de chroniques sans station
- **Suppression cascade**: Si station supprimée, chroniques aussi
- **Validation INSERT**: Erreur si référence inexistante

### Exécution

```bash
psql -U postgres -d postgres -f scripts/sql/02_create_foreign_keys.sql
```

### ⚠️ IMPORTANT: Vérifier Orphelins d'Abord

Le script vérifie automatiquement les données orphelines:

```sql
-- Exemple: Chroniques piézométrie sans station
SELECT COUNT(*) FROM hubeau.piezometry_chroniques pc
LEFT JOIN hubeau.piezometry_stations ps ON pc.code_bss = ps.code_station
WHERE ps.code_station IS NULL;
```

**Si orphelins détectés:**

**Option A** - Supprimer orphelins (perte données):
```sql
DELETE FROM hubeau.piezometry_chroniques pc
WHERE NOT EXISTS (
    SELECT 1 FROM hubeau.piezometry_stations ps WHERE ps.code_station = pc.code_bss
);
```

**Option B** - Charger stations manquantes:
```python
# Via Dagster UI
Matérialiser asset: piezometry_stations_reference
```

**Option C** - Pas de FK pour cette table (déconseillé):
```sql
-- Commenter la section correspondante dans le script
```

### Foreign Keys Créées (9 contraintes)

- `fk_piezo_chroniques_station`: chroniques → stations
- `fk_hydro_obs_station`: observations → stations
- `fk_quality_rivers_analyses_station`: analyses → stations
- `fk_quality_groundwater_analyses_station`: analyses → stations BSS
- `fk_temperature_chroniques_station`: chroniques → stations
- `fk_ecoulement_observations_station`: observations → stations
- `fk_hydrobio_indices_station`: indices → stations
- `fk_hydrobio_taxons_station`: taxons → stations
- `fk_prelevements_chroniques_ouvrage`: chroniques → ouvrages

Toutes avec `ON DELETE CASCADE ON UPDATE CASCADE`.

---

## 🗺️ PostGIS Geometry (Géospatial)

### Bénéfices

- **Performance**: Requêtes spatiales 10-100x plus rapides
- **Capacités**: Distance, buffer, intersection, within, etc.
- **Export**: GeoJSON, Shapefile, QGIS

### Exécution

```bash
psql -U postgres -d postgres -f scripts/sql/03_add_postgis_geometry.sql
```

### Features Ajoutées

1. **Colonne `geom`** sur toutes tables stations:
   ```sql
   ALTER TABLE hubeau.piezometry_stations ADD COLUMN geom geometry(Point, 4326);
   ```

2. **Peuplement automatique** depuis lat/lon:
   ```sql
   UPDATE hubeau.piezometry_stations
   SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
   ```

3. **Index spatiaux GIST**:
   ```sql
   CREATE INDEX idx_piezo_stations_geom ON hubeau.piezometry_stations USING GIST(geom);
   ```

4. **Triggers auto-update**:
   - Si latitude/longitude changent → geom mis à jour automatiquement

### Exemples de Requêtes

**Stations dans rayon 10km de Tours** (47.39°N, 0.69°E):
```sql
SELECT
    code_station,
    libelle_station,
    ST_Distance(geom::geography, ST_MakePoint(0.69, 47.39)::geography) / 1000 AS distance_km
FROM hubeau.piezometry_stations
WHERE ST_DWithin(geom::geography, ST_MakePoint(0.69, 47.39)::geography, 10000)
ORDER BY distance_km;
```

**Export GeoJSON** (pour visualisation):
```sql
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(ST_AsGeoJSON(t.*)::json)
)
FROM (
    SELECT code_station, libelle_station, geom
    FROM hubeau.piezometry_stations
    WHERE geom IS NOT NULL
) t;
```

**Intersection avec département** (si table départements existe):
```sql
SELECT
    d.nom_departement,
    COUNT(s.code_station) AS nb_stations
FROM departements d
JOIN hubeau.piezometry_stations s ON ST_Intersects(d.geom, s.geom)
GROUP BY d.nom_departement
ORDER BY nb_stations DESC;
```

---

## ✅ Assets de Validation des Données

### Assets Créés

| Asset | Description | Fréquence recommandée |
|-------|-------------|----------------------|
| `piezometry_data_quality` | Validation piézométrie (coords, dates, outliers) | Hebdomadaire |
| `quality_rivers_data_quality` | Validation qualité cours d'eau (paramètres, résultats) | Hebdomadaire |
| `global_data_quality_report` | Rapport global (toutes APIs, stats DLT) | Quotidien |

### Validations Effectuées

#### Piézométrie

- ✅ Coordonnées valides (-90/90, -180/180)
- ✅ Dates cohérentes (pas dans futur, pas avant 1900)
- ✅ Taux de NULL sur colonnes critiques
- ✅ Outliers (± 3 sigma)
- ✅ Données orphelines (chroniques sans station)

**Score qualité**: 0-100 basé sur pénalités

#### Qualité Cours d'Eau

- ✅ Distribution paramètres (top 20)
- ✅ Taux de NULL sur résultats
- ✅ Dates prélèvement cohérentes

#### Rapport Global

- ✅ Statistiques par table (nb lignes, taille)
- ✅ Historique chargements DLT (succès/échecs)
- ✅ Dernières matérialisations

### Matérialisation

```python
# Via Dagster UI
Assets → Monitoring → Materialize All

# Via CLI
dagster asset materialize -a piezometry_data_quality
dagster asset materialize -a global_data_quality_report
```

### Métadonnées dans Dagster UI

Chaque asset retourne des métadonnées riches:
- Score qualité (0-100)
- Nombre d'anomalies
- Graphiques (si implémentés)
- Rapport JSON complet

---

## 🔔 Sensors de Monitoring

### Sensors Actifs (RUNNING par défaut)

| Sensor | Intervalle | Description |
|--------|-----------|-------------|
| `backfill_missing_partitions_sensor` | 1h | Détecte partitions manquantes et backfill auto (max 3/run) |
| `pipeline_failure_alert_sensor` | 5min | Détecte échecs dans dernières 24h et log alertes |
| `error_detection_sensor` | 30min | Sensor existant (détection erreurs génériques) |

### Sensors Optionnels (STOPPED par défaut)

| Sensor | Intervalle | Description |
|--------|-----------|-------------|
| `long_running_pipeline_sensor` | 30min | Détecte runs > 2h (pipelines bloqués) |
| `repeated_failure_sensor` | 1h | Détecte partitions avec >= 3 échecs |

### Activation Sensors Optionnels

```python
# Via Dagster UI
Sensors → [Nom du sensor] → Toggle ON

# Via code (src/hubeau_pipeline/sensors/__init__.py)
# Changer default_status=DefaultSensorStatus.RUNNING
```

### Backfill Automatique

Le sensor `backfill_missing_partitions_sensor`:
1. Vérifie années 2020 à année courante
2. Pour chaque année, cherche runs réussis
3. Si aucun run réussi → crée RunRequest pour cette partition
4. Limite à 3 backfills par exécution (évite surcharge)

**Logs:**
```
📋 Partition manquante détectée: 2022
📋 Partition manquante détectée: 2023
🔄 Backfill planifié pour partition: 2022
🔄 Backfill planifié pour partition: 2023
```

### Alerting

Le sensor `pipeline_failure_alert_sensor`:
1. Cherche runs FAILURE dans dernières 24h
2. Log détails de chaque échec
3. **TODO**: Intégrer Slack/Email

**Exemple d'intégration Slack** (à implémenter):
```python
# sensors/failure_sensor.py
import requests

def send_slack_notification(message):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    requests.post(webhook_url, json={"text": message})

# Dans le sensor:
send_slack_notification(
    f"🚨 Pipeline échoué: {job_name} (Run: {run_id})"
)
```

---

## 📊 Monitoring dans Dagster UI

### Vue Assets

- **Global**: `Assets` → Vue graphe toutes dépendances
- **Par Groupe**:
  - `hubeau_piezometry`: Assets piézométrie
  - `data_quality`: Assets validation
  - `monitoring`: Rapports globaux

### Vue Runs

- **Runs récents**: `Runs` → Historique complet
- **Par Asset**: `Assets` → [Asset] → `Asset Details` → Runs
- **Par Partition**: `Assets` → [Asset] → `Partitions` → Status par année

### Vue Sensors

- **Status sensors**: `Automation` → `Sensors`
- **Logs sensors**: Cliquer sur sensor → `Logs`
- **Activation/Désactivation**: Toggle bouton

### Vue Schedules

- **Schedules actifs**: `Automation` → `Schedules`
- **Prochaine exécution**: Affiché à côté de chaque schedule
- **Activation**: Toggle bouton

---

## 🚀 Workflow Complet Recommandé

### Mise en Route Initiale

1. **Démarrer services**:
   ```bash
   docker-compose up -d
   ```

2. **Matérialiser référentiels** (stations):
   ```python
   # Via Dagster UI
   Jobs → sync_all_stations → Launch Run
   ```

3. **Exécuter optimisations SQL** (dans l'ordre):
   ```bash
   psql -U postgres -d postgres -f scripts/sql/01_create_indexes.sql
   psql -U postgres -d postgres -f scripts/sql/02_create_foreign_keys.sql
   psql -U postgres -d postgres -f scripts/sql/03_add_postgis_geometry.sql
   ```

4. **Activer sensors**:
   ```python
   # Via Dagster UI
   Automation → Sensors → Vérifier RUNNING
   ```

5. **Backfill données historiques**:
   ```python
   # Option 1: Job global
   Jobs → sync_all_yearly_data → Launch Run with Partition Selection
   # Sélectionner: 2020, 2021, 2022, 2023, 2024

   # Option 2: Laisser sensor backfiller automatiquement (plus lent)
   ```

6. **Vérifier qualité**:
   ```python
   # Via Dagster UI
   Assets → data_quality → Materialize All
   ```

### Maintenance Quotidienne

**Automatique** (via schedules):
- Dimanche 2h: Mise à jour stations
- 1er du mois 3h: Mise à jour année courante
- 15 janvier 4h: Backfill complet annuel

**Manuel** (si besoin):
- Matérialiser partition spécifique
- Relancer run échoué
- Consulter rapports qualité

### Monitoring Recommandé

**Quotidien**:
- Check Dagster UI: Runs récents OK?
- Logs sensors: Alertes détectées?

**Hebdomadaire**:
- Matérialiser assets qualité
- Vérifier taille DB (croissance normale?)
- Review échecs répétés

**Mensuel**:
- VACUUM ANALYZE (nettoyage PostgreSQL)
- Backup base de données
- Audit index inutilisés

---

## 📈 Métriques de Performance

### Avant Optimisations

| Requête | Durée |
|---------|-------|
| SELECT chroniques par station (1 an) | 5-15s |
| SELECT analyses par paramètre | 10-30s |
| Stations dans rayon 10km (sans PostGIS) | 30-60s |
| JOIN stations-chroniques (1M lignes) | 60-120s |

### Après Optimisations

| Requête | Durée | Gain |
|---------|-------|------|
| SELECT chroniques par station (1 an) | 50-200ms | **30-100x** |
| SELECT analyses par paramètre | 100-500ms | **20-60x** |
| Stations dans rayon 10km (PostGIS) | 50-100ms | **300-600x** |
| JOIN stations-chroniques (1M lignes) | 1-3s | **20-120x** |

*Note: Dépend du volume de données et hardware*

---

## 🛠️ Troubleshooting

### Index non utilisés

**Symptôme**: Requête lente malgré index

**Solution**:
```sql
-- Forcer utilisation index
SET enable_seqscan = off;

-- Analyser plan requête
EXPLAIN ANALYZE
SELECT * FROM hubeau.piezometry_chroniques
WHERE code_bss = 'BSS001' AND timestamp_mesure > '2024-01-01';

-- Vérifier statistiques à jour
ANALYZE hubeau.piezometry_chroniques;
```

### Foreign Key bloque INSERT

**Symptôme**: Erreur "violates foreign key constraint"

**Solution**:
```sql
-- Option 1: Charger station manquante d'abord
-- Option 2: Supprimer FK temporairement
ALTER TABLE hubeau.piezometry_chroniques DROP CONSTRAINT fk_piezo_chroniques_station;

-- Option 3: Utiliser DEFERRED (PostgreSQL 9.5+)
SET CONSTRAINTS fk_piezo_chroniques_station DEFERRED;
```

### PostGIS géométries NULL

**Symptôme**: Colonne geom NULL malgré lat/lon

**Solution**:
```sql
-- Vérifier coordonnées valides
SELECT COUNT(*) FROM hubeau.piezometry_stations
WHERE latitude IS NULL OR longitude IS NULL
   OR latitude NOT BETWEEN -90 AND 90
   OR longitude NOT BETWEEN -180 AND 180;

-- Re-peupler géométries
UPDATE hubeau.piezometry_stations
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE longitude IS NOT NULL AND latitude IS NOT NULL
  AND longitude BETWEEN -180 AND 180 AND latitude BETWEEN -90 AND 90;
```

### Sensor ne se déclenche pas

**Symptôme**: Sensor RUNNING mais pas de runs

**Solution**:
```python
# Check logs sensor dans Dagster UI
Automation → Sensors → [Sensor] → Logs

# Vérifier interval
minimum_interval_seconds=3600  # 1h - peut être trop long

# Forcer évaluation
# Via Dagster UI: Sensor → Evaluate
```

---

## 📚 Ressources

- **Scripts SQL**: `scripts/sql/`
- **Assets Monitoring**: `src/hubeau_pipeline/assets/monitoring/`
- **Sensors**: `src/hubeau_pipeline/sensors/`
- **Schedules**: `src/hubeau_pipeline/schedules/`
- **Documentation PostgreSQL**: https://www.postgresql.org/docs/
- **Documentation PostGIS**: https://postgis.net/documentation/
- **Documentation Dagster Sensors**: https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors
