# Schéma Base de Données Hub'Eau

> **Version** : 5.0 - Architecture DLT + dbt
> **Date** : 2026-01-07

## Architecture

```
Hub'Eau APIs ──┐
               ├──▶ DLT ──▶ staging.* ──▶ dbt ──▶ hubeau.*
ERA5 API ──────┘
```

## Schémas

| Schéma | Gestion | Contenu |
|--------|---------|---------|
| `staging` | DLT | Tables brutes (`*_raw`) |
| `hubeau` | dbt | Tables transformées |

---

## Tables Staging (DLT)

DLT crée automatiquement les tables au premier run.

### Piézométrie
| Table | Description | Volume |
|-------|-------------|--------|
| `piezometry_stations_raw` | Stations BSS | ~23k |
| `piezometry_chroniques_raw` | Mesures niveaux | ~23M |

### Hydrométrie
| Table | Description | Volume |
|-------|-------------|--------|
| `hydrometry_sites_raw` | Sites hydro | ~5k |
| `hydrometry_stations_raw` | Stations hydro | ~5k |
| `hydrometry_obs_elab_raw` | Observations | ~15M |

### ERA5 (Copernicus)
| Table | Description | Volume |
|-------|-------------|--------|
| `era5_france_timeseries` | Météo France métropole | ~300M |

### Métadonnées DLT
- `_dlt_loads` — Historique chargements
- `_dlt_pipeline_state` — État pipelines

---

## Tables Hubeau (dbt)

### Table Principale

#### `hubeau_daily_chroniques`

Données piézométriques enrichies avec météo ERA5.

| Colonne | Type | Description |
|---------|------|-------------|
| `code_bss` | VARCHAR | ID station BSS |
| `date` | DATE | Date mesure |
| `niveau_nappe_eau` | NUMERIC | Niveau nappe (m) |
| `profondeur_nappe` | NUMERIC | Profondeur (m) |
| `temperature_2m` | NUMERIC | Température ERA5 (°C) |
| `total_precipitation` | NUMERIC | Précipitations ERA5 (mm) |
| `potential_evaporation` | NUMERIC | Évaporation ERA5 (mm) |
| `code_commune_insee` | VARCHAR | Code INSEE |
| `nom_commune` | VARCHAR | Commune |
| `code_departement` | VARCHAR | Département |
| `altitude_station` | NUMERIC | Altitude (m) |

**Index** : `(code_bss, date)`, `(date)`, `(code_departement)`

---

## Index Automatiques

dbt crée automatiquement les index suivants au premier run :

```sql
-- ERA5
CREATE INDEX idx_era5_lat_lon_time ON staging.era5_france_timeseries (latitude, longitude, time);

-- Piézométrie
CREATE INDEX idx_piezo_chroniques_full ON staging.piezometry_chroniques_raw (code_bss, date_mesure);
CREATE INDEX idx_piezo_stations_code_bss ON staging.piezometry_stations_raw (code_bss);
```

---

## Requêtes Courantes

### Volume des tables
```sql
SELECT schemaname, tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('staging', 'hubeau')
ORDER BY n_live_tup DESC;
```

### Dernière donnée piézo
```sql
SELECT MAX(date) FROM hubeau.hubeau_daily_chroniques;
```

### Données par département
```sql
SELECT code_departement, COUNT(*) AS nb_mesures
FROM hubeau.hubeau_daily_chroniques
GROUP BY code_departement
ORDER BY nb_mesures DESC;
```
