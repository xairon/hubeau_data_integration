# Database schema

PostgreSQL table structure, layer by layer.

```
Hub'Eau APIs ──┐
               ├──▶ DLT ──▶ bronze.* ──▶ dbt ──▶ silver.* ──▶ dbt ──▶ gold.*
ERA5 API ──────┘
```

| Schema | Owned by | Contents |
|--------|----------|----------|
| `bronze` | DLT + Dagster assets | Raw tables (`*_raw`) plus TME (`tme_entites_hydrogeo`) |
| `silver` | dbt staging | Cleaned tables (`stg_*`) |
| `silver_rejects` | dbt rejects | Filtered-out rows with `rejection_reason` — audit and quality |
| `gold` | dbt (intermediate + marts), Dagster assets, dbt seeds | Analytical tables, standardized indices, reference data |

All four schemas are created on the first run — `bronze` by DLT and the Dagster assets, the
rest by dbt.

## Storage conventions

**Hypertables** (primary key includes the time column, then `create_hypertable`):
`silver.stg_era5_timeseries`, `silver.stg_era5_daily_temp_stats`,
`gold.hubeau_daily_chroniques`, `gold.hydro_daily_chroniques`.

**Compression**: 90 days for the two ERA5 staging tables, 365 days for the two daily marts.
Details in [TIMESCALEDB.md](TIMESCALEDB.md).

**PostGIS**: geometries are built with `make_point(longitude, latitude)` →
`geometry(Point, 4326)` (WGS84), with a GiST index on every `geometry` / `geom` column. Cast
to `::geography` for distances in metres. The `<->` KNN operator relies on that GiST index.

**Indexes**: BRIN on time columns for range queries, B-tree on business keys
(`code_bss`, `code_site`, `code_station`, composites), GiST on geometries.

---

## Bronze

Created automatically on the first run.

| Table | Contents | Rows (measured 2026-08-24) |
|-------|----------|--------------|
| `piezometry_stations_raw` | BSS piezometric stations | 23,333 |
| `piezometry_chroniques_raw` | Groundwater level measurements | ~23 M |
| `hydrometry_sites_raw` | Hydrometric sites | 9,283 |
| `hydrometry_stations_raw` | Hydrometric stations | 6,468 |
| `hydrometry_obs_elab_raw` | Elaborated observations | ~15 M |
| `era5_france_timeseries` | ERA5 time series (direct-to-timeseries) | ~300 M |
| `era5_daily_temp_stats` | True daily temperature statistics | ~320 M |
| `tme_entites_hydrogeo` | TME hydrogeological entities | 3,716 |

Key columns:

- `piezometry_stations_raw` — `code_bss`, `x`, `y`, `nom_commune`, `code_departement`
- `piezometry_chroniques_raw` — `code_bss`, `date_mesure`, `niveau_nappe_eau`, `profondeur_nappe`
- `hydrometry_stations_raw` — `code_station`, `x`, `y`, `code_entite`
- `hydrometry_obs_elab_raw` — `code_entite`, `date_obs_elab`, `resultat_obs_elab`
- `era5_france_timeseries` — `time`, `latitude`, `longitude`, `temperature_2m`, `total_precipitation`, `potential_evaporation`
- `tme_entites_hydrogeo` — `code_eh`, `libelle_eh`, `niveau_eh`, `etat_eh`, `nature_eh`, `milieu_eh`, `theme_eh`, `origine_eh`

TME comes from the Dagster asset `tme_entites_hydrogeo` (local `TME.csv` if present,
otherwise the national ZIP).

DLT also maintains `_dlt_loads` (load history) and `_dlt_pipeline_state`.

---

## Silver (dbt staging)

| Table | Source | Filter |
|-------|--------|--------|
| `stg_piezo_chroniques` | `bronze.piezometry_chroniques_raw` | NULLs dropped |
| `stg_piezo_stations` | `bronze.piezometry_stations_raw` | Non-null coordinates |
| `stg_hydrometry_stations` | `bronze.hydrometry_stations_raw` | Non-null coordinates |
| `stg_hydrometry_sites` | `bronze.hydrometry_sites_raw` | — |
| `stg_hydrometry_obs_elab` | `bronze.hydrometry_obs_elab_raw` | Non-null observations |
| `stg_era5_timeseries` | `bronze.era5_france_timeseries` | Non-null observations |
| `stg_era5_daily_temp_stats` | `bronze.era5_daily_temp_stats` | `min ≤ mean ≤ max` |
| `stg_tme_entites` | `bronze.tme_entites_hydrogeo` | Typing and light normalization |

Each model casts types, renames columns to the project convention, trims, drops NULLs and
builds the PostGIS geometry.

---

## Rejects (`silver_rejects`)

Rows excluded in Silver are never dropped without a trace: they land in a reject table with a
`rejection_reason` column.

| Table | Source | Example reasons |
|-------|--------|-----------------|
| `stg_piezo_chroniques_rejected` | `piezometry_chroniques_raw` | `DATE_MESURE_NULL`, `CODE_BSS_NULL`, `NIVEAU_NAPPE_NULL`, `PROFONDEUR_NAPPE_NULL` |
| `stg_hydrometry_stations_rejected` | `hydrometry_stations_raw` | `CODE_SITE_NOT_IN_SITES`, `CODE_STATION_NULL`, `COORDS_NULL` |
| `stg_hydrometry_obs_elab_rejected` | `hydrometry_obs_elab_raw` | `CODE_SITE_NOT_IN_SITES`, `DATE_OBS_ELAB_NULL`, `GRANDEUR_HYDRO_NULL`, `RESULTAT_OBS_NULL` |

Useful queries live in `src/dbt_hubeau/models/rejects/README.md`.

---

## Gold — intermediate

| Table | Contents |
|-------|----------|
| `int_daily_measurements` | Daily piezometric aggregates by `code_bss` × `date_mesure` (AVG) |
| `int_hydro_daily_measurements` | Daily hydrometric aggregates by `code_station` × `date_obs_elab` × `grandeur_hydro_elab` |
| `int_era5_grid_points` | Distinct ERA5 grid points, for the spatial join |
| `int_station_era5_mapping` | Piezometric stations → nearest ERA5 point, plus TME metadata |
| `int_hydro_station_era5_mapping` | Hydrometric stations → nearest ERA5 point, plus station/site metadata |
| `int_era5_for_all_stations` | ERA5 restricted to the grid points actually used by either domain — one table instead of two |

## Gold — marts

### `hubeau_daily_chroniques`

The main piezometric fact table: groundwater levels + ERA5 weather + TME metadata.
Hypertable (1-year chunks), compressed. Indexed on `(code_bss, date)`, `(date)`,
`(code_departement)`, `(code_eh)`.

| Group | Columns |
|-------|---------|
| Keys | `code_bss`, `date` |
| Observations (all **nullable**) | `niveau_nappe_eau`, `profondeur_nappe`, `temperature_2m`, `total_precipitation`, `potential_evaporation` |
| Station metadata | `codes_bdlisa`, `code_commune_insee`, `nom_commune`, `altitude_station`, `code_departement`, `nom_departement` |
| TME metadata | `code_eh`, `libelle_eh`, `niveau_eh`, `etat_eh`, `nature_eh`, `milieu_eh`, `theme_eh`, `origine_eh` |
| Coordinates | `station_latitude`, `station_longitude`, `era5_latitude`, `era5_longitude` |

> `potential_evaporation` here is the **raw ERA5 PEV**, not the Hargreaves reference ET0 used
> by the climate grid marts. The two differ by roughly a factor of two, on purpose — see
> [ERA5.md](ERA5.md#known-grid--station-inconsistency).

> **Every weather column is nullable, and they fill from two different sources.**
> Precipitation and PET come from `stg_era5_timeseries`; **temperature comes only from
> `stg_era5_daily_temp_stats`**. Loading the ERA5 time series without the daily temperature
> statistics gives a table where `temperature_2m` is NULL on every row, silently — the join is
> a LEFT JOIN and there is no constraint to catch it.
>
> Measured on the 2026-08-24 demo load (2025, one region): 1,058,750 rows, `niveau_nappe_eau`
> 100 % filled, precipitation and PET 95.4 %, TME labels 90 %, `temperature_2m` **0 %** because
> only the time-series path had been loaded. 3,258 of 3,411 stations fell inside the restricted
> ERA5 area.

### `hydro_daily_chroniques`

The hydrometric equivalent, at station × day × quantity grain: `code_station`, `code_site`,
`date`, `grandeur_hydro_elab`, `resultat_obs_elab`, plus station/site metadata and ERA5
weather. Hypertable (1-year chunks), compressed.

### Aggregated facts

| Table | Grain | Contents |
|-------|-------|----------|
| `fct_monthly_chroniques` | station × month | Mean/min/max/stddev, change vs previous month and previous year, 3- and 12-month rolling means. `delete+insert`, 25-month lookback |
| `fct_monthly_hydro` | station × month × quantity | Same, for hydrometry |
| `fct_yearly_stats` | station × year | Annual means, water balance, historical percentiles, class in `TRES_BAS` … `TRES_HAUT` |
| `fct_yearly_hydro` | station × year × quantity | Same, for hydrometry |

### Dimensions

| Table | Contents |
|-------|----------|
| `dim_piezo_stations` | Per station: coverage dates, measurement count, mean level, amplitude, trend, alert level (`NORMAL` / `VIGILANCE` / `ALERTE`), trend quality (`FIABLE` / `INDICATIVE` / `FAIBLE` / `NON_CALCULEE`) |
| `dim_hydro_stations` | Hydrometric station metadata, PostGIS geometry, status (`ACTIVE` / `FERMEE`), headline statistics |
| `dim_date` | `year`, `quarter`, `month`, `week`, `day_of_year`, `iso_day_of_week`, `is_weekend` |
| `dim_geography` | `code_commune`, `nom_commune`, `code_departement`, `nom_departement`, `code_region`, `nom_region` |

### Climate grid

`fct_era5_monthly_grid` and `fct_era5_climatology_grid` are dbt marts; see
[ERA5.md](ERA5.md#climate-grid-marts) for their columns and the PET decision behind them.

---

## Gold — tables built by Dagster, not dbt

Produced by Python assets under `src/hubeau_pipeline/assets/`, group `indices`. The whole
scientific method lives in one pure module, `src/hubeau_pipeline/ml/indices.py`
(`compute_reference_grid`, `grid_to_zscore`, `classify_value`).

They give the standardized piezometric / hydrological index — IPS/SPLI for groundwater, SSFI
for discharge — that is, where a given month sits against its **seasonal normal**, as a
z-score in 7 BSH/Météo-France classes. **The application reads these tables; it no longer
recomputes the indices.**

### Shared method

| Step | Detail |
|------|--------|
| Reference | Fixed window `REF_PERIOD = (1991, 2020)` (WMO/BRGM climate normal). Not a rolling window. |
| Per-station fallback | `_select_reference_window` degrades gracefully: `normale` (1991–2020, ≥15 years) → `adaptee` (best decade-aligned 30-year window, ≥15 years) → `provisoire` (full history, <15 years). The `flag` column carries which one was used. |
| Grid | Per calendar month, empirical percentiles 1→99 (`PCTL_GRID`) over the window. A month with fewer than `MIN_PER_MONTH = 10` observations is interpolated from its circular neighbour; if no month is usable the grid is `NULL` and the index becomes `UNKNOWN` — **a grid is never fabricated**. |
| Z-score | `grid_to_zscore`: percentile rank in the grid → `clip(0.001, 0.999)` → `norm.ppf` → rounded to 3 decimals. Empirical CDF projected onto the standard normal. |
| Classes | z thresholds `[-1.75, -1.28, -0.84, 0.84, 1.28, 1.75]` → `EXTREMEMENT_BAS`, `TRES_BAS`, `BAS`, `NORMAL`, `HAUT`, `TRES_HAUT`, `EXTREMEMENT_HAUT`, plus `UNKNOWN`. In percentiles: `[4.01, 10.03, 20.05, 79.95, 89.97, 95.99]`. |
| Piezometric source | `gold.fct_monthly_chroniques.niveau_moyen` (m NGF), by `code_bss` |
| Hydrometric source | `gold.fct_monthly_hydro.resultat_moyen`, by `code_station`, `positive_only=true` (discharge ≤ 0 discarded) |

> **Cross-repository invariant.** The Junon application ports the same method in
> `dashboard/utils/reference.py` (`value_to_zscore`) — same grid, same clips, same
> thresholds. Warehouse and application cannot drift apart on the method, and a change here
> must be mirrored there.

### `station_reference_stats`

Grain (type, station, calendar month) — 12 rows per station. Holds the reference grid.

Refreshed **weekly**, by `station_reference_stats_refresh` on the `0 7 * * 0` schedule — not
nightly, and not once per decade. The *reference window* is fixed at 1991–2020, but the
per-station grids are not: a station accumulates history, new stations appear, and a station
that was `provisoire` can cross the 15-year threshold and become `normale`. Weekly is the
deliberate compromise (`schedules.py`: "baseline pluriannuelle lente à varier"). The tables
that read it — `fct_monthly_index` and `station_current_index` — are rebuilt nightly by the
sensor chain.

| Column | Type | Description |
|--------|------|-------------|
| `type` | text | `piezo` / `hydro` |
| `code` | text | `code_bss` or `code_station` |
| `month` | int | Calendar month 1–12 |
| `quantile_grid` | jsonb | 99 percentiles, `NULL` when data is insufficient |
| `baseline_start` / `baseline_end` | date | Bounds of the retained window |
| `flag` | text | `normale` / `adaptee` / `provisoire` |
| `n_years` | int | Years in the window |
| `computed_at` | timestamptz | Timestamp |

PK `(type, code, month)`.

### `fct_monthly_index`

Grain (type, station, month) — the complete monthly series, 1967 → current month. Nightly
asset, depends on `station_reference_stats`; re-scores every historical month against the
fixed grid.

| Column | Type | Description |
|--------|------|-------------|
| `type` | text | `piezo` / `hydro` |
| `code` | text | Station code |
| `month` | date | First of the month |
| `z` | double | Standardized index, `NULL` when there is no grid |
| `index_class` | text | One of the 7 classes, or `UNKNOWN` |
| `flag` | text | Reference quality |
| `computed_at` | timestamptz | Timestamp |

PK `(type, code, month)`, index `(type, month)`. Consumed by the application for the sector
timeline and past situation (`observatory_situation.py`, `observatory_common.py`).

> The station-page `/spli` and `/ssfi` endpoints still recompute on the fly instead of
> reading this table. They should be migrated.

### `station_current_index`

Grain (type, station) — one row per station, the latest available month. Nightly asset.

| Column | Type | Description |
|--------|------|-------------|
| `type` / `code` | text | Domain and station |
| `index_name` | text | `IPS` (piezo) or `SSFI` (hydro) |
| `index_value` | double | z-score of the latest month |
| `index_class` | text | One of the 7 classes, or `UNKNOWN` |
| `ref_month` | date | Month classified |
| `baseline_start` / `baseline_end` | date | Reference window used |
| `computed_at` | timestamptz | Timestamp |

PK `(type, code)`, index on `index_class`. Consumed by the map, right drawer, KPIs and the
observatory list.

### ERA5 index tables

`fct_era5_spei_climatology_grid` and `fct_era5_indices_grid` are also Python assets — see
[ERA5.md](ERA5.md#climate-grid-marts).

---

## Reference seed

`ref_stations_meteeau_bsn` is a dbt seed
(`src/dbt_hubeau/seeds/ref_stations_meteeau_bsn.csv`, no computation). It holds the official
BRGM **MétéEAU Nappes** network — 450 point indicators, 431 piezometers plus 19 karst springs
monitored by discharge — so sector aggregation can be restricted to the official network
(`network=meteeau`) and match the BRGM maps. Consumed by
`observatory_situation.py::_official_codes()`.

---

## Useful queries

```sql
-- Table sizes
SELECT schemaname, relname AS table_name, n_live_tup AS rows,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) AS size
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY schemaname, n_live_tup DESC;

-- Latest available data
SELECT MAX(date) AS latest_date FROM gold.hubeau_daily_chroniques;

-- Stations per département
SELECT code_departement, nom_departement,
       COUNT(DISTINCT code_bss) AS n_stations, COUNT(*) AS n_measurements
FROM gold.hubeau_daily_chroniques
GROUP BY code_departement, nom_departement
ORDER BY n_measurements DESC;

-- One station's series
SELECT date, niveau_nappe_eau, profondeur_nappe,
       temperature_2m, total_precipitation, potential_evaporation
FROM gold.hubeau_daily_chroniques
WHERE code_bss = 'BSS001XX0001'
ORDER BY date DESC
LIMIT 100;

-- Coverage per hydrogeological entity
SELECT code_eh, libelle_eh,
       COUNT(DISTINCT code_bss) AS n_stations, COUNT(*) AS n_measurements,
       MIN(date) AS from_date, MAX(date) AS to_date
FROM gold.hubeau_daily_chroniques
WHERE code_eh IS NOT NULL
GROUP BY code_eh, libelle_eh
ORDER BY n_measurements DESC;
```
