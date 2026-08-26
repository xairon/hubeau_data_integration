# ERA5 climate data

ERA5-Land (~0.1° native resolution, served by the Copernicus Climate Data Store) feeds every
climate variable in the warehouse. NetCDF files are never archived in the database: they are
downloaded to temporary storage, extracted in memory and inserted straight into the Bronze
tables.

```
Copernicus CDS — ERA5-Land (~0.1°)
    │
    ▼
Dagster job: download NetCDF (tmp) → xarray extraction (memory) → batch insert
    │
    ├──► bronze.era5_france_timeseries      (00:00 UTC sample + accumulation fluxes)
    └──► bronze.era5_daily_temp_stats       (24 hourly steps aggregated locally)
             │
             ▼
        dbt Silver / Gold
```

## Two ingestion paths, and why

| Path | Bronze table | What is downloaded | What it is used for |
|------|--------------|--------------------|---------------------|
| Time series | `era5_france_timeseries` | The 00:00 UTC step | Precipitation, potential evaporation |
| Daily statistics | `era5_daily_temp_stats` | All 24 hourly steps, aggregated locally | True daily mean/min/max temperature |

The split exists because the two kinds of variable behave differently at 00:00 UTC, and
confusing them has already caused one wrong diagnosis:

> **Precipitation and evaporation are accumulation fluxes.** The ECMWF model produces them as
> a running accumulation, so the 00:00 UTC value **is** the correct daily total, not an
> instantaneous sample. Temperature is different: the 00:00 UTC value is a genuine
> instantaneous reading, and taking it as the daily mean introduces a night-time cold bias.
> Only temperature ever needed the 24-step treatment.

ERA5-Land's own documentation states which parameters are accumulations and which are
instantaneous, so this is a convention to read, not an undocumented trap. What the data itself
never signals is *which* of the two a given column is: get it wrong and the series stays
plausible, just too cold.

**Measured, not assumed** (2026-08-26, all 11,496 cells, 2025-01-01 to 2026-08-19,
935,040 cell-days): mean bias **-2.90 °C**, sd 1.85 °C, p5-p95 -5.76 to +0.27 °C. Worst in
June (-3.42 °C) and July (-3.24 °C), mildest in January (-0.63 °C). Reproduce with:

```sql
SELECT round(avg(s.temperature_2m - t.t2m_mean)::numeric, 2) AS bias_c,
       round(stddev(s.temperature_2m - t.t2m_mean)::numeric, 2) AS sd_c,
       count(*) AS cell_days
FROM bronze.era5_france_timeseries s
JOIN bronze.era5_daily_temp_stats t
  ON s.latitude = t.latitude AND s.longitude = t.longitude AND s.time = t.time;
```

The daily-statistics path reads the raw hourly archive (`reanalysis-era5-land`) rather than
the `derived-era5-land-daily-statistics` product. The derived product is computed server-side
on a saturated queue — about 43 hours per requested year, six weeks for a full backfill,
against roughly 25 minutes per year from the raw archive. Equivalence was checked cell-day by
cell-day: Tn/Tx identical to 0.0000 °C, mean to 0.01 °C (the `NUMERIC(6,2)` rounding).

## Bronze tables

### `bronze.era5_france_timeseries`

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Primary key |
| `time` | TIMESTAMP | Date/time (00:00 UTC) |
| `latitude` | NUMERIC(6,3) | Latitude (0.1° grid) |
| `longitude` | NUMERIC(6,3) | Longitude (0.1° grid) |
| `temperature_2m` | NUMERIC(6,2) | 2 m temperature (°C) |
| `total_precipitation` | NUMERIC(8,4) | Total precipitation (mm) |
| `potential_evaporation` | NUMERIC(8,4) | Potential evaporation (mm) |
| `source_file_id` | TEXT | Traceability (e.g. `era5_hist_2024_2025`) |

Hypertable, 1-year chunks, old chunks compressed. Indexes `idx_era5_time`,
`idx_era5_location`.

### `bronze.era5_daily_temp_stats`

`(time, latitude, longitude, t2m_mean, t2m_min, t2m_max, source_file_id, created_at)` —
hypertable with 1-year chunks, compressed after 30 days, Kelvin converted to °C on insert.

Its Silver model `stg_era5_daily_temp_stats` is incremental-append with `DISTINCT ON` dedup,
rounded to one decimal, and tested with not_null / accepted_range / `min ≤ mean ≤ max`.

## Ingestion jobs

| Job | Purpose |
|-----|---------|
| `era5_meteo_job` | Historical backlog (1950 → present), partitioned by year blocks. Deletes the overlapping range then re-inserts, so it is idempotent. |
| `era5_weekly_job` | Daily smart update. Reads `MAX(time)`, derives the missing window, downloads only that delta. Scheduled 03:00 UTC. |
| `era5_daily_temp_historical_load` | Temperature backfill, partitioned by year (`"YYYY_YYYY"` keys), one request per raw hourly month, months downloaded in parallel. |
| `era5_daily_temp_update_job` | Daily temperature smart update. Scheduled 03:30 UTC; days derived from the real window to avoid a stale CADS cache. |

## Spatial mapping

Piezometric and hydrometric stations attach to their nearest ERA5 grid point through a
PostGIS KNN join, in `int_station_era5_mapping` and `int_hydro_station_era5_mapping`:

```sql
SELECT ...
FROM stations s
CROSS JOIN LATERAL (
    SELECT latitude, longitude
    FROM era5_grid e
    ORDER BY s.geom <-> e.geom
    LIMIT 1
) e
```

## Climate grid marts

| Table | Built by | Contents |
|-------|----------|----------|
| `gold.fct_era5_monthly_grid` | dbt | Monthly aggregates per 0.1° cell, 1950 → present (~10.5 M rows) |
| `gold.fct_era5_climatology_grid` | dbt | 1991–2020 normals (gamma MoM + μ/σ) per cell × month × window — serves SPI (gamma) and STI (z-score) |
| `gold.fct_era5_spei_climatology_grid` | Python asset | Generalized-logistic parameters for SPEI (`glo_alpha`, `glo_k`, `glo_xi`), 1991–2020 reference |
| `gold.fct_era5_indices_grid` | Python asset | SPI / STI / SPEI over 1, 3, 6 and 12-month windows |

The two SPEI tables are Python assets rather than dbt models because the L-moment fit needs
the ~30 annual samples *and* the Γ function, which PostgreSQL does not provide.

In `fct_era5_monthly_grid`, `etp_totale` and `bilan_hydrique` are in **positive** mm, and
`temperature_moyenne` / `_min` / `_max` derive from `stg_era5_daily_temp_stats` — the true
daily statistics, not the 00:00 UTC sample. Precipitation, PET and water balance still derive
from `stg_era5_timeseries`, since no true daily source exists for them.

The `ll_*` columns of `fct_era5_spei_climatology_grid` are **dead** — leftovers of the earlier
log-logistic fit, still written but never read.

## Two decisions that shape the numbers

### Reference PET is Hargreaves, not the ERA5 PEV

`etp_totale` is a FAO-56 Hargreaves reference ET0, computed from the true daily Tmin/Tmax/Tmean.
Extraterrestrial radiation `Ra` is derived analytically from latitude and day of year, so no
wind, humidity or radiation input is needed.

ERA5-Land's own `potential_evaporation` was abandoned. Measured over 30,888 cell-months
(2015–2025, same cells):

| | Hargreaves | ERA5-Land PEV | ratio |
|---|---|---|---|
| Annual PET | **818 mm** | **1,756 mm** | **×2.15** |
| P − PET balance | **+146 mm/yr** | **−793 mm/yr** | — |

818 mm/yr matches the published reference ET0 range for France (700–900 mm); 1,756 mm/yr does
not, and put the entire country in permanent water deficit. ERA5's PEV is not a FAO reference
ET0 — it is evaporation from an unstressed surface using the model's aerodynamic resistance,
known to overestimate ET0 substantially. Hargreaves is FAO-56's recommended fallback when
radiation, wind and humidity are unavailable, and is what the attribution literature uses
("ERA5 + Hargreaves"), which keeps our SPEI comparable to published work.

The raw PEV stays exposed as `etp_pev_era5` for traceability. **Do not consume it.**

### SPEI uses the generalized logistic, not the log-logistic

The original 3-parameter log-logistic fit (Vicente-Serrano 2010) only converged on **74.6 %**
of cell × month × window triples. Instrumenting the rejections showed **100 % of them** came
from the `β ≤ 1` guard and **none** from missing data. Since `β = 1/τ₃` and `|τ₃| < 1` always,
`β ≤ 1` implies `τ₃ < 0` — negative skew, which a positively-skewed law structurally cannot
represent. The generalized logistic (`k = −τ₃`) accepts both signs and reaches **100 %
coverage**.

The switch is purely additive: the log-logistic is exactly the GLO reparameterized
(`k = −1/β`), so values are identical wherever the old law worked — verified at a maximum
deviation of 0.000 across 35,614 cells.

## Known grid ↔ station inconsistency

The **station** chain (`int_era5_for_all_stations` → `hubeau_daily_chroniques` /
`hydro_daily_chroniques` / `fct_monthly_*`) still exposes the **raw PEV** under
`potential_evaporation`. It is the forcing input of the Pastas TFN models, and changing it
would invalidate every existing calibration.

This is deliberate, not an oversight. The consequence: a "PET" from the Climat module
(Hargreaves) and a "PET" on the Station page (PEV) are **not the same quantity** and differ by
roughly a factor of two. The `/observatory/era5/*` endpoint rebuilds its daily
`potential_evaporation` from `etp_totale` (`-(etp_totale / n_days)`), so that endpoint follows
Hargreaves.

Station-level `temperature_2m` does come from the true daily statistics. Monthly station
`temperature_min` / `_max` remain min/max **of daily means**, not true Tn/Tx: exposing those
would require adding `t2m_min` / `t2m_max` at station grain, which means altering a
hypertable.

## Rebuild procedures

### Rebuilding SPEI — order matters

1. **Reference first.** It is deliberately absent from the nightly job: it is a fixed
   1991–2020 reference, pointless to recompute every night.
   ```bash
   docker exec brgm-dlt-worker dagster asset materialize \
     --select fct_era5_spei_climatology_grid -m hubeau_pipeline.definitions   # ~2.5 min
   ```
2. **Then the indices.**
   ```bash
   docker exec brgm-dlt-worker dagster asset materialize \
     --select fct_era5_indices_grid -m hubeau_pipeline.definitions
   ```
   > On an **already-populated** table this asset only recomputes the **last three months**
   > (`latest_index_month()` is non-null, so it takes the nightly branch). After any method
   > change, `spei` would stay NULL across the whole history. A historical backfill is
   > **mandatory**: loop `_compute_range` in 5-year slices from 1950 to today. It upserts
   > (`ON CONFLICT DO UPDATE`), so it is non-destructive — spi/sti are rewritten identically.
   > Measured: ~12,000 rows/s, about **50 minutes** for 41.96 M rows.
3. **Purge the Junon cache**: `junon:obs_climat_*` (and `junon-redis-dev` for the dev stack),
   otherwise the app serves stale values for up to 24 hours.

### Loading temperature after the time series — the silent NULL

If `bronze.era5_france_timeseries` is loaded first and `bronze.era5_daily_temp_stats` after, for
the **same period**, a plain `dbt build` leaves `temperature_2m` NULL on every row. No error,
no warning: every test passes.

The cause is in `int_era5_for_all_stations`. Its temperature CTE carries an incremental guard:

```sql
{% if is_incremental() %}
WHERE d.time > (SELECT COALESCE(MAX(era5_date), '1900-01-01'::date) FROM {{ this }})
{% endif %}
```

The table already holds rows up to the end of the period, put there by the time-series path. So
`d.time > MAX(era5_date)` matches nothing, the CTE is empty, and the downstream `LEFT JOIN`
yields NULL for every row. The guard assumes temperature always arrives *later* than the time
series, never *alongside* it.

It bites on a demo load, on any repair of the temperature path, and on the cutover procedure
itself. Measured on 2026-08-24: 4,196,040 rows in Bronze, 4,196,040 in Silver, and **0 %** of
`gold.hubeau_daily_chroniques` rows with a temperature.

Fix — rebuild the intermediate model in full, then propagate:

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run \
  --full-refresh --select int_era5_for_all_stations

docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run \
  --select hubeau_daily_chroniques hydro_daily_chroniques \
  --vars '{"daily_recompute_window_days": "22000"}'
```

After that, measured: temperature filled on **95.4 %** of rows — the same rate as precipitation,
the missing 4.6 % being stations outside the ingested ERA5 area. Mean 12.8 °C, min −2.7 °C,
max 31.1 °C, which is what a year in central France should look like.

> Check the fill rate rather than assuming it. A NULL column here is invisible: the join is a
> LEFT JOIN and no constraint catches it.
>
> ```sql
> SELECT round(100.0*count(temperature_2m)/count(*),1) AS pct_temperature
> FROM gold.hubeau_daily_chroniques;
> ```

### Backfilling temperature — the silver trap

> **A Bronze backfill does not reach Silver on its own.** `stg_era5_daily_temp_stats` is
> incremental-append with the filter `time > (SELECT MAX(time) FROM {{ this }})`. The nightly
> asset already populates Silver with the current year, so `MAX(time)` is roughly today: every
> backfilled row has `time < MAX(time)` and is **silently skipped** — no error, but the mart
> ends up with temperature only on the years already present and NULL everywhere else. Force a
> Silver reprocess over the whole backfilled range *before* rebuilding the mart. This applies
> to **every** future temperature backfill.

```bash
# Option A (recommended) — full refresh: rebuilds Silver from Bronze, the incremental
# filter does not apply, no PK conflict possible.
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --full-refresh --select stg_era5_daily_temp_stats

# Option B — TRUNCATE then targeted reprocess. The TRUNCATE is required: the model is
# `append`, so re-inserting from 1950 into a non-empty table violates the
# (latitude, longitude, time) PK on the recent overlap.
docker exec -it brgm-postgres psql -U postgres -d postgres \
  -c "TRUNCATE silver.stg_era5_daily_temp_stats;"
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --select stg_era5_daily_temp_stats \
  --vars '{era5_daily_temp_reprocess_from_timestamp: "1950-01-01"}'
```

`era5_daily_temp_reprocess_from_timestamp` replaces the `time > MAX(time)` filter with
`time >= <ts>::timestamp` — same mechanism as `era5_reprocess_from_timestamp` on the twin
model `stg_era5_timeseries`. Check afterwards that
`SELECT MIN(time) FROM silver.stg_era5_daily_temp_stats;` returns 1950 and not the current
year, then rebuild the mart:

```bash
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt run --full-refresh --select fct_era5_monthly_grid
```

### Rebuilding the station marts

> **Never run `--full-refresh` on the two daily hypertables** — it produces phantom
> hypertables. Use the window variable instead.

```bash
# 1. intermediate (plain table, safe)
dbt run --full-refresh --select int_era5_for_all_stations
# 2. daily hypertables: historical reprocess through the widened window (covers 1967 →)
dbt run --select hubeau_daily_chroniques --vars '{"daily_recompute_window_days": "22000"}'
dbt run --select hydro_daily_chroniques  --vars '{"daily_recompute_window_days": "22000"}'
# 3. downstream plain marts (they aggregate from the hypertables)
dbt run --full-refresh --select fct_monthly_chroniques fct_monthly_hydro \
                                fct_yearly_stats fct_yearly_hydro dim_piezo_stations
```

Then flush the Junon Redis cache and do a visual pass. IPS/SSFI is unaffected — it does not
consume temperature.

## Maintenance

### Inspect what is loaded

```sql
SELECT source_file_id, COUNT(*) AS n_rows, MIN(time) AS from_date, MAX(time) AS to_date
FROM bronze.era5_france_timeseries
GROUP BY source_file_id
ORDER BY source_file_id DESC;
```

### Reload a period

From the Dagster UI: job `era5_meteo_job` → pick the partition covering the period → launch.
The job deletes the overlap before re-inserting; a manual `DELETE` first is wise if the
existing data is corrupt.
