# Quickstart — from an empty database to something you can look at

This page answers one question: **what do I launch, in what order, and how do I know it
worked?** It is written for someone who has just cloned the repository and has never seen
Dagster.

There are three sensible targets. Pick one before you start — they differ by hours and by tens
of gigabytes.

| Target | What you get | Copernicus key | Rough cost |
|--------|--------------|----------------|------------|
| **A. Ingestion smoke test** | The Bronze layer, loaded from the real APIs. Proves the plumbing works. **dbt cannot complete** — see below | not needed | ~20 min, ~200 MB |
| **B. Demo dataset** | The whole pipeline on one year and one region: Bronze → Silver → Gold, and Junon showing real stations, maps and time series | **required** | ~1 h, ~500 MB |
| **C. Full production** | Everything, France-wide, 1950 → today | **required** | days, tens of GB |

> **The Copernicus key is not optional past Bronze.** `dbt_transform` builds every model, and
> `int_era5_for_all_stations` — which the two daily fact tables depend on — reads
> `stg_era5_timeseries` and `stg_era5_daily_temp_stats`. Without ERA5 in Bronze, dbt fails with
> `relation "bronze.era5_daily_temp_stats" does not exist`, and no Gold table is produced.
> Target A is therefore an ingestion test, not a working warehouse. Budget the key from the
> start unless all you want is to watch data arrive.

Start with A anyway: it is quick, needs no credentials, and if the APIs are unreachable from
your network you find out in twenty minutes instead of two hours.

---

## Before anything: is the stack healthy?

```bash
docker compose ps                 # 6 services; postgres_tuning exits 0 on purpose
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:49500   # Dagster UI -> 200
docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt debug      # -> All checks passed!
```

Then open the Dagster UI at <http://localhost:49500>. Everything below can be done from
**Jobs → *name* → Launchpad → Launch Run**; the shell equivalents are given for automation.

> Schedules and sensors are **enabled by default** in the Docker stack
> (`DAGSTER_ENABLE_SCHEDULES`/`SENSORS` default to `true` in `docker-compose.yml`, unlike the
> `false` in the code). On a fresh install that means nightly ingestion will start firing on
> its own. If you only want to explore, set both to `false` in `.env` and
> `docker compose up -d dlt_worker` — then launch things yourself.

---

## A. Smoke test — no API key

Four jobs, in this order. Wait for each to reach **SUCCESS** before launching the next: Dagster
allows up to five concurrent runs, and only forbids two copies of the *same* pipeline
(see [ARCHITECTURE.md](ARCHITECTURE.md#concurrency)), so launching them all at once would run
them in parallel — not queue them — and the later steps would find no data.

| # | Job | What it loads | Measured on 2026-08-24 |
|---|-----|---------------|------------------------|
| 1 | `reference_data_bronze` | BDLISA/TME hydrogeological entities | 3,716 rows |
| 2 | `piezometry_stations_bronze` | Piezometric station referential | 23,333 rows |
| 3 | `hydrometry_stations_bronze` | Hydrometric sites and stations | 9,283 sites, 6,468 stations |
| 4 | `piezometry_chroniques_bronze` **partition `2025`** | One year of groundwater levels | 1,058,750 rows, ~8 min |
| 5 | `hydrometry_chroniques_bronze` **partition `2025`** | One year of river observations | — |

Steps 4 and 5 are partitioned by year: pick the partition `2025` in the Launchpad before
launching. They query Hub'Eau in batches of 100 stations, so they take several minutes each —
this is normal, watch the `Lot n/234` counter in the logs.

**Do not skip step 5.** `dbt_transform` builds every model, so a Bronze table that was never
loaded fails the whole run with `relation "bronze.hydrometry_obs_elab_raw" does not exist`.

Then transform:

| # | Job | What it does |
|---|-----|--------------|
| 5 | `dbt_transform` | Bronze → Silver → Gold, all 29 models |

> **Loading a past year? Run dbt by hand the first time, with a wide window.** The incremental
> models delete on `CURRENT_DATE - 30 days` but select on the data's own latest date. Load 2025
> today and the delete matches nothing while the insert repeats rows already present — the run
> dies on a primary-key violation. See
> [OPERATIONS.md](OPERATIONS.md#duplicate-key-value-violates-unique-constraint-on-an-incremental-model).
>
> ```bash
> docker exec -w /app/src/dbt_hubeau brgm-dlt-worker dbt build \
>   --vars '{"daily_recompute_window_days": "22000"}'
> ```

| 6 | `station_reference_stats_refresh` | The IPS/SSFI reference grids |
| 7 | `station_index_refresh` | `fct_monthly_index` + `station_current_index` |

If sensors are enabled, step 5 fires by itself once Bronze materializes, and 6/7 follow. Watch
**Runs** rather than launching them by hand.

### Did it work?

```bash
docker exec brgm-postgres psql -U postgres -d postgres -c "
SELECT schemaname, relname AS table_name, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze','silver','gold') AND n_live_tup > 0
ORDER BY schemaname, n_live_tup DESC;"
```

You should see rows in all three schemas. Then `docker exec -w /app/src/dbt_hubeau
brgm-dlt-worker dbt test` for the data-quality assertions.

**In Junon** you now get: the station map, station pages with their time series, the IPS/SSFI
classification. The **Climat tab stays empty** — it needs target B.

---

## B. Demo dataset — real climate on one region

Everything in A, plus ERA5. This is the smallest configuration in which the Climat tab and the
drought indices are meaningful.

**First**, wire and verify your Copernicus key — see [API_KEYS.md](API_KEYS.md). Do the two
verification commands there *before* launching a load; a bad key or an unaccepted licence
fails late and wastes hours.

**Then shrink the ingestion area.** ERA5-Land over the whole of France is about 11,500 grid
cells; a single region is a few hundred. Edit `configs/era5/era5_france_meteo.yml`:

```yaml
# Format: [North, West, South, East]
# area: [51.5, -5.5, 41.0, 10.0]   # production: metropolitan France, ~11,500 cells
  area: [48.6, 0.5, 46.3, 3.2]     # demo: Centre-Val de Loire, ~620 cells
```

`configs/` is bind-mounted into the worker, so the change takes effect immediately — no
rebuild, no restart. Do the same in `configs/era5/era5_daily_temp_stats.yml` if you want true
daily temperatures.

**Why this matters more than shortening the period:** the standardized indices are computed
against a fixed **1991–2020** reference. Loading only two recent years gives you no
climatology, so SPI/STI/SPEI stay NULL and the maps stay blank. Shrinking the *area* instead of
the *period* keeps the 30 years of reference while dividing the volume by roughly eighteen.

Launch `era5_historical_load`, one yearly partition at a time (`1991_1991` … `2020_2020` for
the reference, plus recent years for the display), then:

| Job / asset | Why |
|-------------|-----|
| `dbt_transform` | builds `fct_era5_monthly_grid` and `fct_era5_climatology_grid` |
| asset `fct_era5_spei_climatology_grid` | the SPEI reference — **not** in the nightly job, materialize it explicitly |
| asset `fct_era5_indices_grid` | SPI/STI/SPEI themselves |

Order matters and the SPEI reference must come first — the full procedure, including the
historical backfill trap, is in [ERA5.md](ERA5.md#rebuilding-spei--order-matters).

> The CDS is a **queued** service. A request may return in seconds or sit for an hour. Launch
> the partitions and come back later; do not conclude the job is stuck.

---

## C. Full production

`full_bootstrap` does the whole thing in order: reference data → stations → time series year by
year → ERA5 → dbt. It is restartable (state in `ops.bootstrap_state`), so relaunching resumes
rather than restarting.

Budget **days**, not hours, and tens of gigabytes: piezometry goes back to 1967, hydrometry to
2000, ERA5-Land to 1950. Restrict it first unless you really mean it — see
[OPERATIONS.md](OPERATIONS.md#restricting-what-the-bootstrap-loads), and note that the
restriction variables are not forwarded to the worker by the main compose file.

---

## When a job fails

1. **Dagster UI → Runs → the failed run → Logs.** The failing step and its exception are there.
2. Common causes are collected in [OPERATIONS.md](OPERATIONS.md#5-common-incidents): Hub'Eau
   503, CDS timeouts, TimescaleDB decompression limits, NULL TME labels.
3. `docker compose logs -f dlt_worker` for what the run process itself printed.

> **Do not launch runs with `docker exec … dagster job launch`.** It starts its own ephemeral
> code server, enqueues the run against it, then exits — the daemon can no longer find the code
> location and the run dies with *"This run has been marked as failed from outside the execution
> context"*. Use the UI, or the GraphQL API against the registered code location.
