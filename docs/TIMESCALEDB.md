# TimescaleDB in this project

What is actually configured here — which tables are hypertables, how they are compressed,
which index types are used and why. For how TimescaleDB works in general, read the
[TimescaleDB documentation](https://docs.timescale.com/); this page does not repeat it.

## Hypertables

A hypertable is split into **chunks** along a time column, each chunk being an ordinary
PostgreSQL table. Queries filtered on that column skip the chunks they do not overlap.

Four dbt models are converted, through the `convert_to_hypertable` macro
(`macros/timescaledb.sql`):

| Model | Schema | Time column | Chunk interval |
|-------|--------|-------------|----------------|
| `hubeau_daily_chroniques` | `gold` | `date` | 1 year |
| `hydro_daily_chroniques` | `gold` | `date` | 1 year |
| `stg_era5_timeseries` | `silver` | `time` | 1 month |
| `stg_era5_daily_temp_stats` | `silver` | `time` | 1 month |

ERA5 uses a one-month interval because its grid produces far more rows per unit of time
than station series do.

Bronze ERA5 tables are turned into hypertables separately, in
`docker/postgres/init.sql:125`.

**Constraint to remember**: the primary key must include the time column, otherwise
`create_hypertable` refuses the conversion. In the macro chain this is why
`add_primary_key` has to run *before* `convert_to_hypertable`.

## Compression

Set through the `enable_compression` macro, which also registers the retention-style
compression policy.

| Table | Segment by | Order by | Compressed after |
|-------|-----------|----------|------------------|
| `hubeau_daily_chroniques` | `code_bss` | `date DESC` | 365 days |
| `hydro_daily_chroniques` | `code_station` | `date DESC` | 365 days |
| `stg_era5_timeseries` | — | `time DESC` | 90 days |
| `stg_era5_daily_temp_stats` | — | `time DESC` | 90 days |
| Bronze ERA5 tables | `source_file_id` | — | set in `assets/bronze/era5_assets.py` |

Station series segment by station code because almost every query filters on one station.
The ERA5 series have no useful segment key — a grid cell is not a natural filter — so they
are only ordered by time.

**Querying compressed data needs no special SQL.** TimescaleDB decompresses the chunks a
query touches, on the fly. Reading compressed chunks is usually cheaper in I/O than reading
the same rows uncompressed, so queries spanning the compression boundary stay fine.

## Index types

Declared per model in the dbt `config(indexes=[...])` block, so the authoritative list is
the model file itself.

| Type | Used for | Where |
|------|----------|-------|
| **BRIN** | Time columns, which are already physically ordered inside chunks. Stores a min/max summary per block range instead of an entry per row — tiny, and enough to skip blocks on a date-range filter. | `date`, `time`, `date_mesure`, `date_obs_elab`, `era5_date`, `mois` |
| **GiST** | PostGIS geometries: `ST_Contains`, `ST_DWithin`, `ST_Intersects`, and the `<->` nearest-neighbour operator. | `geom`, `geometry` on every spatial model |
| **B-tree** | Everything else: equality, joins, sorting. The default when a type is not specified. | `code_bss`, `code_station`, composite keys |

The `<->` operator is what makes the station → nearest ERA5 grid point mapping viable in
`int_station_era5_mapping` and `int_hydro_station_era5_mapping`; without the GiST index that
KNN join degrades to a full scan of the grid.
