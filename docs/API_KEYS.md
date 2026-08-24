# API keys

The pipeline talks to three external services. Only one of them needs a key.

| Source | What it provides | Key needed |
|--------|------------------|------------|
| **Hub'Eau** (`hubeau.eaufrance.fr`) | Piezometric stations and time series, hydrometric sites/stations/observations | **No.** Open API, no registration, no quota to declare |
| **BDLISA / TME** | Hydrogeological entities used to enrich stations | **No.** Public download |
| **Copernicus CDS** (`cds.climate.copernicus.eu`) | ERA5-Land reanalysis: temperature, precipitation, potential evaporation | **Yes** — see below |

## Why the Copernicus key matters

ERA5 is the only climate source in the warehouse. Without it:

- `bronze.era5_france_timeseries` and `bronze.era5_daily_temp_stats` stay empty
- every ERA5-derived Gold table is empty — `fct_era5_monthly_grid`, `fct_era5_climatology_grid`,
  `fct_era5_indices_grid`, `fct_era5_spei_climatology_grid`
- the **Climat tab of the Junon observatory has nothing to show**, and the SPI/STI/SPEI layers
  are blank
- the two daily fact tables (`hubeau_daily_chroniques`, `hydro_daily_chroniques`) join station
  measurements against ERA5, so they lose their weather columns

Piezometry and hydrometry **ingest** perfectly well without any key — Bronze fills up, and you
can query it. But the transformation layer does not: `dbt_transform` builds every model, and
`int_era5_for_all_stations`, on which both daily fact tables depend, reads the two ERA5 staging
models. Without ERA5 in Bronze it fails with `relation "bronze.era5_daily_temp_stats" does not
exist` and **no Gold table is produced at all**.

So a key-less install is an ingestion test, not a usable warehouse. Get the key before you
start unless watching data arrive is all you want.

## Getting a key

1. **Create an account** at <https://cds.climate.copernicus.eu>. It is free; an institutional
   address works.
2. **Accept the dataset licence.** This is the step that catches everyone: the account can be
   valid and the API still refuse to serve data. Open the
   [ERA5-Land dataset page](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land),
   go to the **Download** tab, scroll to *Terms of use*, and accept
   **"Licence to use Copernicus products"**. Nothing works until this is ticked.
3. **Copy your Personal Access Token** from your profile page
   (<https://cds.climate.copernicus.eu/profile>).

### Which format?

The CDS was rebuilt in 2024 and the credential format changed.

| | Old CDS (retired) | Current CDS |
|---|---|---|
| API URL | `https://cds.climate.copernicus.eu/api/v2` | `https://cds.climate.copernicus.eu/api` |
| Credential | `UID:api-key` (e.g. `12345:abcd-...`) | a single UUID, e.g. `00000000-0000-0000-0000-000000000000` |

This project targets the **current** CDS: `cds_api_url` is set to
`https://cds.climate.copernicus.eu/api` in `configs/era5/era5_france_meteo.yml` and
`configs/era5/era5_daily_temp_stats.yml`. Supply the plain UUID — a `UID:key` pair will be
rejected.

## Where the key goes

One place: `COPERNICUS_API_KEY` in the `.env` file at the repository root.

```bash
cp .env.example .env
# then edit:
COPERNICUS_API_KEY=00000000-0000-0000-0000-000000000000
```

`docker-compose.yml` forwards it to the `dlt_worker` container, which is the only place it is
needed. It is read at `assets/bronze/era5_assets.py:211` and
`assets/bronze/era5_daily_temp_assets.py:577`, with the variable name itself overridable per
source through `credentials.cds_api_key_env` in the YAML config.

Apply a change with:

```bash
docker compose up -d dlt_worker
```

`.env` is gitignored. Never commit the key; if it leaks, revoke it from the CDS profile page
and issue a new one.

## Verifying the key before you launch anything

Both checks run inside the worker, against the real service. Do them before starting a
multi-hour ERA5 load — a bad key or an unaccepted licence fails late otherwise.

```bash
# 1. Is the token valid? Expect HTTP 200 and your account email.
docker exec brgm-dlt-worker python3 -c "
import os, urllib.request, json
req = urllib.request.Request('https://cds.climate.copernicus.eu/api/profiles/v1/account',
                             headers={'PRIVATE-TOKEN': os.environ['COPERNICUS_API_KEY']})
r = urllib.request.urlopen(req, timeout=25)
print(r.status, json.loads(r.read()).get('email'))"

# 2. Is the ERA5 licence accepted? Expect the id to appear in the list.
docker exec brgm-dlt-worker python3 -c "
import os, urllib.request, json
req = urllib.request.Request('https://cds.climate.copernicus.eu/api/profiles/v1/account/licences',
                             headers={'PRIVATE-TOKEN': os.environ['COPERNICUS_API_KEY']})
acc = {l['id'] for l in json.loads(urllib.request.urlopen(req, timeout=25).read())['licences']}
print('licence-to-use-copernicus-products' in acc)"
```

A `401` means the token is wrong or was revoked. A `403` on a data request, with a valid
token, almost always means step 2 of *Getting a key* was skipped.

## What to expect from the CDS once it works

The CDS is a **queued** service, not a synchronous API. A request is submitted, waits behind
other users, then downloads. Queue time is unpredictable — seconds to hours, depending on load
and on how large the request is.

Two consequences for this pipeline:

- Requests are chunked (`chunking.years_per_request: 2` for the time series) to stay under the
  per-request limits. A full 1950 → present backfill is dozens of queued requests.
- The daily-temperature path issues **one request per month**, because it pulls all 24 hourly
  steps. A 30-year backfill is 360 requests. Measured throughput on the raw archive is about
  25 minutes per year for the whole of France; restricting `area` in the YAML config shrinks
  it proportionally.

Copernicus also publishes ERA5-Land with roughly five days of latency, which is why the
ingestion window stops at `today − ERA5_AVAILABILITY_LAG_DAYS` (default 5). See
[CONFIGURATION.md](CONFIGURATION.md#era5-copernicus).
