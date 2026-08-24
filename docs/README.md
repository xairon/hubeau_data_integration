# Documentation map

Every document in this repository, by what you are trying to do. Nothing lives here that is
not listed below; if you add a document, add its line too.

**Verified on 2026-08-24** against commit `0360237`.

## Start here

| Document | Read it when |
|----------|--------------|
| [../README.md](../README.md) | You are new. What the project does, how to install and start it. |
| [QUICKSTART.md](QUICKSTART.md) | The stack is up and you need to know **what to launch, in what order, and how to check it worked**. Three targets: a key-less smoke test, a regional demo dataset with real climate indices, or the full production load. |

## Understand what this is

| Document | Read it when |
|----------|--------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | You need the shape of the system: Medallion layers, what Dagster orchestrates, which container does what, where the code lives. |
| [ERA5.md](ERA5.md) | You touch anything climate. Explains the two ingestion paths, why reference PET is Hargreaves and not the ERA5 PEV, why SPEI uses the generalized logistic, and the grid ↔ station inconsistency that is deliberate. |

## Look something up

| Document | Read it when |
|----------|--------------|
| [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md) | You need a table, a column, or the method behind the standardized indices. Also lists which Gold tables are built by Dagster rather than dbt. |
| [CONFIGURATION.md](CONFIGURATION.md) | You are setting an environment variable. Includes which variables actually reach the worker container — several do not. |
| [DATA_SOURCES.md](DATA_SOURCES.md) | You are publishing, presenting or exporting anything built on this data. Copernicus requires a verbatim acknowledgement and disclaimer; Hub'Eau and BDLISA expect attribution. |
| [API_KEYS.md](API_KEYS.md) | You are wiring credentials. Which sources need a key (only Copernicus), how to obtain it, the licence step everyone forgets, and two commands to verify it before launching a multi-hour load. |
| [TIMESCALEDB.md](TIMESCALEDB.md) | You need the hypertable, compression or index settings that are specific to this project. |

## Do something

| Document | Read it when |
|----------|--------------|
| [OPERATIONS.md](OPERATIONS.md) | You are running the thing: initial bootstrap, daily checks, reprocessing a window, an incident, backup and restore. |
| [DEPLOY_SANDBOX.md](DEPLOY_SANDBOX.md) | You are deploying to the research sandbox through Portainer + GitOps. |
| [../CLAUDE.md](../CLAUDE.md) | You are a coding agent, or you want the condensed list of traps that have already cost time. |

## Related repository

The **Junon observatory** (`time-serie-explo`) consumes this warehouse: it reads
`gold.fct_era5_*`, `gold.fct_monthly_index` and `gold.station_current_index` over the
`hubeau_data_integration_default` Docker network. Its deployment guide carries the ordered
procedure for standing up both stacks together. Two invariants bind the repositories:

- The IPS/SSFI classification maths is duplicated in both and guarded by matching golden
  tables — see [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md#shared-method).
- This stack must be started first, and its checkout directory must stay named
  `hubeau_data_integration`.

## Conventions

- **One fact, one place.** A fact lives in exactly one document; everywhere else links to it.
  If you find the same table described twice, one of them is wrong.
- **Every document is listed here.** A document absent from this map is unfindable, which
  makes it worse than no document.
- **Dates are claims.** The "verified on" date at the top of this file and of the README says
  when someone last checked the content against the code. Update it when you check; do not
  update it when you only edit prose.
- **Design notes and implementation plans do not live in the repository.** They were removed
  on 2026-08-24. What survived from them is folded into the documents above; the rest is in
  the Git history (`git log --diff-filter=D --name-only` to find a file, `git show <sha>^:<path>`
  to read it).
