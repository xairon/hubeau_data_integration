# TME config (hydrogeological entities reference)

Configuration for ingesting the TME reference dataset (Tableau Multi-Échelles).

- **bdlisa_entites.yml** — where the TME file is fetched from: the national BDLISA ZIP, or a
  custom URL.
- See the [quickstart](../../docs/QUICKSTART.md) for how this fits into a first load, and
  [ARCHITECTURE.md](../../docs/ARCHITECTURE.md) for where TME lands in the Medallion layers.

## Where the data comes from

The pipeline loads `TME.csv` from, in order:

1. a local `TME.csv` file (takes precedence)
2. the national BDLISA ZIP (fallback)
3. a custom URL configured in `bdlisa_entites.yml`

## Scope

Full BDLISA (with geometries) and the Sandre nomenclatures were removed from the pipeline.
Only the base TME reference is ingested.
