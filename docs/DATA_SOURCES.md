# Data sources and attribution

This warehouse redistributes data it does not own. Each source carries its own terms, and two
of them require an explicit acknowledgement in anything built on top. **Any public deployment,
publication or screenshot derived from this project must carry the notices below.**

| Source | What it provides | Terms |
|--------|------------------|-------|
| **Copernicus Climate Change Service (C3S) — ERA5-Land** | Temperature, precipitation, potential evaporation | [Licence to use Copernicus Products](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land) — acknowledgement **required** |
| **Hub'Eau** (eaufrance) | Piezometric stations and levels, hydrometric sites, stations and discharge | [hubeau.eaufrance.fr](https://hubeau.eaufrance.fr/) — French open public data, attribution expected |
| **BDLISA** (BRGM / Sandre) | TME hydrogeological entities | [bdlisa.eaufrance.fr](https://bdlisa.eaufrance.fr/) — attribution expected |

## Copernicus — the required wording

The Copernicus licence asks for a specific acknowledgement and a specific disclaimer. Reproduce
both, verbatim, wherever the data or a product derived from it is shown:

> Generated using Copernicus Climate Change Service information, 2026.

> Neither the European Commission nor ECMWF is responsible for any use that may be made of the
> Copernicus information or data it contains.

Adjust the year to the year of generation. This applies to the Climat tab of the observatory,
to any exported figure, and to any publication using the SPI, STI or SPEI values — those are
derived products, and the obligation follows the derivation.

## Hub'Eau and BDLISA

Both are French public datasets published through the Eaufrance portal. Credit the source when
redistributing or displaying the data:

> Source: Hub'Eau — eaufrance.fr

> Source: BDLISA — BRGM / Sandre

## Before publishing

The exact terms of each source can change, and this page is a summary written on 2026-08-24,
not legal advice. Before a public release, open each of the three links above and confirm the
current wording — particularly the Copernicus one, whose formula is prescriptive.

Note also what this project does **not** redistribute: no raw Hub'Eau or ERA5 archive is
committed here. The repository contains code and configuration; the data is fetched at runtime
by whoever runs the pipeline, under their own account for Copernicus. That keeps the
redistribution question narrow — it concerns what you *display*, not what you ship.
