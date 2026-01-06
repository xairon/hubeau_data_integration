"""
Jobs Aggregation - Silver Layer

Jobs pour créer les tables agrégées combinant ERA5 et piézométrie.
"""

from dagster import define_asset_job, AssetSelection, AssetKey


aggregation_job = define_asset_job(
    name="aggregation_piezometry_era5",
    description=(
        "Silver: Create aggregated table combining piezometry measurements "
        "with ERA5 weather data at daily granularity. "
        "Creates staging.station_era5_mapping and staging.daily_piezometry_era5. "
        "Runtime: ~15 minutes."
    ),
    selection=AssetSelection.keys(
        AssetKey("station_era5_mapping"),
        AssetKey("daily_piezometry_era5")
    ),
    tags={"dagster/concurrency_key": "aggregation"}
)
