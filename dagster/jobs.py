"""Dagster job definitions for the dlt ingestion assets."""
from __future__ import annotations

from dagster import ScheduleDefinition, define_asset_job

from dagster.assets.dlt_assets import hydrobio_taxons


sync_hubeau_daily = define_asset_job(
    name="sync_hubeau_daily",
    selection=[hydrobio_taxons.key],
)


sync_hubeau_daily_schedule = ScheduleDefinition(
    job=sync_hubeau_daily,
    cron_schedule="0 4 * * *",
)
