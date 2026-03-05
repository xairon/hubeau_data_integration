"""
Dagster Definitions - Entry point
"""

from dagster import Definitions

from .assets import all_assets
from .jobs import all_jobs
from .resources import RESOURCES
from .io.io_managers import noop_io_manager
from .assets.dbt_assets import dbt_resource
from .schedules import all_schedules
from .sensors import all_sensors


defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
    resources={
        **RESOURCES, 
        "noop_io_manager": noop_io_manager,
        "dbt": dbt_resource,
    },
)
