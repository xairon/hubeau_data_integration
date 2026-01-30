"""
Dagster Definitions - Entry point
"""

import os
from dagster import Definitions

from .assets import all_assets
from .jobs import all_jobs
from .resources import RESOURCES
from .io.io_managers import noop_io_manager
from .assets.dbt_assets import dbt_resource
from .schedules import all_schedules
from .sensors import all_sensors


def _env_true(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y"}


ENABLE_SENSORS = _env_true("DAGSTER_ENABLE_SENSORS", "false")


defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors if ENABLE_SENSORS else None,
    resources={
        **RESOURCES, 
        "noop_io_manager": noop_io_manager,
        "dbt": dbt_resource,
    },
)
