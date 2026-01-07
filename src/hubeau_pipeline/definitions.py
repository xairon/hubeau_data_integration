"""
Dagster Definitions - Entry point
"""

from dagster import Definitions

from .assets import all_assets
from .jobs import all_jobs
from .resources import RESOURCES
from .io.io_managers import noop_io_manager
from .assets.dbt_assets import dbt_resource


defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    resources={
        **RESOURCES, 
        "noop_io_manager": noop_io_manager,
        "dbt": dbt_resource,
    },
)
