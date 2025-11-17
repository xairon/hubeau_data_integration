"""
Dagster Definitions - Application entry point
"""

from dagster import Definitions, multiprocess_executor

from .assets import all_assets
from .jobs import all_jobs
from .schedules import all_schedules
from .sensors import all_sensors
from .resources import RESOURCES
from .io.io_managers import noop_io_manager

# Limit parallelism to 3 concurrent assets (prevents OOM)
limited_executor = multiprocess_executor.configured({
    "max_concurrent": 3
})

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
    resources={
        **RESOURCES,
        "noop_io_manager": noop_io_manager,
    },
    executor=limited_executor,
)
