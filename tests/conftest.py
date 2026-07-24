"""conftest.py — stub heavy packages so pure-Python ML tests can run without
the full Dagster / DLT / Pastas stack installed.
"""
import sys
import types
from pathlib import Path

_SRC_ROOT = str(Path(__file__).resolve().parent.parent / "src" / "hubeau_pipeline")

# ---------------------------------------------------------------------------
# Pre-stub hubeau_pipeline package so its __init__.py (which imports dagster)
# is NOT executed when tests import hubeau_pipeline.ml.* submodules.
# ---------------------------------------------------------------------------
if "hubeau_pipeline" not in sys.modules:
    pkg = types.ModuleType("hubeau_pipeline")
    pkg.__path__ = [_SRC_ROOT]  # type: ignore[assignment]
    pkg.__package__ = "hubeau_pipeline"
    sys.modules["hubeau_pipeline"] = pkg

if "hubeau_pipeline.ml" not in sys.modules:
    ml = types.ModuleType("hubeau_pipeline.ml")
    ml.__path__ = [str(Path(_SRC_ROOT) / "ml")]  # type: ignore[assignment]
    ml.__package__ = "hubeau_pipeline.ml"
    sys.modules["hubeau_pipeline.ml"] = ml

# ---------------------------------------------------------------------------
# Stub hubeau_pipeline.assets package so its __init__.py (which imports dlt,
# dagster, etc.) is NOT executed when tests import assets.monthly_index_assets.
# ---------------------------------------------------------------------------
if "hubeau_pipeline.assets" not in sys.modules:
    assets_pkg = types.ModuleType("hubeau_pipeline.assets")
    assets_pkg.__path__ = [str(Path(_SRC_ROOT) / "assets")]  # type: ignore[assignment]
    assets_pkg.__package__ = "hubeau_pipeline.assets"
    sys.modules["hubeau_pipeline.assets"] = assets_pkg

# Same trick one level deeper: hubeau_pipeline.assets.bronze/__init__.py pulls
# in dlt_assets.py + era5_assets.py (dlt, cdsapi, psycopg2...) as a side
# effect of the package import. Stubbing the package lets tests import a
# single submodule (e.g. era5_daily_temp_assets) without paying for the rest
# of the bronze package.
if "hubeau_pipeline.assets.bronze" not in sys.modules:
    bronze_pkg = types.ModuleType("hubeau_pipeline.assets.bronze")
    bronze_pkg.__path__ = [str(Path(_SRC_ROOT) / "assets" / "bronze")]  # type: ignore[assignment]
    bronze_pkg.__package__ = "hubeau_pipeline.assets.bronze"
    sys.modules["hubeau_pipeline.assets.bronze"] = bronze_pkg

# Stub dagster so asset modules can be imported without the full install.
if "dagster" not in sys.modules:
    dagster_stub = types.ModuleType("dagster")

    def _asset(*args, **kwargs):
        """Passthrough decorator stub for dagster.asset."""
        if args and callable(args[0]):
            return args[0]
        return lambda fn: fn

    class _StaticPartitionsDefinitionStub:
        """Minimal stand-in: only stores the partition keys, no scheduling logic."""

        def __init__(self, partition_keys, **kwargs):
            self.partition_keys = partition_keys

    class _OutputStub:
        """Minimal stand-in for dagster.Output: keeps value/metadata, no I/O manager wiring."""

        def __init__(self, value, metadata=None):
            self.value = value
            self.metadata = metadata

    dagster_stub.asset = _asset
    dagster_stub.AssetExecutionContext = object
    dagster_stub.MetadataValue = types.SimpleNamespace(int=lambda x: x, text=lambda x: x)
    dagster_stub.StaticPartitionsDefinition = _StaticPartitionsDefinitionStub
    dagster_stub.Output = _OutputStub
    sys.modules["dagster"] = dagster_stub

# Stub hubeau_pipeline.resources (PostgreSQLResource not needed in unit tests).
if "hubeau_pipeline.resources" not in sys.modules:
    res_stub = types.ModuleType("hubeau_pipeline.resources")
    res_stub.PostgreSQLResource = object  # type: ignore[assignment]
    sys.modules["hubeau_pipeline.resources"] = res_stub

# Stub psycopg2 (persistence modules only need execute_values at import time;
# unit tests exercise pure logic, never a real DB connection).
if "psycopg2" not in sys.modules:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_extras_stub = types.ModuleType("psycopg2.extras")
    psycopg2_extras_stub.execute_values = lambda *args, **kwargs: None
    psycopg2_stub.extras = psycopg2_extras_stub
    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extras"] = psycopg2_extras_stub

# Stub dagster_dbt (get_asset_key_for_model not needed to exercise pure logic).
if "dagster_dbt" not in sys.modules:
    dagster_dbt_stub = types.ModuleType("dagster_dbt")
    dagster_dbt_stub.get_asset_key_for_model = lambda *args, **kwargs: None
    dagster_dbt_stub.DbtCliResource = object
    dagster_dbt_stub.DbtProject = object
    dagster_dbt_stub.dbt_assets = lambda *args, **kwargs: (lambda fn: fn)
    sys.modules["dagster_dbt"] = dagster_dbt_stub

# Stub hubeau_pipeline.assets.dbt_assets (compiles the dbt manifest as a side
# effect of import — far too heavy for pure-Python asset unit tests).
if "hubeau_pipeline.assets.dbt_assets" not in sys.modules:
    dbt_assets_stub = types.ModuleType("hubeau_pipeline.assets.dbt_assets")
    dbt_assets_stub.hubeau_dbt_assets = None
    sys.modules["hubeau_pipeline.assets.dbt_assets"] = dbt_assets_stub

