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

# Stub dagster so asset modules can be imported without the full install.
if "dagster" not in sys.modules:
    dagster_stub = types.ModuleType("dagster")

    def _asset(*args, **kwargs):
        """Passthrough decorator stub for dagster.asset."""
        if args and callable(args[0]):
            return args[0]
        return lambda fn: fn

    dagster_stub.asset = _asset
    dagster_stub.AssetExecutionContext = object
    dagster_stub.MetadataValue = types.SimpleNamespace(int=lambda x: x)
    sys.modules["dagster"] = dagster_stub

# Stub hubeau_pipeline.resources (PostgreSQLResource not needed in unit tests).
if "hubeau_pipeline.resources" not in sys.modules:
    res_stub = types.ModuleType("hubeau_pipeline.resources")
    res_stub.PostgreSQLResource = object  # type: ignore[assignment]
    sys.modules["hubeau_pipeline.resources"] = res_stub

