"""conftest.py — stub heavy packages so pure-Python ML tests can run without
the full Dagster / DLT / Pastas stack installed.
"""
import sys
import types

# ---------------------------------------------------------------------------
# Pre-stub hubeau_pipeline package so its __init__.py (which imports dagster)
# is NOT executed when tests import hubeau_pipeline.ml.* submodules.
# ---------------------------------------------------------------------------
if "hubeau_pipeline" not in sys.modules:
    pkg = types.ModuleType("hubeau_pipeline")
    pkg.__path__ = ["src/hubeau_pipeline"]  # type: ignore[assignment]
    pkg.__package__ = "hubeau_pipeline"
    sys.modules["hubeau_pipeline"] = pkg

if "hubeau_pipeline.ml" not in sys.modules:
    ml = types.ModuleType("hubeau_pipeline.ml")
    ml.__path__ = ["src/hubeau_pipeline/ml"]  # type: ignore[assignment]
    ml.__package__ = "hubeau_pipeline.ml"
    sys.modules["hubeau_pipeline.ml"] = ml
