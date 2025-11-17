"""
GRPC Server Entry Point for Dagster Code Location

This file is used by the GRPC server to load the Dagster definitions.
With --watch flag, changes to source code trigger automatic reload.

Usage in docker-compose:
  command: dagster api grpc -f /app/src/hubeau_pipeline/grpc_server.py --watch
"""

from hubeau_pipeline.definitions import defs

# Export definitions for Dagster
__all__ = ["defs"]
