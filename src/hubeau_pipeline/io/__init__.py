"""
Hub'Eau Pipeline Resources Package

This package contains custom resources for the Hub'Eau pipeline,
including custom IO managers.
"""

# Import io_managers to make them available
from .io_managers import noop_io_manager

__all__ = ["noop_io_manager"]
