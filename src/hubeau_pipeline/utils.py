"""
Shared utility functions for the Hub'Eau pipeline.
"""

import os
from typing import Any


def env_true(name: str, default: str = "false") -> bool:
    """Check if an environment variable is truthy."""
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y"}


def extract_dlt_row_count(load_info: Any, logger=None) -> int:
    """
    Extract total row count from a DLT LoadInfo object.
    Handles both list and dict formats for load_packages/jobs.

    Args:
        load_info: DLT LoadInfo object from pipeline.run()
        logger: Optional logger for error reporting

    Returns:
        Total number of rows loaded
    """
    rows = 0
    try:
        packages = getattr(load_info, "load_packages", []) or []
        for pkg in packages:
            jobs = getattr(pkg, "jobs", [])
            if isinstance(jobs, dict):
                all_jobs = []
                for job_list in jobs.values():
                    if isinstance(job_list, list):
                        all_jobs.extend(job_list)
                jobs = all_jobs
            for job in jobs:
                rows += getattr(job, "metrics", {}).get("items", 0)
    except Exception as e:
        if logger:
            logger.error(f"Failed to extract metrics from load_info: {e}")
    return rows
