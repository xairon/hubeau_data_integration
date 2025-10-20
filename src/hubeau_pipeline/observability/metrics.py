"""
Prometheus Metrics pour Hub'Eau Pipeline
Instrumentation pour DLT workers et Dagster assets
"""
import time
from functools import wraps
from typing import Any, Callable
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    Info,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
    multiprocess,
    CollectorRegistry as MultiprocessCollectorRegistry
)
import os

# ═══════════════════════════════════════════════════════════
# REGISTRY CONFIGURATION (multiprocess pour workers)
# ═══════════════════════════════════════════════════════════

def get_registry():
    """
    Get Prometheus registry (multiprocess-aware pour DLT workers)
    """
    if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
        # Mode multiprocess (DLT workers)
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return registry
    else:
        # Mode single-process (orchestrator)
        from prometheus_client import REGISTRY
        return REGISTRY


# ═══════════════════════════════════════════════════════════
# METRICS DEFINITIONS
# ═══════════════════════════════════════════════════════════

# DLT Extraction Metrics
dlt_records_extracted_total = Counter(
    'hubeau_dlt_records_extracted_total',
    'Total number of records extracted from Hub\'Eau API',
    ['source', 'resource', 'partition']
)

dlt_records_loaded_total = Counter(
    'hubeau_dlt_records_loaded_total',
    'Total number of records loaded to destination',
    ['source', 'resource', 'destination', 'partition']
)

dlt_extraction_duration_seconds = Histogram(
    'hubeau_dlt_extraction_duration_seconds',
    'Time spent extracting data from Hub\'Eau API',
    ['source', 'resource', 'partition'],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600]  # 1s à 1h
)

dlt_load_duration_seconds = Histogram(
    'hubeau_dlt_load_duration_seconds',
    'Time spent loading data to destination',
    ['source', 'resource', 'destination', 'partition'],
    buckets=[1, 5, 10, 30, 60, 120, 300]  # 1s à 5min
)

dlt_errors_total = Counter(
    'hubeau_dlt_errors_total',
    'Total number of DLT errors',
    ['source', 'resource', 'error_type', 'partition']
)

# API Metrics
hubeau_api_requests_total = Counter(
    'hubeau_api_requests_total',
    'Total number of requests to Hub\'Eau API',
    ['endpoint', 'status_code']
)

hubeau_api_rate_limit_hits_total = Counter(
    'hubeau_api_rate_limit_hits_total',
    'Total number of rate limit hits',
    ['endpoint']
)

hubeau_api_response_time_seconds = Histogram(
    'hubeau_api_response_time_seconds',
    'Hub\'Eau API response time',
    ['endpoint'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]  # 100ms à 30s
)

# Data Quality Metrics
data_quality_check_failures_total = Counter(
    'hubeau_data_quality_check_failures_total',
    'Total number of data quality check failures',
    ['source', 'resource', 'check_type', 'partition']
)

data_quality_invalid_records_total = Counter(
    'hubeau_data_quality_invalid_records_total',
    'Total number of invalid records detected',
    ['source', 'resource', 'validation_rule', 'partition']
)

# Pipeline State Metrics
pipeline_runs_total = Counter(
    'hubeau_pipeline_runs_total',
    'Total number of pipeline runs',
    ['pipeline_name', 'status']  # status: success, failure, retry
)

pipeline_duration_seconds = Histogram(
    'hubeau_pipeline_duration_seconds',
    'Pipeline run duration',
    ['pipeline_name', 'partition'],
    buckets=[60, 300, 600, 1800, 3600, 7200, 14400]  # 1min à 4h
)

# Dagster Asset Metrics
dagster_asset_materializations_total = Counter(
    'hubeau_dagster_asset_materializations_total',
    'Total number of asset materializations',
    ['asset_key', 'partition']
)

dagster_asset_materialization_duration_seconds = Histogram(
    'hubeau_dagster_asset_materialization_duration_seconds',
    'Asset materialization duration',
    ['asset_key', 'partition'],
    buckets=[60, 300, 600, 1800, 3600, 7200]
)

# Incremental Loading Metrics
incremental_last_timestamp = Gauge(
    'hubeau_incremental_last_timestamp',
    'Last timestamp processed in incremental loading',
    ['source', 'resource']
)

incremental_gap_days = Gauge(
    'hubeau_incremental_gap_days',
    'Number of days between last loaded data and now',
    ['source', 'resource']
)


# ═══════════════════════════════════════════════════════════
# DECORATORS FOR AUTOMATIC INSTRUMENTATION
# ═══════════════════════════════════════════════════════════

def track_extraction(source: str, resource: str, partition: str = ""):
    """
    Decorator to track DLT extraction metrics

    Usage:
        @track_extraction("piezometry", "chroniques", "2024")
        def extract_data():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time

                # Count records if result is iterable
                if hasattr(result, '__iter__'):
                    count = sum(1 for _ in result)
                    dlt_records_extracted_total.labels(
                        source=source,
                        resource=resource,
                        partition=partition
                    ).inc(count)

                dlt_extraction_duration_seconds.labels(
                    source=source,
                    resource=resource,
                    partition=partition
                ).observe(duration)

                return result

            except Exception as e:
                dlt_errors_total.labels(
                    source=source,
                    resource=resource,
                    error_type=type(e).__name__,
                    partition=partition
                ).inc()
                raise

        return wrapper
    return decorator


def track_api_request(endpoint: str):
    """
    Decorator to track Hub'Eau API requests

    Usage:
        @track_api_request("/chroniques")
        def fetch_chroniques():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            status_code = 200

            try:
                result = func(*args, **kwargs)
                return result

            except Exception as e:
                # Extract status code from exception if available
                if hasattr(e, 'status_code'):
                    status_code = e.status_code
                elif hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                    status_code = e.response.status_code
                else:
                    status_code = 500
                raise

            finally:
                duration = time.time() - start_time

                hubeau_api_requests_total.labels(
                    endpoint=endpoint,
                    status_code=status_code
                ).inc()

                hubeau_api_response_time_seconds.labels(
                    endpoint=endpoint
                ).observe(duration)

        return wrapper
    return decorator


def track_pipeline(pipeline_name: str, partition: str = ""):
    """
    Decorator to track complete pipeline runs

    Usage:
        @track_pipeline("piezometry_chroniques", "2024")
        def run_pipeline():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            status = "success"

            try:
                result = func(*args, **kwargs)
                return result

            except Exception as e:
                status = "failure"
                raise

            finally:
                duration = time.time() - start_time

                pipeline_runs_total.labels(
                    pipeline_name=pipeline_name,
                    status=status
                ).inc()

                pipeline_duration_seconds.labels(
                    pipeline_name=pipeline_name,
                    partition=partition
                ).observe(duration)

        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════
# METRICS ENDPOINT (pour exposition HTTP)
# ═══════════════════════════════════════════════════════════

def metrics_handler():
    """
    Handler pour endpoint /metrics (compatible avec Prometheus scraping)
    """
    registry = get_registry()
    metrics_output = generate_latest(registry)
    return metrics_output, CONTENT_TYPE_LATEST


# ═══════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════

def record_data_quality_failure(source: str, resource: str, check_type: str, partition: str = ""):
    """Record a data quality check failure"""
    data_quality_check_failures_total.labels(
        source=source,
        resource=resource,
        check_type=check_type,
        partition=partition
    ).inc()


def record_invalid_records(source: str, resource: str, validation_rule: str, count: int = 1, partition: str = ""):
    """Record invalid records detected"""
    data_quality_invalid_records_total.labels(
        source=source,
        resource=resource,
        validation_rule=validation_rule,
        partition=partition
    ).inc(count)


def update_incremental_state(source: str, resource: str, last_timestamp: float, gap_days: float):
    """Update incremental loading state metrics"""
    incremental_last_timestamp.labels(
        source=source,
        resource=resource
    ).set(last_timestamp)

    incremental_gap_days.labels(
        source=source,
        resource=resource
    ).set(gap_days)
