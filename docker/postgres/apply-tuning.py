"""Apply PostgreSQL performance tuning via Patroni REST API.

Runs once after PostgreSQL is healthy. Settings persist in Patroni's
DCS and are re-applied on every PostgreSQL restart automatically.
"""
import json
import urllib.request
import sys
import time

PATRONI_URL = "http://postgres:8008/config"

TUNING = {
    "postgresql": {
        "parameters": {
            # Memory - work_mem is critical for dbt JOINs/GROUP BY/CTEs
            "work_mem": "128MB",
            # SSD I/O optimization
            "random_page_cost": 1.1,
            "effective_io_concurrency": 200,
            "maintenance_io_concurrency": 200,
            # WAL & checkpoints - optimized for DLT bulk ingestion
            "max_wal_size": "8GB",
            "min_wal_size": "2GB",
            "wal_buffers": "64MB",
            "wal_compression": "on",
            "checkpoint_timeout": 900,
            # Query planner
            "default_statistics_target": 500,
            "jit": "off",
        }
    }
}


def apply_tuning(retries=5, delay=5):
    for attempt in range(1, retries + 1):
        try:
            data = json.dumps(TUNING).encode()
            req = urllib.request.Request(
                PATRONI_URL,
                data=data,
                method="PATCH",
                headers={"Content-Type": "application/json"},
            )
            resp = urllib.request.urlopen(req, timeout=10)
            result = resp.read().decode()
            print(f"Patroni config updated: {result}")
            return True
        except Exception as e:
            print(f"Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    return False


if __name__ == "__main__":
    print("Applying PostgreSQL performance tuning via Patroni API...")
    if apply_tuning():
        print("Done. Settings will persist across restarts.")
    else:
        print("Failed to apply tuning after all retries.", file=sys.stderr)
        sys.exit(1)
