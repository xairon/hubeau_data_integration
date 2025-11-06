"""
CSV Ingestion Sensor - Auto-detect new CSV files

WORKFLOW:
1. Sensor runs every 60 seconds
2. Scans /app/data/csv_inbox/ for new CSV files
3. Matches CSV files with YAML configs
4. Triggers corresponding asset when new file detected

BENEFITS:
- Zero-touch ingestion: drop CSV → auto-ingested
- No manual triggering needed
- Files archived after successful ingestion
"""

from pathlib import Path
from typing import Optional
from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    DefaultSensorStatus
)

from hubeau_pipeline.sources.csv_source import (
    get_csv_configs,
    get_pending_csv_files
)


@sensor(
    name="csv_file_watcher",
    description="Auto-detect new CSV files in inbox and trigger ingestion",
    minimum_interval_seconds=60,  # Check every minute
    default_status=DefaultSensorStatus.STOPPED  # Must be manually enabled
)
def csv_file_watcher_sensor(context: SensorEvaluationContext):
    """
    Sensor qui surveille /app/data/csv_inbox/ et déclenche l'ingestion automatiquement.

    LOGIC:
    1. Scan csv_inbox for CSV files
    2. Check if corresponding YAML config exists
    3. Trigger asset if match found
    4. Track last processed files in sensor cursor

    Example:
        User drops: piezometers_2024.csv
        Config exists: piezometers.yml (file_pattern: piezometers*.csv)
        → Triggers: csv_piezometers asset
    """
    csv_inbox = Path("/app/data/csv_inbox")
    csv_configs = get_csv_configs()

    if not csv_configs:
        return SkipReason("No CSV ingestion configs found in /app/configs/csv_ingestion/")

    # Get pending files
    pending_files = get_pending_csv_files()

    if not pending_files:
        return SkipReason("No CSV files found in inbox")

    # Load cursor (track processed files)
    cursor = context.cursor or ""
    processed_files = set(cursor.split(",")) if cursor else set()

    run_requests = []

    for csv_file in pending_files:
        file_key = csv_file.name

        # Skip if already processed
        if file_key in processed_files:
            continue

        # Find matching config
        matched_config = None
        for config_name in csv_configs:
            # Simple pattern matching (can be enhanced with fnmatch/regex)
            # For now: exact name match or starts with config_name
            if (
                csv_file.stem == config_name or
                csv_file.stem.startswith(config_name)
            ):
                matched_config = config_name
                break

        if matched_config:
            context.log.info(f"New CSV detected: {csv_file.name} → config: {matched_config}.yml")

            # Create run request for corresponding asset
            run_requests.append(
                RunRequest(
                    run_key=f"{matched_config}_{file_key}",
                    asset_selection=[f"csv_{matched_config}"],
                    tags={
                        "csv_file": csv_file.name,
                        "config": matched_config,
                        "auto_triggered": "true"
                    }
                )
            )

            # Mark as processed
            processed_files.add(file_key)
        else:
            context.log.warning(
                f"No matching config for: {csv_file.name}. "
                f"Available configs: {', '.join(csv_configs)}"
            )

    # Update cursor
    if processed_files:
        new_cursor = ",".join(sorted(processed_files))
        context.update_cursor(new_cursor)

    if run_requests:
        return run_requests
    else:
        return SkipReason(f"No new CSV files to process (checked {len(pending_files)} files)")


@sensor(
    name="csv_archive_cleaner",
    description="Archive processed CSV files after successful ingestion",
    minimum_interval_seconds=3600,  # Check every hour
    default_status=DefaultSensorStatus.STOPPED  # Must be manually enabled
)
def csv_archive_cleaner_sensor(context: SensorEvaluationContext):
    """
    Optional: Archive or delete successfully processed CSV files.

    LOGIC:
    1. Check which CSVs have been successfully ingested
    2. Move to /app/data/csv_archive/{date}/
    3. Keep inbox clean

    NOTE: Enable this sensor manually if you want auto-archiving
    """
    csv_inbox = Path("/app/data/csv_inbox")
    csv_archive = Path("/app/data/csv_archive")

    # Implementation: check last runs, move processed files
    # This is a placeholder - implement based on your archiving policy

    return SkipReason("Archive cleaner not yet implemented")
