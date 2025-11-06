"""
Generic CSV Ingestion Source - Config-Driven

Ce module permet d'ingérer N'IMPORTE QUEL CSV sans créer de nouveau code.

WORKFLOW:
1. User drops CSV in /app/data/csv_inbox/
2. User creates minimal YAML config in /app/configs/csv_ingestion/
3. Sensor detects new files and triggers ingestion
4. DLT auto-detects schema and loads to staging_<table_name>
5. DBT transforms staging → production tables

NO MORE MANUAL ASSETS/JOBS FOR EACH FILE!
"""

import dlt
import pandas as pd
import yaml
from pathlib import Path
from typing import Iterator, List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class CSVIngestionConfig:
    """
    Config loader for CSV ingestion.
    Reads YAML configs from /app/configs/csv_ingestion/
    """

    def __init__(self, config_name: str):
        """
        Args:
            config_name: Name of YAML config file (without .yml extension)
        """
        config_dir = Path("/app/configs/csv_ingestion")
        config_path = config_dir / f"{config_name}.yml"

        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        logger.info(f"Loaded CSV config: {config_name}")

    @property
    def file_pattern(self) -> str:
        return self.config["source"]["file_pattern"]

    @property
    def delimiter(self) -> str:
        return self.config["source"].get("delimiter", ",")

    @property
    def date_columns(self) -> Optional[List[str]]:
        return self.config["source"].get("date_columns")

    @property
    def table_name(self) -> str:
        # Préfixe "staging_" pour toutes les tables CSV
        table = self.config["destination"]["table_name"]
        return f"staging_{table}"

    @property
    def write_disposition(self) -> str:
        return self.config["destination"].get("write_disposition", "replace")

    @property
    def primary_key(self) -> Optional[List[str]]:
        return self.config["destination"].get("primary_key")


@dlt.resource
def csv_to_staging(
    config_name: str,
    csv_inbox_dir: str = "/app/data/csv_inbox"
) -> Iterator[List[Dict[str, Any]]]:
    """
    Generic DLT resource for CSV ingestion.

    Args:
        config_name: Name of YAML config (e.g., "piezometers")
        csv_inbox_dir: Directory where CSVs are dropped

    Yields:
        Batches of records (10k rows per batch for memory efficiency)

    Example:
        # Just create piezometers.yml config, then:
        @asset
        def ingest_piezometers(context):
            pipeline = _create_dlt_pipeline("csv_piezometers", context)
            return pipeline.run(csv_to_staging(config_name="piezometers"))
    """
    # Load config
    config = CSVIngestionConfig(config_name)
    inbox = Path(csv_inbox_dir)

    # Find matching CSV files
    csv_files = list(inbox.glob(config.file_pattern))

    if not csv_files:
        logger.warning(f"No CSV files found matching: {config.file_pattern}")
        return

    logger.info(f"Found {len(csv_files)} CSV file(s) to ingest")

    # Process each file
    for csv_file in csv_files:
        logger.info(f"Processing: {csv_file.name}")

        # Read CSV with pandas
        df = pd.read_csv(
            csv_file,
            sep=config.delimiter,
            parse_dates=config.date_columns,
            low_memory=False
        )

        total_rows = len(df)
        logger.info(f"Loaded {total_rows:,} rows from {csv_file.name}")

        # Normalize column names (lowercase, replace spaces with underscores)
        df.columns = (
            df.columns
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('é', 'e')
            .str.replace('è', 'e')
            .str.replace('ê', 'e')
            .str.replace('à', 'a')
            .str.replace('ù', 'u')
        )

        # Replace NaN with None for PostgreSQL
        df = df.where(pd.notnull(df), None)

        # Yield in batches (10k rows for memory efficiency)
        batch_size = 10000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            yield batch.to_dict('records')

            if (i + batch_size) % 50000 == 0:
                logger.info(f"Progress: {i + batch_size:,}/{total_rows:,} rows")

        logger.info(f"✅ Completed: {csv_file.name} ({total_rows:,} rows)")


def get_csv_configs() -> List[str]:
    """
    Scan /app/configs/csv_ingestion/ and return list of available configs.

    Returns:
        List of config names (without .yml extension)

    Example:
        >>> get_csv_configs()
        ['piezometers', 'meteo_data', 'quality_samples']
    """
    config_dir = Path("/app/configs/csv_ingestion")

    if not config_dir.exists():
        return []

    configs = [
        f.stem for f in config_dir.glob("*.yml")
        if not f.name.startswith('_')
    ]

    return sorted(configs)


def get_pending_csv_files(csv_inbox_dir: str = "/app/data/csv_inbox") -> List[Path]:
    """
    Scan CSV inbox directory and return list of CSV files.

    Args:
        csv_inbox_dir: Directory to scan

    Returns:
        List of CSV file paths

    Example:
        >>> get_pending_csv_files()
        [PosixPath('/app/data/csv_inbox/piezometers_2024.csv')]
    """
    inbox = Path(csv_inbox_dir)

    if not inbox.exists():
        inbox.mkdir(parents=True, exist_ok=True)
        return []

    csv_files = list(inbox.glob("*.csv"))
    return sorted(csv_files, key=lambda p: p.stat().st_mtime)
