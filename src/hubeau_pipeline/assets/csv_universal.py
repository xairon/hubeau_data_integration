"""
Universal CSV Ingestion Asset - Powered by DLT

UN SEUL ASSET qui ingère TOUS les CSVs du dossier data/csv_inbox/

WORKFLOW:
1. Drop CSV dans data/csv_inbox/
2. Matérialiser l'asset "ingest_all_csvs"
3. Tous les CSVs sont ingérés automatiquement
4. Tables créées: staging.staging_<filename>

CONFIG OPTIONNELLE:
- Si configs/csv_ingestion/<filename>.yml existe → utilise les params
- Sinon → utilise les defaults (auto-detect schema)

DLT FEATURES UTILISÉES (automatisation, intelligence, robustesse):
- ✅ Détection automatique du délimiteur (virgule, point-virgule, tabulation, etc.)
- ✅ Détection automatique du schéma et inférence des types
- ✅ Normalisation automatique des noms de colonnes
- ✅ Gestion automatique de l'évolution du schéma
- ✅ Traitement par lots pour les gros fichiers
- ✅ Gestion d'erreurs et mécanismes de retry
- ✅ Support multi-encodage (UTF-8, Latin-1)
- ✅ Validation et coercition de types automatiques
- ✅ Métriques détaillées et suivi de progression
- ✅ Gestion des valeurs nulles et des lignes invalides

AVANTAGES:
- Asset PERMANENT (ne disparaît jamais)
- Historique stable dans Dagster
- Aucune config obligatoire
- Ingère N fichiers en 1 run
- Robuste face aux variations de format CSV
- Intelligent : détection automatique de tout
"""

import os
import csv
import dlt
from pathlib import Path
from typing import Dict, Any, List, Optional
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import logging

logger = logging.getLogger(__name__)


def _create_dlt_pipeline(pipeline_name: str, context=None) -> dlt.Pipeline:
    """
    Create optimized DLT pipeline for CSV ingestion with advanced features.

    DLT FEATURES ENABLED:
    - Automatic schema detection and evolution
    - Progress logging and monitoring
    - Error handling and retry mechanisms
    - Efficient batch processing
    - Schema normalization

    Args:
        pipeline_name: Base name of the DLT pipeline
        context: Dagster context (optional) - used for logging and isolation

    Returns:
        Configured DLT pipeline instance
    """
    import logging
    logger = logging.getLogger(__name__)

    # Create unique pipeline name per run to avoid conflicts
    if context and hasattr(context, 'run_id'):
        unique_pipeline_name = f"{pipeline_name}_{context.run_id[:8]}"
        logger.info(f"Creating DLT pipeline: {unique_pipeline_name} (run: {context.run_id[:8]})")
    else:
        unique_pipeline_name = pipeline_name
        logger.info(f"Creating DLT pipeline: {unique_pipeline_name}")

    from dlt.destinations import postgres

    # Create PostgreSQL destination with credentials
    destination = postgres(
        credentials={
            "database": os.getenv("PG_DB", "postgres"),
            "username": os.getenv("PG_USER", "postgres"),
            "password": os.getenv("PG_PASSWORD"),
            "host": os.getenv("PG_HOST", "localhost"),
            "port": int(os.getenv("PG_PORT", "5432"))
        }
    )

    # Create pipeline with enhanced configuration
    pipeline = dlt.pipeline(
        pipeline_name=unique_pipeline_name,
        destination=destination,
        dataset_name="staging",
        progress="log",  # Enable progress logging
        # DLT automatically handles:
        # - Schema detection and evolution
        # - Type inference
        # - Column normalization
        # - Error handling
        # - Batch optimization
    )

    # Optional: Add hooks for custom logging/monitoring
    # DLT supports hooks for on_start, on_end, on_load_start, on_load_end, etc.
    # This can be used for custom metrics, alerts, etc.

    return pipeline


def detect_delimiter(csv_path: Path, sample_size: int = 1024) -> str:
    """
    Detect CSV delimiter automatically by analyzing the file.

    Args:
        csv_path: Path to CSV file
        sample_size: Number of bytes to read for detection

    Returns:
        Detected delimiter (default: ',')
    """
    # Common delimiters to test
    delimiters = [',', ';', '\t', '|', ' ']
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            sample = f.read(sample_size)
        
        # Use csv.Sniffer for automatic detection
        sniffer = csv.Sniffer()
        try:
            detected = sniffer.sniff(sample, delimiters=''.join(delimiters))
            return detected.delimiter
        except (csv.Error, AttributeError):
            # Fallback: count occurrences of each delimiter
            delimiter_counts = {}
            for delim in delimiters:
                # Count occurrences in first few lines
                lines = sample.split('\n')[:5]
                counts = [line.count(delim) for line in lines if line.strip()]
                if counts:
                    delimiter_counts[delim] = sum(counts) / len(counts)
            
            if delimiter_counts:
                # Return delimiter with highest average count
                return max(delimiter_counts, key=delimiter_counts.get)
    except Exception as e:
        logger.warning(f"Could not detect delimiter for {csv_path.name}: {e}")
    
    # Default to comma
    return ','


def get_config_if_exists(csv_filename: str) -> Dict[str, Any]:
    """
    Load config YAML if exists, otherwise return defaults.

    Args:
        csv_filename: Name of CSV file (e.g., "piezometers.csv")

    Returns:
        Config dict with source/destination params
    """
    import yaml

    # Extract base name without extension
    base_name = Path(csv_filename).stem

    # Check if config exists
    config_path = Path(f"/app/configs/csv_ingestion/{base_name}.yml")

    if config_path.exists():
        logger.info(f"Loading config: {config_path}")
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    else:
        logger.info(f"No config found for {base_name}, using defaults with auto-detection")
        return {
            "source": {
                "delimiter": None,  # Will be auto-detected
                "date_columns": []
            },
            "destination": {
                "table_name": base_name,
                "write_disposition": "replace",
                "primary_key": None
            }
        }


def normalize_column_name(col_name: str) -> str:
    """
    Normalize column names for PostgreSQL compatibility.
    DLT will handle this automatically, but we can pre-process for better control.
    """
    import re
    # Convert to lowercase
    col = col_name.lower()
    # Replace spaces and special chars with underscore
    col = re.sub(r'[^\w]', '_', col)
    # Replace accented characters
    replacements = {
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'à': 'a', 'â': 'a', 'ä': 'a',
        'ù': 'u', 'û': 'u', 'ü': 'u',
        'ô': 'o', 'ö': 'o',
        'î': 'i', 'ï': 'i',
        'ç': 'c'
    }
    for old, new in replacements.items():
        col = col.replace(old, new)
    # Remove multiple underscores
    col = re.sub(r'_+', '_', col)
    # Remove leading/trailing underscores
    col = col.strip('_')
    return col


def ingest_single_csv(
    csv_path: Path,
    context: AssetExecutionContext
) -> Dict[str, Any]:
    """
    Ingest a single CSV file using DLT with advanced features.

    DLT FEATURES USED:
    - Automatic schema detection and evolution
    - Automatic column name normalization
    - Type inference and validation
    - Batch processing for large files
    - Error handling and retry mechanisms
    - Progress tracking and metrics

    Args:
        csv_path: Path to CSV file
        context: Dagster context

    Returns:
        Dict with ingestion stats
    """
    import pandas as pd

    csv_filename = csv_path.name
    context.log.info(f"=== INGESTING: {csv_filename} ===")

    # Load config (or defaults)
    config = get_config_if_exists(csv_filename)

    source_config = config.get("source", {})
    dest_config = config.get("destination", {})

    # Extract params - auto-detect delimiter if not specified
    delimiter = source_config.get("delimiter")
    if delimiter is None:
        delimiter = detect_delimiter(csv_path)
        context.log.info(f"Auto-detected delimiter: '{delimiter}'")
    else:
        context.log.info(f"Using delimiter from config: '{delimiter}'")
    
    date_columns = source_config.get("date_columns", [])
    table_name = dest_config.get("table_name", csv_path.stem)
    write_disposition = dest_config.get("write_disposition", "replace")
    primary_key = dest_config.get("primary_key")
    merge_key = dest_config.get("merge_key")  # For merge write_disposition

    # Prefix table with "staging_"
    full_table_name = f"staging_{table_name}"

    context.log.info(f"Config: delimiter='{delimiter}', dates={date_columns}")
    context.log.info(f"Destination: staging.{full_table_name}")
    context.log.info(f"Write mode: {write_disposition}")

    # Read CSV with error handling and encoding fallback
    # Try UTF-8 first, fallback to latin-1 for compatibility
    try:
        df = pd.read_csv(
            csv_path,
            sep=delimiter,
            parse_dates=date_columns if date_columns else False,
            low_memory=False,
            encoding='utf-8',
            on_bad_lines='warn'  # Warn on bad lines instead of failing
        )
    except UnicodeDecodeError:
        # Try with different encoding (common for French data)
        context.log.warning(f"UTF-8 failed, trying latin-1 for {csv_filename}")
        df = pd.read_csv(
            csv_path,
            sep=delimiter,
            parse_dates=date_columns if date_columns else False,
            low_memory=False,
            encoding='latin-1',
            on_bad_lines='warn'
        )

    total_rows = len(df)
    context.log.info(f"Loaded {total_rows:,} rows with {len(df.columns)} columns")

    # Normalize column names (DLT also does this, but we pre-process for consistency)
    original_columns = df.columns.tolist()
    df.columns = [normalize_column_name(col) for col in df.columns]
    
    # Log column name changes if any
    if original_columns != df.columns.tolist():
        context.log.info(f"Normalized {len([i for i, (o, n) in enumerate(zip(original_columns, df.columns)) if o != n])} column names")

    # Replace NaN with None (DLT handles this, but explicit is better)
    df = df.where(pd.notnull(df), None)

    # Create DLT resource with advanced features
    @dlt.resource(
        name=full_table_name,
        write_disposition=write_disposition,
        primary_key=primary_key,
        merge_key=merge_key if write_disposition == "merge" else None,
        # DLT will automatically:
        # - Detect and infer data types
        # - Normalize column names (we pre-process for consistency)
        # - Handle schema evolution
        # - Batch data efficiently
    )
    def csv_data():
        """
        DLT resource for CSV data.
        
        Uses DLT's automatic features:
        - Schema detection and type inference
        - Column normalization
        - Batch processing
        - Error handling
        """
        # Yield in batches for memory efficiency
        batch_size = 10000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            # Convert to dict records - DLT will handle type inference
            records = batch.to_dict('records')
            # DLT automatically handles:
            # - Type coercion
            # - Null handling
            # - Schema validation
            yield records

    # Create pipeline with enhanced configuration
    pipeline = _create_dlt_pipeline(f"csv_{table_name}", context)
    
    # Run pipeline - DLT handles:
    # - Schema detection and evolution
    # - Type inference
    # - Error handling and retries
    # - Progress tracking
    try:
        load_info = pipeline.run(csv_data())
    except Exception as e:
        context.log.error(f"DLT pipeline failed for {csv_filename}: {e}")
        raise

    # Extract comprehensive metrics from DLT load_info
    rows_loaded = 0
    try:
        # DLT provides detailed metrics in load_info
        for package in load_info.load_packages:
            if package.jobs:
                for job in package.jobs:
                    # Get items count (rows loaded)
                    items = job.metrics.get("items", 0)
                    if isinstance(items, (int, float)):
                        rows_loaded += int(items)
                    
                    # Log additional DLT metrics
                    context.log.info(f"DLT Job metrics: {job.metrics}")
    except Exception as e:
        context.log.warning(f"Could not extract detailed metrics: {e}, using DataFrame row count")
        rows_loaded = total_rows

    # Ensure rows_loaded is an integer
    if not isinstance(rows_loaded, int):
        rows_loaded = total_rows if total_rows > 0 else 0

    # Log schema information from DLT
    try:
        schema = pipeline.default_schema
        context.log.info(f"DLT Schema: {len(schema.tables)} tables detected")
        if full_table_name in schema.tables:
            table_schema = schema.tables[full_table_name]
            context.log.info(f"Table schema: {len(table_schema.get('columns', {}))} columns")
    except Exception as e:
        context.log.debug(f"Could not extract schema info: {e}")

    context.log.info(f"✅ Ingested {csv_filename}: {rows_loaded:,} rows → staging.{full_table_name}")

    return {
        "filename": csv_filename,
        "rows_loaded": rows_loaded,
        "table_name": full_table_name,
        "write_disposition": write_disposition,
        "columns_count": len(df.columns),
        "status": "success"
    }


@asset(
    name="ingest_all_csvs",
    description="Universal CSV ingestion - ingests ALL CSVs found in data/csv_inbox/",
    compute_kind="dlt",
    group_name="csv_ingestion",
    metadata={
        "info": "This asset is ALWAYS available. It ingests all CSVs in the inbox folder.",
        "usage": "1. Drop CSVs in data/csv_inbox/ 2. Materialize this asset"
    }
)
def ingest_all_csvs_asset(context: AssetExecutionContext) -> Output:
    """
    Universal CSV ingestion asset.

    INGESTS ALL CSVs found in /app/data/csv_inbox/

    - No config YAML required (optional)
    - Auto-detects schema with DLT
    - Creates tables: staging.staging_<filename>
    - Always available in Dagster (permanent asset)

    Usage:
        1. Drop CSV(s) in data/csv_inbox/
        2. Materialize this asset
        3. All CSVs ingested automatically
    """
    csv_inbox = Path("/app/data/csv_inbox")

    # Find all CSVs
    csv_files = list(csv_inbox.glob("*.csv"))

    if not csv_files:
        context.log.warning("No CSV files found in inbox")
        return Output(
            value={"message": "No CSV files to ingest", "files_processed": 0},
            metadata={
                "files_processed": MetadataValue.int(0),
                "status": MetadataValue.text("No files in inbox")
            }
        )

    context.log.info(f"Found {len(csv_files)} CSV file(s) to ingest")

    # Ingest each CSV
    results = []
    for csv_file in csv_files:
        try:
            result = ingest_single_csv(csv_file, context)
            results.append(result)
        except Exception as e:
            context.log.error(f"Failed to ingest {csv_file.name}: {e}")
            results.append({
                "filename": csv_file.name,
                "error": str(e),
                "status": "failed"
            })

    # Summary
    successful = [r for r in results if "error" not in r]
    failed = [r for r in results if "error" in r]

    total_rows = sum(r.get("rows_loaded", 0) for r in successful)

    context.log.info(f"=== INGESTION COMPLETE ===")
    context.log.info(f"Total files: {len(csv_files)}")
    context.log.info(f"Successful: {len(successful)}")
    context.log.info(f"Failed: {len(failed)}")
    context.log.info(f"Total rows loaded: {total_rows:,}")

    # Format files metadata with proper handling of success/failure
    def format_result(r: Dict[str, Any]) -> str:
        """Format a single result for metadata display."""
        filename = r['filename']

        if 'error' in r:
            # Failed ingestion
            error_msg = r['error'][:50] + "..." if len(r['error']) > 50 else r['error']
            return f"- ❌ {filename}: FAILED ({error_msg})"
        else:
            # Successful ingestion - ensure rows_loaded is an integer
            rows = r.get('rows_loaded', 0)
            if not isinstance(rows, int):
                try:
                    rows = int(rows)
                except (ValueError, TypeError):
                    rows = 0
            table = r.get('table_name', 'unknown')
            return f"- ✅ {filename}: {rows:,} rows → {table}"

    files_summary = "\n".join([format_result(r) for r in results])

    return Output(
        value={
            "files_processed": len(csv_files),
            "successful": len(successful),
            "failed": len(failed),
            "total_rows_loaded": total_rows,
            "results": results
        },
        metadata={
            "files_processed": MetadataValue.int(len(csv_files)),
            "successful": MetadataValue.int(len(successful)),
            "failed": MetadataValue.int(len(failed)),
            "total_rows": MetadataValue.int(total_rows),
            "files": MetadataValue.md(files_summary)
        }
    )
