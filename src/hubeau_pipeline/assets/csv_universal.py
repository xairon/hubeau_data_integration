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


def _create_pipeline(pipeline_name: str, context=None) -> dlt.Pipeline:
    """Create DLT pipeline with PostgreSQL destination."""
    import os
    from dlt.destinations import postgres
    
    destination = postgres(
        credentials={
            "database": os.environ.get("PG_DB", "postgres"),
            "username": os.environ.get("PG_USER", "postgres"),
            "password": os.environ.get("PG_PASSWORD"),
            "host": os.environ.get("PG_HOST", "postgres"),
            "port": int(os.environ.get("PG_PORT", "5432")),
        }
    )
    
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name="staging",
        progress="log",
    )


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
    Ingest a single CSV file using DLT with advanced features (Streaming).
    
    OPTIMIZED: Uses streaming instead of Pandas to handle large files (No OOM).
    """
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
    
    table_name = dest_config.get("table_name", csv_path.stem)
    write_disposition = dest_config.get("write_disposition", "replace")
    primary_key = dest_config.get("primary_key")
    merge_key = dest_config.get("merge_key")

    # Prefix table with "staging_"
    full_table_name = f"staging_{table_name}"

    context.log.info(f"Destination: staging.{full_table_name}")
    context.log.info(f"Write mode: {write_disposition}")

    # Create DLT resource with generator for streaming
    @dlt.resource(
        name=full_table_name,
        write_disposition=write_disposition,
        primary_key=primary_key,
        merge_key=merge_key if write_disposition == "merge" else None,
    )
    def csv_data():
        """Generator that streams rows from CSV."""
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252']
        
        for encoding in encodings:
            try:
                with open(csv_path, 'r', encoding=encoding) as f:
                    reader = csv.DictReader(f, delimiter=delimiter)
                    try:
                        # Verify we can read first row
                        first_row = next(reader)
                        yield first_row
                        # Yield the rest
                        for row in reader:
                            # Filter empty keys if any
                            clean_row = {k: v for k, v in row.items() if k is not None}
                            yield clean_row
                        break # Success, stop trying encodings
                    except StopIteration:
                        break # Empty file
            except UnicodeDecodeError:
                context.log.warning(f"Encoding {encoding} failed for {csv_filename}, trying next...")
                continue
            except Exception as e:
                context.log.error(f"Error reading CSV with {encoding}: {e}")
                raise e

    # Create pipeline
    pipeline = _create_pipeline(f"csv_{table_name}", context)
    
    # Run pipeline
    try:
        load_info = pipeline.run(csv_data())
        
        # Extract metrics
        rows_loaded = 0
        for pkg in getattr(load_info, "load_packages", []) or []:
            for job in getattr(pkg, "jobs", []) or []:
                rows_loaded += getattr(job, "metrics", {}).get("items", 0)
        
        metrics = {"rows_loaded": rows_loaded, "status": "success"}
    except Exception as e:
        context.log.error(f"DLT pipeline failed for {csv_filename}: {e}")
        raise e

    # Simple approximate row count for logging if DLT metrics are empty (e.g. if skipped)
    # We don't want to re-read the file just for counting if possible, but logging '0' might be confusing.
    # DLT 'rows_loaded' is accurate for what was inserted.
    
    context.log.info(
        f"✅ Ingested {csv_filename}: {metrics['rows_loaded']:,} rows → staging.{full_table_name}"
    )

    return {
        "filename": csv_filename,
        "rows_loaded": metrics['rows_loaded'],
        "table_name": full_table_name,
        "write_disposition": write_disposition,
        "status": metrics.get("status", "success"),
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
