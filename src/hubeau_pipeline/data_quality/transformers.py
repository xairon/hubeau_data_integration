"""
DLT Transformers for Data Quality
Functions decorated with @dlt.transformer for data cleaning and validation
"""
from typing import Iterator, Dict, Any, List, Callable
import dlt
from datetime import datetime
import logging

from hubeau_pipeline.observability.metrics import record_invalid_records

logger = logging.getLogger(__name__)


@dlt.transformer(name="normalize_dates")
def normalize_dates(items: Iterator[Dict[str, Any]], fields: List[str]) -> Iterator[Dict[str, Any]]:
    """
    Normalize date fields to ISO 8601 format

    Args:
        items: Iterator of records
        fields: List of date field names to normalize

    Yields:
        Records with normalized dates

    Usage in YAML:
        transformers:
          - normalize_dates:
              fields:
                - date_obs
                - date_debut
                - date_fin
    """
    for item in items:
        for field in fields:
            if field in item and item[field] is not None:
                try:
                    value = item[field]

                    # Parse various date formats
                    if isinstance(value, str):
                        # Try ISO format
                        if 'T' in value or '-' in value:
                            date_obj = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        else:
                            # Try other common formats
                            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']:
                                try:
                                    date_obj = datetime.strptime(value, fmt)
                                    break
                                except ValueError:
                                    continue
                            else:
                                raise ValueError(f"Cannot parse date format: {value}")

                    elif isinstance(value, (int, float)):
                        # Unix timestamp (milliseconds)
                        date_obj = datetime.fromtimestamp(value / 1000)

                    elif isinstance(value, datetime):
                        date_obj = value

                    else:
                        logger.warning(f"Unsupported date type for field {field}: {type(value)}")
                        yield item
                        continue

                    # Normalize to ISO 8601 string
                    item[field] = date_obj.isoformat()

                except Exception as e:
                    logger.warning(f"Failed to normalize date field {field}: {e}")
                    # Keep original value
                    pass

        yield item


@dlt.transformer(name="clean_numeric_fields")
def clean_numeric_fields(
    items: Iterator[Dict[str, Any]],
    fields: List[str],
    replace_invalid_with_null: bool = True
) -> Iterator[Dict[str, Any]]:
    """
    Clean numeric fields by removing invalid values

    Args:
        items: Iterator of records
        fields: List of numeric field names to clean
        replace_invalid_with_null: If True, replace invalid values with null

    Yields:
        Records with cleaned numeric fields

    Usage in YAML:
        transformers:
          - clean_numeric_fields:
              fields:
                - resultat
                - niveau_nappe_ngf
              replace_invalid_with_null: true
    """
    for item in items:
        for field in fields:
            if field in item and item[field] is not None:
                try:
                    value = item[field]

                    # Try to convert to float
                    if isinstance(value, str):
                        # Remove spaces, replace comma with dot
                        value = value.strip().replace(',', '.').replace(' ', '')

                        # Handle special values
                        if value.lower() in ['nan', 'null', 'none', '', '-']:
                            if replace_invalid_with_null:
                                item[field] = None
                            continue

                    # Convert to float
                    item[field] = float(value)

                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid numeric value for field {field}: {value} ({e})")
                    if replace_invalid_with_null:
                        item[field] = None

        yield item


@dlt.transformer(name="validate_and_flag_records")
def validate_and_flag_records(
    items: Iterator[Dict[str, Any]],
    validator_func: Callable[[Dict[str, Any]], tuple[bool, List[str]]],
    drop_invalid: bool = False,
    add_validation_flag: bool = True
) -> Iterator[Dict[str, Any]]:
    """
    Validate records using a custom validator function and flag/drop invalid ones

    Args:
        items: Iterator of records
        validator_func: Function that takes a record and returns (is_valid, errors_list)
        drop_invalid: If True, drop invalid records. If False, flag them
        add_validation_flag: If True, add _is_valid and _validation_errors fields

    Yields:
        Validated records (possibly with validation flags)

    Usage:
        from hubeau_pipeline.data_quality.validators import validate_piezometry_record

        @dlt.resource
        def piezometry_data():
            data = extract_data()
            yield from data | validate_and_flag_records(
                validator_func=validate_piezometry_record,
                drop_invalid=False
            )
    """
    for item in items:
        is_valid, errors = validator_func(item)

        if not is_valid:
            # Record metrics
            source = item.get('_source', 'unknown')
            resource = item.get('_resource', 'unknown')

            for error in errors:
                record_invalid_records(
                    source=source,
                    resource=resource,
                    validation_rule=error.split(':')[0],  # Extract rule name
                    count=1
                )

            logger.warning(f"Invalid record: {errors}")

            if drop_invalid:
                # Skip this record
                continue

        if add_validation_flag:
            item['_is_valid'] = is_valid
            item['_validation_errors'] = errors if not is_valid else []

        yield item


@dlt.transformer(name="deduplicate_records")
def deduplicate_records(
    items: Iterator[Dict[str, Any]],
    keys: List[str]
) -> Iterator[Dict[str, Any]]:
    """
    Deduplicate records based on composite key

    Args:
        items: Iterator of records
        keys: List of fields that form the composite key

    Yields:
        Deduplicated records (keeps first occurrence)

    Usage in YAML:
        transformers:
          - deduplicate_records:
              keys:
                - code_station
                - date_obs
    """
    seen_keys = set()

    for item in items:
        # Create composite key
        key_values = tuple(item.get(k) for k in keys)

        if key_values not in seen_keys:
            seen_keys.add(key_values)
            yield item
        else:
            logger.debug(f"Duplicate record skipped: {dict(zip(keys, key_values))}")


@dlt.transformer(name="enrich_with_metadata")
def enrich_with_metadata(
    items: Iterator[Dict[str, Any]],
    source_name: str,
    resource_name: str,
    partition: str = ""
) -> Iterator[Dict[str, Any]]:
    """
    Enrich records with pipeline metadata

    Args:
        items: Iterator of records
        source_name: Name of the DLT source
        resource_name: Name of the DLT resource
        partition: Partition identifier

    Yields:
        Records enriched with metadata fields

    Usage:
        @dlt.resource
        def my_data():
            data = extract_data()
            yield from data | enrich_with_metadata(
                source_name="piezometry",
                resource_name="chroniques",
                partition="2024"
            )
    """
    ingestion_timestamp = datetime.now().isoformat()

    for item in items:
        item['_ingestion_timestamp'] = ingestion_timestamp
        item['_source'] = source_name
        item['_resource'] = resource_name
        if partition:
            item['_partition'] = partition

        yield item


@dlt.transformer(name="filter_by_date_range")
def filter_by_date_range(
    items: Iterator[Dict[str, Any]],
    date_field: str,
    start_date: str,
    end_date: str
) -> Iterator[Dict[str, Any]]:
    """
    Filter records by date range

    Args:
        items: Iterator of records
        date_field: Name of the date field to filter on
        start_date: Start date (ISO format)
        end_date: End date (ISO format)

    Yields:
        Records within the date range

    Usage in YAML:
        transformers:
          - filter_by_date_range:
              date_field: date_obs
              start_date: "2024-01-01"
              end_date: "2024-12-31"
    """
    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)

    for item in items:
        if date_field in item and item[date_field] is not None:
            try:
                record_date = datetime.fromisoformat(str(item[date_field]).replace('Z', '+00:00'))

                if start <= record_date <= end:
                    yield item
                else:
                    logger.debug(f"Record filtered out (date {record_date} not in range [{start}, {end}])")

            except (ValueError, TypeError) as e:
                logger.warning(f"Cannot parse date field {date_field}: {e}")
                # Include record if date cannot be parsed (let downstream validation handle it)
                yield item
        else:
            # Include records without the date field (let downstream validation handle it)
            yield item


# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS FOR TRANSFORMER COMPOSITION
# ═══════════════════════════════════════════════════════════

def apply_standard_quality_checks(
    items: Iterator[Dict[str, Any]],
    source_name: str,
    resource_name: str,
    date_fields: List[str] = None,
    numeric_fields: List[str] = None,
    validator_func: Callable = None,
    partition: str = ""
) -> Iterator[Dict[str, Any]]:
    """
    Apply standard data quality transformations pipeline

    This is a convenience function that chains multiple transformers:
    1. Normalize dates
    2. Clean numeric fields
    3. Validate records
    4. Enrich with metadata

    Args:
        items: Iterator of records
        source_name: DLT source name
        resource_name: DLT resource name
        date_fields: List of date fields to normalize
        numeric_fields: List of numeric fields to clean
        validator_func: Custom validator function
        partition: Partition identifier

    Yields:
        Cleaned and validated records

    Usage:
        @dlt.resource
        def my_data():
            raw_data = extract_data()
            yield from apply_standard_quality_checks(
                raw_data,
                source_name="piezometry",
                resource_name="chroniques",
                date_fields=["date_obs"],
                numeric_fields=["resultat"],
                validator_func=validate_piezometry_record,
                partition="2024"
            )
    """
    pipeline = items

    # Step 1: Normalize dates
    if date_fields:
        pipeline = normalize_dates(pipeline, fields=date_fields)

    # Step 2: Clean numeric fields
    if numeric_fields:
        pipeline = clean_numeric_fields(pipeline, fields=numeric_fields)

    # Step 3: Validate records
    if validator_func:
        pipeline = validate_and_flag_records(
            pipeline,
            validator_func=validator_func,
            drop_invalid=False,  # Keep invalid records but flag them
            add_validation_flag=True
        )

    # Step 4: Enrich with metadata
    pipeline = enrich_with_metadata(
        pipeline,
        source_name=source_name,
        resource_name=resource_name,
        partition=partition
    )

    yield from pipeline
