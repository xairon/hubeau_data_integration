"""
Data Quality Validators for Hub'Eau Pipeline
Schema contracts et validation rules pour DLT
"""
from typing import Dict, List, Any, Optional
import dlt
from dlt.common.schema.typing import TTableSchema
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def create_schema_contract(
    mode: str = "evolve",
    freeze_columns: bool = False,
    freeze_tables: bool = False
) -> Dict[str, str]:
    """
    Create DLT schema contract configuration

    Args:
        mode: "evolve" (accept new columns/tables) or "freeze" (reject changes)
        freeze_columns: If True, reject new columns in existing tables
        freeze_tables: If True, reject new tables

    Returns:
        Schema contract dict for DLT source

    Usage:
        @dlt.source(schema_contract=create_schema_contract(freeze_columns=True))
        def hubeau_source():
            ...
    """
    if mode == "freeze":
        return {
            "tables": "freeze",
            "columns": "freeze",
            "data_type": "freeze"
        }
    elif mode == "strict":
        return {
            "tables": "freeze" if freeze_tables else "evolve",
            "columns": "freeze" if freeze_columns else "evolve",
            "data_type": "freeze"  # Toujours freeze les types de données
        }
    else:  # evolve
        return {
            "tables": "evolve",
            "columns": "evolve",
            "data_type": "discard_value"  # Accepter les valeurs mais logger warning
        }


def validate_primary_keys(record: Dict[str, Any], primary_keys: List[str]) -> tuple[bool, Optional[str]]:
    """
    Validate that all primary key fields are present and non-null

    Args:
        record: Data record to validate
        primary_keys: List of primary key field names

    Returns:
        (is_valid, error_message)
    """
    for key in primary_keys:
        if key not in record:
            return False, f"Missing primary key field: {key}"

        value = record[key]
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return False, f"Primary key '{key}' is null or empty"

    return True, None


def validate_coordinates(
    record: Dict[str, Any],
    lat_field: str = "latitude",
    lon_field: str = "longitude",
    allow_null: bool = True
) -> tuple[bool, Optional[str]]:
    """
    Validate geographic coordinates

    Args:
        record: Data record to validate
        lat_field: Name of latitude field
        lon_field: Name of longitude field
        allow_null: If True, allow null coordinates

    Returns:
        (is_valid, error_message)
    """
    lat = record.get(lat_field)
    lon = record.get(lon_field)

    # Check if null
    if lat is None or lon is None:
        if allow_null:
            return True, None
        else:
            return False, f"Coordinates are null (lat={lat}, lon={lon})"

    # Validate ranges
    try:
        lat_float = float(lat)
        lon_float = float(lon)

        if not (-90 <= lat_float <= 90):
            return False, f"Latitude {lat_float} out of range [-90, 90]"

        if not (-180 <= lon_float <= 180):
            return False, f"Longitude {lon_float} out of range [-180, 180]"

        # France métropolitaine bounds (optionnel, strict)
        # if not (41 <= lat_float <= 51) or not (-5 <= lon_float <= 10):
        #     return False, f"Coordinates ({lat_float}, {lon_float}) outside France bounds"

        return True, None

    except (ValueError, TypeError) as e:
        return False, f"Invalid coordinate format: {str(e)}"


def validate_date_range(
    record: Dict[str, Any],
    date_field: str,
    min_date: Optional[datetime] = None,
    max_date: Optional[datetime] = None,
    allow_null: bool = True
) -> tuple[bool, Optional[str]]:
    """
    Validate that a date field is within acceptable range

    Args:
        record: Data record to validate
        date_field: Name of date field
        min_date: Minimum acceptable date (default: 1900-01-01)
        max_date: Maximum acceptable date (default: today + 1 year)
        allow_null: If True, allow null dates

    Returns:
        (is_valid, error_message)
    """
    value = record.get(date_field)

    if value is None:
        if allow_null:
            return True, None
        else:
            return False, f"Date field '{date_field}' is null"

    # Parse date
    try:
        if isinstance(value, str):
            # Try ISO format
            date_obj = datetime.fromisoformat(value.replace('Z', '+00:00'))
        elif isinstance(value, (int, float)):
            # Unix timestamp (milliseconds)
            date_obj = datetime.fromtimestamp(value / 1000)
        elif isinstance(value, datetime):
            date_obj = value
        else:
            return False, f"Unsupported date format: {type(value)}"

        # Default ranges
        if min_date is None:
            min_date = datetime(1900, 1, 1)
        if max_date is None:
            max_date = datetime.now().replace(year=datetime.now().year + 1)

        # Validate range
        if date_obj < min_date:
            return False, f"Date {date_obj} before minimum {min_date}"
        if date_obj > max_date:
            return False, f"Date {date_obj} after maximum {max_date}"

        return True, None

    except (ValueError, TypeError) as e:
        return False, f"Invalid date format: {str(e)}"


def validate_numeric_range(
    record: Dict[str, Any],
    field: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    allow_null: bool = True
) -> tuple[bool, Optional[str]]:
    """
    Validate that a numeric field is within acceptable range

    Args:
        record: Data record to validate
        field: Name of numeric field
        min_value: Minimum acceptable value
        max_value: Maximum acceptable value
        allow_null: If True, allow null values

    Returns:
        (is_valid, error_message)
    """
    value = record.get(field)

    if value is None:
        if allow_null:
            return True, None
        else:
            return False, f"Numeric field '{field}' is null"

    # Validate type and range
    try:
        numeric_value = float(value)

        if min_value is not None and numeric_value < min_value:
            return False, f"Value {numeric_value} below minimum {min_value}"

        if max_value is not None and numeric_value > max_value:
            return False, f"Value {numeric_value} above maximum {max_value}"

        return True, None

    except (ValueError, TypeError) as e:
        return False, f"Invalid numeric format: {str(e)}"


def validate_not_null(record: Dict[str, Any], fields: List[str]) -> tuple[bool, Optional[str]]:
    """
    Validate that specified fields are not null

    Args:
        record: Data record to validate
        fields: List of field names that must not be null

    Returns:
        (is_valid, error_message)
    """
    for field in fields:
        if field not in record:
            return False, f"Required field '{field}' is missing"

        value = record[field]
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return False, f"Required field '{field}' is null or empty"

    return True, None


# ═══════════════════════════════════════════════════════════
# COMPOSITE VALIDATORS (combines multiple checks)
# ═══════════════════════════════════════════════════════════

def validate_piezometry_record(record: Dict[str, Any]) -> tuple[bool, List[str]]:
    """
    Validate a piezometry record (composite validator)

    Returns:
        (is_valid, list_of_errors)
    """
    errors = []

    # Primary keys
    is_valid, error = validate_primary_keys(record, ['code_bss', 'timestamp_mesure'])
    if not is_valid:
        errors.append(error)

    # Coordinates
    is_valid, error = validate_coordinates(record, allow_null=False)
    if not is_valid:
        errors.append(error)

    # Date range
    is_valid, error = validate_date_range(
        record,
        'timestamp_mesure',
        min_date=datetime(2000, 1, 1),
        allow_null=False
    )
    if not is_valid:
        errors.append(error)

    # Numeric range (piezometric level in meters)
    if 'niveau_nappe_ngf' in record:
        is_valid, error = validate_numeric_range(
            record,
            'niveau_nappe_ngf',
            min_value=-100,  # Below sea level
            max_value=3000   # Max altitude in France
        )
        if not is_valid:
            errors.append(error)

    return len(errors) == 0, errors


def validate_hydrometry_record(record: Dict[str, Any]) -> tuple[bool, List[str]]:
    """
    Validate a hydrometry record (composite validator)

    Returns:
        (is_valid, list_of_errors)
    """
    errors = []

    # Primary keys
    is_valid, error = validate_primary_keys(record, ['code_station', 'date_obs'])
    if not is_valid:
        errors.append(error)

    # Date range
    is_valid, error = validate_date_range(
        record,
        'date_obs',
        min_date=datetime(1900, 1, 1),
        allow_null=False
    )
    if not is_valid:
        errors.append(error)

    # Flow rate (débit in m³/s)
    if 'resultat_obs' in record:
        is_valid, error = validate_numeric_range(
            record,
            'resultat_obs',
            min_value=0,      # Positive flow
            max_value=10000   # Max realistic flow for French rivers
        )
        if not is_valid:
            errors.append(error)

    return len(errors) == 0, errors


def validate_quality_analysis_record(record: Dict[str, Any]) -> tuple[bool, List[str]]:
    """
    Validate a water quality analysis record (composite validator)

    Returns:
        (is_valid, list_of_errors)
    """
    errors = []

    # Primary keys
    is_valid, error = validate_primary_keys(
        record,
        ['code_station', 'date_prelevement', 'code_parametre']
    )
    if not is_valid:
        errors.append(error)

    # Date range
    is_valid, error = validate_date_range(
        record,
        'date_prelevement',
        min_date=datetime(1990, 1, 1),
        allow_null=False
    )
    if not is_valid:
        errors.append(error)

    # Result value (depends on parameter, accept all for now)
    if 'resultat' in record and record['resultat'] is not None:
        is_valid, error = validate_numeric_range(
            record,
            'resultat',
            min_value=0,  # Most parameters are positive
            max_value=None  # No max (varies by parameter)
        )
        if not is_valid:
            errors.append(error)

    return len(errors) == 0, errors
