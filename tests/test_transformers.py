"""Tests pour les transformers DLT."""
import pytest
from dlt_pipeline.transformers import validate_primary_keys, validate_coordinates


def test_validate_primary_keys_valid():
    """Test validation avec primary keys valides."""
    items = [
        {"code_station": "ABC", "value": 10},
        {"code_station": "XYZ", "value": 20},
    ]
    
    validated = list(validate_primary_keys(
        iter(items),
        primary_keys=["code_station"],
        api_name="test"
    ))
    
    assert len(validated) == 2


def test_validate_primary_keys_rejects_null():
    """Test rejet des primary keys NULL."""
    items = [
        {"code_station": None, "value": 10},  # Rejete
        {"code_station": "XYZ", "value": 20},  # Garde
    ]
    
    validated = list(validate_primary_keys(
        iter(items),
        primary_keys=["code_station"],
        api_name="test"
    ))
    
    assert len(validated) == 1
    assert validated[0]["code_station"] == "XYZ"


def test_validate_coordinates():
    """Test validation coordonnees."""
    items = [
        {"latitude": 48.8566, "longitude": 2.3522},  # Paris (valide)
        {"latitude": 91.0, "longitude": 2.0},  # Invalide
    ]
    
    validated = list(validate_coordinates(iter(items)))
    
    assert validated[0]["latitude"] == 48.8566
    assert validated[1]["latitude"] is None  # Nettoye
