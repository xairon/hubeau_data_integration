"""Modeles Pydantic pour validation Hub'Eau."""
from datetime import datetime
from typing import Dict, List, Optional
from pydantic import BaseModel, Field, validator


class StationMonthData(BaseModel):
    """Donnees de stations actives par mois."""
    stations: Dict[str, List[str]] = Field(..., description="Mapping station_code -> liste de mois actifs")
    total_stations: int = Field(..., ge=0)
    total_station_months: int = Field(..., ge=0)
    
    @validator('total_station_months')
    def validate_totals(cls, v, values):
        if 'stations' in values:
            calculated = sum(len(months) for months in values['stations'].values())
            if v != calculated:
                raise ValueError(f"Incoherence: total_station_months={v} != calcule={calculated}")
        return v


class APIResponse(BaseModel):
    """Reponse standardisee des APIs Hub'Eau."""
    data: List[Dict] = Field(default_factory=list)
    count: Optional[int] = None
    next: Optional[str] = None
