"""YAML configuration validation for the generic dlt pipeline."""
from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, Field, ValidationError, field_validator, ConfigDict


class PaginationConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str = Field(default="page")
    page_param: str = Field(default="page")
    page_size_param: str = Field(default="size")
    page_size: int = Field(default=500, ge=1)
    until_expr: str | None = None


class SlicerConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    mode: str
    param: str | None = None
    values: list[str] | None = None
    reference: Dict[str, Any] | None = None
    stations: list[str] | None = None
    campaigns: list[Any] | None = None
    start_param: str | None = None
    end_param: str | None = None
    window_days: int | None = Field(default=None, ge=1)
    start_date: str | None = None
    end_offset_days: int | None = Field(default=1, ge=0)
    # Paramètres pour le mode dept_datetime
    dept_param: str | None = None
    dept_chunk_size: int | None = Field(default=None, ge=1)
    dept_list: list[str] | None = None

    @field_validator("mode")
    def validate_mode(cls, value: str) -> str:
        allowed = {"datetime", "dept", "dept_datetime", "station_month", "campaign", "global"}
        if value not in allowed:
            raise ValueError(f"Unsupported slicer mode: {value}")
        return value


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    source: str
    base_url: str
    path: str
    method: str = Field(default="GET")
    params_default: Dict[str, Any] | None = None
    records_path: str | None = None
    primary_keys: list[str]
    replication_key: str | None = None
    pagination: PaginationConfig | None = None
    slicer: SlicerConfig | None = None
    fallbacks: Dict[str, Any] | None = None
    rate_limit: Dict[str, Any] | None = None
    pre_scan: Dict[str, Any] | None = None
    dataset_name: str | None = None
    file_format: str | None = None
    layout: str | None = None
    state_store: str | None = None


def validate_config(cfg: Dict[str, Any]) -> ConfigModel:
    try:
        return ConfigModel.model_validate(cfg)
    except ValidationError as exc:
        raise ValueError(f"Invalid configuration: {exc}")
