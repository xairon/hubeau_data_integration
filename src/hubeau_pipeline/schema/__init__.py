"""Schema management - Type mappings from Swagger/OpenAPI docs"""

from .hubeau_type_mappings import (
    get_field_type,
    get_table_schema,
    get_primary_key,
    get_foreign_keys,
    has_geometry,
    get_geometry_columns,
    get_all_tables,
    HUBEAU_FIELD_TYPES,
    TABLE_PK_MAPPING,
    TABLE_FK_MAPPING,
    GEOMETRY_COLUMNS,
    COLUMN_TYPE_OVERRIDES,
)

__all__ = [
    "get_field_type",
    "get_table_schema",
    "get_primary_key",
    "get_foreign_keys",
    "has_geometry",
    "get_geometry_columns",
    "get_all_tables",
    "HUBEAU_FIELD_TYPES",
    "TABLE_PK_MAPPING",
    "TABLE_FK_MAPPING",
    "GEOMETRY_COLUMNS",
    "COLUMN_TYPE_OVERRIDES",
]

