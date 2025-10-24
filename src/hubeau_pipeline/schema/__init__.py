"""Schema management and optimization for Hub'Eau PostgreSQL tables"""

from .schema_optimizer import SchemaOptimizer, TableOptimization, ColumnTypeInfo

__all__ = ["SchemaOptimizer", "TableOptimization", "ColumnTypeInfo"]
