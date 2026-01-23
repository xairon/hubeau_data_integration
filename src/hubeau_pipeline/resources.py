"""
Dagster resources for external dependencies used by the Hub'Eau pipeline.

Uses modern ConfigurableResource pattern (Dagster 1.3+) for:
- Type safety with Pydantic
- Lazy initialization (no crashes at import time)
- Automatic lifecycle management
- Better IDE support
"""

import logging
import os
from contextlib import contextmanager
from typing import Optional

import psycopg2
from dagster import ConfigurableResource
from pydantic import Field, PrivateAttr


logger = logging.getLogger(__name__)


class PostgreSQLResource(ConfigurableResource):
    """
    PostgreSQL connection resource with lazy initialization.

    Environment variables:
    - PG_HOST: PostgreSQL host (default: postgres)
    - PG_PORT: PostgreSQL port (default: 5432)
    - PG_DB: Database name (default: postgres)
    - PG_USER: Username (default: postgres)
    - PG_PASSWORD: Password (REQUIRED in production)

    Example usage:
        ```python
        @asset
        def my_asset(context: AssetExecutionContext, pg: PostgreSQLResource):
            with pg.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
        ```
    """

    # Public configuration fields (can be overridden)
    host: str = Field(
        default_factory=lambda: os.getenv("PG_HOST", "postgres"),
        description="PostgreSQL host"
    )
    port: int = Field(
        default_factory=lambda: int(os.getenv("PG_PORT", "5432")),
        description="PostgreSQL port"
    )
    database: str = Field(
        default_factory=lambda: os.getenv("PG_DB", "postgres"),
        description="Database name"
    )
    user: str = Field(
        default_factory=lambda: os.getenv("PG_USER", "postgres"),
        description="PostgreSQL user"
    )
    password: Optional[str] = Field(
        default_factory=lambda: os.getenv("PG_PASSWORD"),
        description="PostgreSQL password (loaded from environment)"
    )

    # Private connection pool
    _connection = PrivateAttr(default=None)

    def _validate_password(self):
        """Validate that password is set (fail-fast in production)"""
        if not self.password:
            error_msg = (
                "❌ CRITICAL: PG_PASSWORD not set!\n"
                "This MUST be defined in environment variables (GitLab CI/CD Variables)."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

    @contextmanager
    def get_connection(self):
        """
        Get a PostgreSQL connection (context manager for automatic cleanup).

        Yields:
            psycopg2.connection: PostgreSQL connection

        Raises:
            ValueError: If PG_PASSWORD is not set
        """
        self._validate_password()

        conn = None
        try:
            logger.info(f"🔗 Connecting to PostgreSQL: {self.user}@{self.host}:{self.port}/{self.database}")

            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                sslmode=os.getenv('PG_SSLMODE', 'prefer')  # prefer=SSL if available
            )

            yield conn

        finally:
            if conn:
                conn.close()
                logger.debug("✅ PostgreSQL connection closed")

    def get_dsn(self) -> str:
        """
        Get PostgreSQL DSN string (for libraries that need it).

        Returns:
            str: PostgreSQL DSN in format postgresql://user:password@host:port/database
        """
        self._validate_password()
        logger.info(f"🔗 PostgreSQL DSN: postgresql://{self.user}:***@{self.host}:{self.port}/{self.database}")
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"



# ============================================================================
# RESOURCE DEFINITIONS
# ============================================================================
# Modern Dagster resources using ConfigurableResource
# These are instantiated lazily - NO code execution at import time!
# ============================================================================

from dagster_dlt import DagsterDltResource

RESOURCES = {
    "pg": PostgreSQLResource(),
    "dlt": DagsterDltResource(),  # Official dagster-dlt integration
}

