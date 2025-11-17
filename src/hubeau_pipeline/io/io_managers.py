"""
Custom IO Managers for Hub'Eau Pipeline

This module provides custom IO managers for specific use cases where
the default filesystem IO manager is not appropriate.
"""
from dagster import IOManager, io_manager
from typing import Any, Optional


class NoOpIOManager(IOManager):
    """
    IO Manager that does nothing - for assets that don't need output persistence.

    Use case: DLT assets that write directly to PostgreSQL and only return
    metadata dictionaries for logging/monitoring. The actual data is already
    in PostgreSQL via DLT, so there's no need to persist the output.

    Benefits:
    - Avoids filesystem storage conflicts (IsADirectoryError)
    - No unnecessary I/O operations
    - Clear separation: data in PostgreSQL, metadata in logs
    - Allows assets to return metadata for downstream context

    Example:
        @asset(io_manager_key="noop_io_manager")
        def my_asset(context):
            # Write data to PostgreSQL via DLT
            load_info = pipeline.run(source)

            # Return metadata for logging (not persisted)
            return {"rows_loaded": 1234, "table": "my_table"}
    """

    def handle_output(self, context, obj: Any) -> None:
        """
        Accept any output but don't persist it.

        The metadata is already logged via context.log in the asset,
        and the actual data is in PostgreSQL via DLT.

        Args:
            context: Output context from Dagster
            obj: The object to handle (typically a metadata dict)
        """
        # Explicitly do nothing - data is already in PostgreSQL via DLT
        # Metadata is already logged via context.log in the asset
        pass

    def load_input(self, context) -> Optional[Any]:
        """
        Should never be called since outputs aren't used downstream.

        DLT assets write directly to PostgreSQL and don't chain outputs.

        Args:
            context: Input context from Dagster

        Raises:
            NotImplementedError: Always, as this operation is not supported
        """
        raise NotImplementedError(
            "NoOpIOManager does not support loading inputs. "
            "DLT assets write directly to PostgreSQL and don't chain outputs. "
            "If you need to use asset outputs downstream, consider using a "
            "different IO manager or querying PostgreSQL directly."
        )


@io_manager
def noop_io_manager():
    """
    Factory function for NoOpIOManager.

    This is registered as a resource in Dagster definitions and can be
    referenced by assets using io_manager_key="noop_io_manager".

    Returns:
        NoOpIOManager: An instance of the no-op IO manager
    """
    return NoOpIOManager()
