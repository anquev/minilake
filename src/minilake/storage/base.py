"""Base interfaces for storage implementations."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pyarrow as pa


class StorageInterface(ABC):
    """Interface for storage operations."""

    @abstractmethod
    def create_table(
        self,
        table_name: str,
        delta_path: str,
        partition_by: list[str] | None = None,
        schema: pa.Schema | None = None,
        mode: str = "overwrite",
    ) -> None:
        """Create a Delta table from a DuckDB table.

        Args:
            table_name: Source DuckDB table name
            delta_path: Path where the Delta table will be stored
            partition_by: Optional columns to partition by
            schema: Optional PyArrow schema to enforce
            mode: Write mode ("overwrite" or "append")
        """
        pass

    @abstractmethod
    def read_to_duckdb(
        self,
        delta_path: str,
        table_name: str,
        version: int | None = None,
        timestamp: str | datetime | None = None,
    ) -> None:
        """Read a Delta table into DuckDB.

        Args:
            delta_path: Path to the Delta table
            table_name: Name of the DuckDB table to create
            version: Optional specific version to load
            timestamp: Optional timestamp to load data as of
        """
        pass

    @abstractmethod
    def get_table_info(self, delta_path: str) -> dict[str, Any]:
        """Get information about a Delta table.

        Args:
            delta_path: Path to the Delta table

        Returns:
            Dictionary containing table metadata
        """
        pass

    @abstractmethod
    def vacuum(self, delta_path: str, retention: int | None = 168) -> None:
        """Clean up old versions of a Delta table.

        Args:
            delta_path: Path to the Delta table
            retention: Retention period in hours (minimum 168 hours/7 days)
        """
        pass

    @abstractmethod
    def optimize(self, delta_path: str, zorder_by: list[str] | None = None) -> None:
        """Optimize a Delta table.

        Args:
            delta_path: Path to the Delta table
            zorder_by: Optional columns to z-order by
        """
        pass
