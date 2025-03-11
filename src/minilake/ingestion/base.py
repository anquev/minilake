"""Base interfaces for data ingestion."""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Dict, Any
import duckdb

class IngestionStrategy(ABC):
    """Interface for file ingestion strategies."""

    @abstractmethod
    def ingest(self, conn: duckdb.DuckDBPyConnection, file_path: Path, 
              table_name: str, schema: Optional[Dict[str, str]] = None,
              batch_size: Optional[int] = None, **kwargs: Any) -> None:
        """Ingest data from a file into a DuckDB table.

        Args:
            conn: DuckDB connection
            file_path: Path to the file to ingest
            table_name: Name of the DuckDB table to create
            schema: Optional schema definition
            batch_size: Optional batch size for large files
            kwargs: Additional format-specific parameters
        """
        pass
