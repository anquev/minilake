"""Parquet file ingestion strategy."""
from pathlib import Path
from typing import Optional, Dict, Any
import duckdb

from minilake.ingestion.base import IngestionStrategy
from minilake.core.exceptions import IngestionError

class ParquetIngestion(IngestionStrategy):
    """Ingesting Parquet files."""

    def ingest(self, conn: duckdb.DuckDBPyConnection, file_path: Path, 
              table_name: str, schema: Optional[Dict[str, str]] = None,
              batch_size: Optional[int] = None, **kwargs: Any) -> None:
        """Ingest a Parquet file into DuckDB.

        Args:
            conn: DuckDB connection
            file_path: Path to the Parquet file
            table_name: Name of the DuckDB table to create
            schema: Not used for Parquet (schema is derived from file)
            batch_size: Not used for Parquet
            kwargs: Additional parameters (not used)
        """
        try:
            query = f"""
            CREATE TABLE "{table_name}" AS
            SELECT * FROM read_parquet(?)
            """
            conn.execute(query, [str(file_path)])
        except Exception as e:
            raise IngestionError(f"Error ingesting Parquet file: {str(e)}") from e
