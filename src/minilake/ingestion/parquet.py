"""Parquet file ingestion strategy."""

from pathlib import Path
from typing import Any

import duckdb

from minilake.core.exceptions import IngestionError
from minilake.ingestion.base import IngestionStrategy


class ParquetIngestion(IngestionStrategy):
    """Ingesting Parquet files."""

    def ingest(
        self,
        conn: duckdb.DuckDBPyConnection,
        file_path: Path,
        table_name: str,
        schema: dict[str, str] | None = None,
        batch_size: int | None = None,
        **kwargs: Any,
    ) -> None:
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
            raise IngestionError(f"Error ingesting Parquet file: {e!s}") from e
