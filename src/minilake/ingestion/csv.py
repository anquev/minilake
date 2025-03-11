"""CSV file ingestion strategy."""
from pathlib import Path
from typing import Optional, Dict, Any
import duckdb

from minilake.ingestion.base import IngestionStrategy
from minilake.core.exceptions import IngestionError

class CsvIngestion(IngestionStrategy):
    """Ingesting CSV files."""

    def ingest(self, conn: duckdb.DuckDBPyConnection, file_path: Path, 
              table_name: str, schema: Optional[Dict[str, str]] = None,
              batch_size: Optional[int] = None, **kwargs: Any) -> None:
        """Ingest a CSV file into DuckDB.

        Args:
            conn: DuckDB connection
            file_path: Path to the CSV file
            table_name: Name of the DuckDB table to create
            schema: Optional schema definition
            batch_size: Not used for CSV
            kwargs: Additional parameters like delimiter, header, etc.
        """
        try:
            if schema:
                schema_sql = self._create_schema(schema)
                conn.execute(f'CREATE TABLE "{table_name}" {schema_sql}')
                conn.execute(f"""
                    INSERT INTO "{table_name}"
                    SELECT * FROM read_csv_auto(?)
                """, [str(file_path)])
            else:
                # Auto-detect schema
                conn.execute(f"""
                    CREATE TABLE "{table_name}" AS
                    SELECT * FROM read_csv_auto(?)
                """, [str(file_path)])
        except Exception as e:
            raise IngestionError(f"Error ingesting CSV file: {str(e)}") from e

    @staticmethod
    def _create_schema(schema: Dict[str, str]) -> str:
        """Create a SQL schema string from a dictionary.

        Args:
            schema: Dictionary mapping column names to types

        Returns:
            SQL schema string
        """
        columns = [f'"{col}" {dtype}' for col, dtype in schema.items()]
        return f"({', '.join(columns)})"
