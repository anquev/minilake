# src/minilake/services/file_ingestion.py
"""
Module to ingest data from various file formats into a DuckDB database.
"""
from pathlib import Path
from typing import Union, Optional
import duckdb
import pandas as pd
import polars as pl
from minilake.db_conn import DBConnection

class DataIngestion:
    """
    Class to ingest data from various file formats into a DuckDB database.
    Accepted formats are:
    - CSV
    - Parquet
    - JSON
    - Excel
    """
    def __init__(self, conn: Optional[duckdb.DuckDBPyConnection] = None):
        """Initialize the DataIngestion class."""
        self.conn = conn or DBConnection.get_connection()

    def ingest_file(
            self,
            file_path: Union[str, Path],
            table_name: str,
            schema: Optional[dict] = None,
            batch_size: Optional[int] = 100000
    ) -> None:
        """
        Ingest data from a file into the database.

        Inputs:
        - file_path: str or Path
            Path to the file to ingest.
        - table_name: str
            Table name in the database.
        - schema: dict, default None
            Table schema. If None, schema will be inferred from the file.
        - batch_size: int, default 100000
            Batch size to ingest data.
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError("File not found, please provide a valid file path.")

        extension = file_path.suffix.lower()

        try:
            if extension == '.parquet':
                self._ingest_parquet(file_path, table_name)
            elif extension == '.json':
                self._ingest_json(file_path, table_name, schema)
            elif extension in ['.csv', '.txt']:
                self._ingest_csv(file_path, table_name, schema)
            elif extension in ['.xls', '.xlsx']:
                self._ingest_excel(file_path, table_name, schema, batch_size)
            else:
                raise ValueError(f"File format not handled yet {extension}")
        except Exception as e:
            raise RuntimeError(f"Error during file ingestion: {str(e)}") from e

    def _ingest_parquet(self, file_path: Path, table_name: str) -> None:
        query = f"""
        CREATE TABLE "{table_name}" AS
        SELECT * FROM read_parquet(?)
        """
        self.conn.execute(query, [str(file_path)])

    def _ingest_csv(self, file_path: Path, table_name: str, schema: Optional[dict]) -> None:
        if schema:
            schema_sql = self._create_schema(schema)
            self.conn.execute(f'CREATE TABLE "{table_name}" {schema_sql}')
            self.conn.execute(f"""
                INSERT INTO "{table_name}"
                SELECT * FROM read_csv_auto(?)
            """, [str(file_path)])
        else:
            self.conn.execute(f"""
                CREATE TABLE "{table_name}" AS
                SELECT * FROM read_csv_auto(?)
            """, [str(file_path)])

    def _ingest_json(self, file_path: Path, table_name: str, schema: Optional[dict]) -> None:
        if schema:
            schema_sql = self._create_schema(schema)
            self.conn.execute(f'CREATE TABLE "{table_name}" {schema_sql}')
            self.conn.execute(f"""
                INSERT INTO "{table_name}"
                SELECT * FROM read_json(?)
            """, [str(file_path)])
        else:
            self.conn.execute(f"""
                CREATE TABLE "{table_name}" AS
                SELECT * FROM read_json(?)
            """, [str(file_path)])

    def _ingest_excel(
        self,
        file_path: Path,
        table_name: str,
        schema: Optional[dict],
        batch_size: Optional[int] = None
    ) -> None:
        df = pd.read_excel(file_path)
        self._ingest_dataframe(df, table_name, schema, batch_size)

    def _ingest_dataframe(
        self,
        df: Union[pd.DataFrame, pl.DataFrame],
        table_name: str,
        schema: Optional[dict],
        batch_size: Optional[int] = None
    ) -> None:
        if schema:
            schema_sql = self._create_schema(schema)
            self.conn.execute(f'CREATE TABLE "{table_name}" {schema_sql}')

            total_rows = len(df)
            batch_size = batch_size or total_rows

            for i in range(0, total_rows, batch_size):
                if isinstance(df, pd.DataFrame):
                    batch = df.iloc[i:i+batch_size]
                else:
                    batch = df.slice(i, batch_size)

                self.conn.register('current_batch', batch)
                self.conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM current_batch')
                self.conn.unregister('current_batch')
        else:
            self.conn.register('temp_df', df)
            self.conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM temp_df')
            self.conn.unregister('temp_df')

    @staticmethod
    def _create_schema(schema: dict) -> str:
        columns = [f'"{col}" {dtype}' for col, dtype in schema.items()]
        return f"({', '.join(columns)})"

    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get information about a table in the database.
        """
        return self.conn.execute(f'DESCRIBE "{table_name}"').df()

    def close_connection(self) -> None:
        """Close the connection to the database."""
        self.conn.close()
