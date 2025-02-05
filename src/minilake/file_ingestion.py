"""
This module contains the DataIngestion class 
which is used for ingesting data from various file formats.
"""
from pathlib import Path
from typing import Union, Optional
import duckdb
import pandas as pd
import polars as pl

class DataIngestion:
    """
    Class for ingesting data from various file formats.
    Accepted file formats are:
    - CSV
    - Parquet
    - JSON
    - Excel
    """
    def __init__(self, database: Optional[str] = ':memory:'):
        self.conn = duckdb.connect(database)
        self._init_extensions()

    def _init_extensions(self):
        self.conn.execute("INSTALL httpfs; LOAD httpfs;")
        self.conn.execute("INSTALL parquet; LOAD parquet;")
        self.conn.execute("INSTALL json; LOAD json;")

    def ingest_file(
            self,
            file_path: Union[str, Path],
            table_name: str,
            schema: Optional[dict] = None,
            batch_size: Optional[int] = 100000
    ) -> None:
        """
        Ingest data from a file into a table in the database.
        
        Inputs:
        - file_path: str or Path
            Path to the file to ingest.
        - table_name: str
            Name of the table to create in the database.
        - schema: dict, default None
            Schema of the table to create.
            If None, the schema will be inferred from the file.
        - batch_size: int, default 100000
            Batch size to use when ingesting data from a DataFrame.
            
        Returns:
        None
        """

        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError("File not found, please verify the provided path")

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
                raise ValueError(f"""
                                 Unsupported file format {extension},
                                 please provide a valid file extension.
                                 """)
        except Exception as e:
            raise e

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

    def query(self,
              sql: str,
              output_format: Union[pd.DataFrame, pl.DataFrame] = pd.DataFrame
              ) -> Union[pd.DataFrame, pl.DataFrame]:
        """
        Execute a SQL query on the database and return the result in the specified format.
        
        Inputs:
        - sql: str
            SQL query to execute (using DuckDB syntax).
        - output_format: Union[pd.DataFrame, pl.DataFrame], default pd.DataFrame
            Output format of the result.
            Supported formats are pd.DataFrame and pl.DataFrame.
        
        Returns:
        - Union[pd.DataFrame, pl.DataFrame]
            Result of the query in the specified format.
        """

        if output_format == pd.DataFrame:
            return self.conn.execute(sql).df()
        if output_format == pl.DataFrame:
            return self.conn.execute(sql).pl()
        raise ValueError(f"Output format not supported: {output_format}")

    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get information about a table in the database.
        
        Inputs:
        - table_name: str
            Name of the table to get information about.
            
        Returns:
        - pd.DataFrame
            Information about the table.
        """
        return self.conn.execute(f'DESCRIBE "{table_name}"').df()

    def close(self):
        """
        Close the connection to the database.
        """
        self.conn.close()
