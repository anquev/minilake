import duckdb
import pandas as pd
import polars as pl
from pathlib import Path
from typing import Union, Optional, List

class DataIngestion:
    def __init__(self, database: Optional[str] = ':memory'):
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
                self._ingest_csv(file_path, table_name, schema, batch_size)

            elif extension in ['.xls', '.xlsx']:
                self._ingest_excel(file_path, table_name, schema, batch_size)

            ## Add support for sas tables
            # elif extension in 'sas7dbat':
            #   self._ingest_sas(file_path, table_name, schema, batch_size)

            else:
                raise ValueError("Unsupported file format {extension}, please provide a valid file extension.")
            
        except Exception as e:
            raise e
        
    def _ingest_parquet(self, file_path: Path, table_name: str) -> None:
        query = """
        CREATE TABLE {table_name} AS
        SELECT * FROM read_parquet('{file_path}')
        """
        self.conn.execute(query)

    # Better use PyIceberg ?
    # def _ingest_iceberg(self, file_path: Path, table_name: str) -> None:
    #     query = """
    #     CREATE TABLE {table_name} AS
    #     SELECT * FROM iceberg_scan('{file_path}')"""

    def _ingest_csv(
        self,
        file_path: Path,
        table_name: str,
        schema: Optional[dict]
        ) -> None:
        
        schema_sql = self._create_schema(schema) if schema else ""

        if schema_sql:
            self.conn.execute(f"CREATE TABLE {table_name} {schema_sql}")
            self.conn.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM read_csv_auto('{file_path}')
            """)
        else:
            query= f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{file_path}')
            """
            self.conn.execute(query)

    def _ingest_json(
        self,
        file_path: Path,
        table_name: str,
        schema: Optional[dict]
        ) -> None:

        schema_sql = self._create_schema(schema) if schema else ""

        if schema_sql:
            self.conn.execute("CREATE TABLE {table_name} {schema_sql}")
            self.conn.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM read_json('{file_path}')
            """)
        else:
            query= f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_json('{file_path}')
            """
            self.conn.execute(query)

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
        batch_size: Optional[int] = len(df)
        ) -> None:

        if schema:
            schema_sql = self._create_schema(schema)
            self.conn.execute(f"CREATE TABLE {table_name} {schema_sql}")

            for i in range(0, len(df), batch_size):
                batch = df.iloc[i: i + batch_size]
                self.conn.execute("INSERT INTO {table_name} SELECT * FROM batch",
                                  {"batch": batch}
                                  )
            
        else:
            self.conn.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM df",
                {"df": df}
            )

    @staticmethod
    def _create_schema(schema: dict) -> str:
        columns = [f"{col} {dtype}" for col, dtype in schema.items()]
        return f"({', '.join(columns)})"

    def query(self, sql: str, output_format: Union[pd.DataFrame, pl.DataFrame] = pd.DataFrame) -> Union[pd.DataFrame, pl.DataFrame]:
        if output_format == pd.DataFrame:
            return self.conn.execute(sql).df()
        elif output_format == pl.DataFrame:
            return self.conn.execute(sql).pl()
        else:
            raise ValueError("Output format not supported {output_format}.")
            
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        return self.conn.execute(f"DESCRIBE {table_name}").df()

    def close(self):
        self.conn.close()