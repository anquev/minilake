# src/minilake/services/s3_manager.py
"""Module to manage S3 operations with DuckDB layer."""
from typing import Optional, List, Union
from pathlib import Path
import pyarrow as pa
from minilake.config import Config
from minilake.db_conn import DBConnection
from minilake.services.file_ingestion import DataIngestion
from minilake.services.delta_storage import DeltaStorage

class S3Manager:
    """
    Handle S3 operations with DuckDB layer.
    """

    def __init__(
        self,
        config: Optional[Config] = None,
        data_ingestion: Optional[DataIngestion] = None,
        delta_storage: Optional[DeltaStorage] = None
    ):
        """Initialize the S3Manager class with DuckDB connection."""
        self.config = config or Config()

        if not self.config.use_minio:
            raise ValueError("S3Manager requires a S3 configuration")

        # DuckDB connection
        self.conn = DBConnection.get_connection()

        # Init storage et ingestion services
        self.ingestion = data_ingestion or DataIngestion(conn=self.conn)
        self.storage = delta_storage or DeltaStorage(config=self.config, conn=self.conn)

    def make_deltatable(
        self,
        file_path: Union[str, Path],
        table_name: str,
        delta_path: str,
        partition_by: Optional[List[str]] = None
    ) -> None:
        """Smooth file ingestion and Delta table creation."""
        try:
            # Ingest file to DuckDB
            self.ingestion.ingest_file(
                file_path=file_path,
                table_name=table_name
            )

            # Wrtie table to bucket
            self.storage.create_table(
                table_name=table_name,
                delta_path=delta_path,
                partition_by=partition_by,
                mode="overwrite"
            )
        except Exception as e:
            raise RuntimeError(f"{file_path} processing failed: {str(e)}") from e

    def query_table(
        self,
        delta_path: str,
        query: str,
        temp_table: str = "temp_delta"
    ) -> pa.Table:
        """Execute a query on a Delta table in S3/MinIO"""
        self.storage.read_to_duckdb(delta_path, temp_table)
        result = self.ingestion.conn.execute(query).arrow()
        self.ingestion.conn.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
        return result

    def close(self):
        """Close connection."""
        return None
