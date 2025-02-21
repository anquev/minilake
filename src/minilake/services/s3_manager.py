"""Module for communicating with S3 storage deltalake tables"""
import os
from typing import Optional, List, Dict, Union
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import pyarrow as pa
from minilake.services.file_ingestion import DataIngestion
from minilake.services.delta_storage import DeltaStorage

class S3Manager:
    """Manages Delta Lake tables with S3/MinIO storage backend."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None
    ):
        """Initialize S3Manager.

        Args:
            endpoint: S3/MinIO endpoint URL (e.g., 'localhost:9000')
            access_key: S3/MinIO access key
            secret_key: S3/MinIO secret key
            bucket: S3/MinIO bucket name
        """
        load_dotenv()

        self.endpoint = endpoint or os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.access_key = access_key or os.getenv('MINIO_ROOT_USER')
        self.secret_key = secret_key or os.getenv('MINIO_ROOT_PASSWORD')
        self.bucket = bucket or os.getenv('MINIO_DEFAULT_BUCKETS', '').split(',')[0]

        if not all([self.endpoint, self.access_key, self.secret_key, self.bucket]):
            raise ValueError("Missing required S3/MinIO configuration")

        self.ingestion = DataIngestion(':memory:')
        self.storage = DeltaStorage(
            duckdb_conn=self.ingestion.conn,
            minio_endpoint=self.endpoint,
            minio_access_key=self.access_key,
            minio_secret_key=self.secret_key,
            minio_bucket=self.bucket,
            use_minio=True
        )

    def make_deltatable(
        self,
        file_path: Union[str, Path],
        table_name: str,
        delta_path: str,
        partition_by: Optional[List[str]] = None
    ) -> None:
        """Ingest a file and write it as a Delta table to S3/MinIO.

        Args:
            file_path: Path to the input file
            table_name: Name for the temporary DuckDB table
            delta_path: Path where the Delta table will be created in S3
            partition_by: List of columns to partition by
        """
        try:
            # Ingest the file into DuckDB
            self.ingestion.ingest_file(
                file_path=file_path,
                table_name=table_name
            )

            # Write to Delta format in S3
            self.storage.create_table(
                table_name=table_name,
                delta_path=delta_path,
                partition_by=partition_by,
                mode="overwrite"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to process file {file_path}: {str(e)}") from e

    def _to_duckdb(
        self,
        delta_path: str,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[Union[str, datetime]] = None
    ) -> None:
        """Read a Delta table from S3/MinIO into DuckDB.

        Args:
            delta_path: Path to the Delta table in S3
            table_name: Name for the DuckDB table
            version: Specific version to read
            timestamp: Specific timestamp to read
        """
        self.storage.read_to_duckdb(
            delta_path=delta_path,
            table_name=table_name,
            version=version,
            timestamp=timestamp
        )

    def query_table(
        self,
        delta_path: str,
        query: str,
        temp_table: str = "temp_delta"
    ) -> pa.Table:
        """Query a Delta table directly.

        Args:
            delta_path: Path to the Delta table in S3
            query: SQL query to execute
            temp_table: Name for temporary table

        Returns:
            PyArrow Table
        """
        self._to_duckdb(delta_path, temp_table)
        result = self.ingestion.conn.execute(query).arrow()
        self.ingestion.conn.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
        return result

    def get_delta_info(self, delta_path: str) -> Dict:
        """Get information about a Delta table in S3/MinIO.

        Args:
            delta_path: Path to the Delta table in S3

        Returns:
            Dictionary containing table information
        """
        return self.storage.get_table_info(delta_path)

    def optimize_delta(
        self,
        delta_path: str,
        vacuum: bool = True,
        retention_hours: Optional[int] = 168,
        zorder_by: Optional[List[str]] = None
    ) -> None:
        """Optimize a Delta table in S3/MinIO.

        Args:
            delta_path: Path to the Delta table in S3
            vacuum: Whether to run vacuum operation
            retention_hours: Hours to retain history for vacuum
            zorder_by: Columns to use for Z-ordering
        """
        if vacuum:
            self.storage.vacuum(delta_path, retention_hours)
        self.storage.optimize(delta_path, zorder_by)

    def close(self):
        """Close all connections."""
        self.ingestion.close()
