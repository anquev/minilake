# src/minilake/services/delta_storage.py
"""Module to manage Delta Lake storage operations with DuckDB and MinIO integration."""
from pathlib import Path
from typing import Optional, Union, List, Dict
from datetime import datetime
import json
import duckdb
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import boto3
from botocore.client import Config
from minilake.config import Config as MinilakeConfig
from minilake.db_conn import DBConnection

class DeltaStorage:
    """Handle Delta Lake storage operations with DuckDB and MinIO"""

    def __init__(
        self,
        config: Optional[MinilakeConfig] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None
    ):
        """Initialize the DeltaStorage class with minio client."""
        self.config = config or MinilakeConfig()
        self.conn = conn or DBConnection.get_connection()

        self.use_minio = self.config.use_minio

        if self.use_minio:
            self.minio_endpoint = self.config.minio_endpoint.replace('http://', '')
            self.minio_client = boto3.client(
                's3',
                endpoint_url=f'http://{self.minio_endpoint}',
                aws_access_key_id=self.config.minio_access_key,
                aws_secret_access_key=self.config.minio_secret_key,
                config=Config(signature_version='s3v4'),
                region_name='eu-east-1'
            )
            self.minio_bucket = self.config.minio_bucket
            self.delta_root = self.config.delta_root
            self.storage_options = self.config.get_storage_options()
        else:
            self.delta_root = Path(self.config.delta_root)
            self.delta_root.mkdir(parents=True, exist_ok=True)
            self.storage_options = None

    def create_table(
            self,
            table_name: str,
            delta_path: str,
            partition_by: Optional[List[str]] = None,
            schema: Optional[pa.Schema] = None,
            mode: str = "overwrite"
    ) -> None:
        """Create a Delta table from DuckDB."""
        if self.use_minio:
            _path = f"s3://{self.minio_bucket}/{self.delta_root}/{delta_path}"
        else:
            _path = self.delta_root / delta_path

        table = self.conn.execute(f'SELECT * FROM "{table_name}"').arrow()

        if schema:
            table = table.cast(schema)

        write_deltalake(
            str(_path),
            table,
            mode=mode,
            partition_by=partition_by,
            storage_options=self.storage_options
        )

    def read_to_duckdb(
        self,
        delta_path: str,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[Union[str, datetime]] = None
    ) -> None:
        """Read a Delta table to DuckDB."""
        try:
            if self.use_minio:
                _path = f"s3://{self.minio_bucket}/{self.delta_root}/{delta_path}"
            else:
                _path = self.delta_root / delta_path

            dt_args = {
                "table_uri": str(_path),
                "storage_options": self.storage_options
            }

            if version is not None:
                dt_args["version"] = version
            elif timestamp is not None:
                dt_args["timestamp"] = timestamp

            dt = DeltaTable(**dt_args)

            files = dt.files()
            if not files:
                raise RuntimeError("No files found in Delta table")

            if self.use_minio:
                try:
                    self.conn.execute("INSTALL httpfs")
                    self.conn.execute("LOAD httpfs")
                except duckdb.CatalogException:
                    pass  # If already done

                # Configure MinIO connection
                self.conn.execute("SET s3_region='us-east-1'")
                self.conn.execute(
                    f"SET s3_access_key_id='{self.storage_options['AWS_ACCESS_KEY_ID']}'"
                    )
                self.conn.execute(
                    f"SET s3_secret_access_key='{self.storage_options['AWS_SECRET_ACCESS_KEY']}'"
                    )
                self.conn.execute(
                    f"SET s3_endpoint='{self.minio_endpoint}'"
                    )
                self.conn.execute("SET s3_use_ssl=false")
                self.conn.execute("SET s3_url_style='path'")

            file_paths = []
            for file in files:
                if self.use_minio:
                    file_paths.append(
                        f"s3://{self.minio_bucket}/{self.delta_root}/{delta_path}/{file}"
                        )
                else:
                    file_paths.append(str(_path / file))

            # Create table from first file
            create_query = f'''
                CREATE OR REPLACE TABLE "{table_name}" AS
                SELECT * FROM parquet_scan('{file_paths[0]}')
            '''
            self.conn.execute(create_query)

            # Add data from other files
            if len(file_paths) > 1:
                for file_path in file_paths[1:]:
                    insert_query = f'''
                        INSERT INTO "{table_name}"
                        SELECT * FROM parquet_scan('{file_path}')
                    '''
                    self.conn.execute(insert_query)

        except Exception as e:
            raise RuntimeError(f"Error while reading DeltaLake table: {str(e)}") from e

    def get_table_info(self, delta_path: str) -> Dict:
        """Get information about a Delta table."""
        if self.use_minio:
            _path = f"s3://{self.minio_bucket}/{self.delta_root}/{delta_path}"
        else:
            _path = self.delta_root / delta_path

        dt = DeltaTable(str(_path), storage_options=self.storage_options)

        schema_str = json.loads(dt.schema().to_json())

        return {
            'version': dt.version(),
            'metadata': dt.metadata(),
            'files': dt.files(),
            'history': dt.history(),
            'schema': schema_str
        }

    def vacuum(
            self,
            delta_path: str,
            retention: Optional[int] = 168
    ) -> None:
        """Clean up old versions of a Delta table."""
        if self.use_minio:
            _path = f"s3://{self.minio_bucket}/{self.delta_root}/{delta_path}"
        else:
            _path = self.delta_root / delta_path

        dt = DeltaTable(str(_path), storage_options=self.storage_options)

        if retention is not None and retention < 168:
            retention = 168  # 7 days

        dt.vacuum(retention_hours=retention)

    def optimize(
            self,
            delta_path: str,
            zorder_by: Optional[List[str]] = None
    ) -> None:
        """
        Optimize a Delta table by compacting and reordering data.
        If zorder_by is provided, the data will be z-ordered by the specified columns.
        """
        if self.use_minio:
            _path = f"s3://{self.minio_bucket}/{self.delta_root}/{delta_path}"
        else:
            _path = self.delta_root / delta_path

        dt = DeltaTable(str(_path), storage_options=self.storage_options)

        if zorder_by:
            dt.optimize.compact()
            dt.optimize.z_order(zorder_by)
        else:
            dt.optimize.compact()
