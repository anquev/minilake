"""DeltaStorage module for managing Delta Lake operations with DuckDB and MinIO integration."""
from pathlib import Path
from typing import Optional, Union, List, Dict
from datetime import datetime
import json
import duckdb
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import boto3
from botocore.client import Config

class DeltaStorage:
    """Manages Delta Lake storage operations with DuckDB and MinIO integration."""

    def __init__(
        self,
        duckdb_conn: duckdb.DuckDBPyConnection,
        delta_root: Optional[str] = None,
        minio_endpoint: Optional[str] = None,
        minio_access_key: Optional[str] = None,
        minio_secret_key: Optional[str] = None,
        minio_bucket: Optional[str] = None,
        use_minio: bool = False
    ):
        """Initialize DeltaStorage instance with optional MinIO configuration."""
        self.conn = duckdb_conn
        self.use_minio = use_minio

        if use_minio:
            if not all([minio_endpoint, minio_access_key, minio_secret_key, minio_bucket]):
                raise ValueError("All MinIO parameters must be provided when use_minio is True")

            self.minio_endpoint = minio_endpoint.replace('http://', '')
            self.minio_client = boto3.client(
                's3',
                endpoint_url=f'http://{self.minio_endpoint}',
                aws_access_key_id=minio_access_key,
                aws_secret_access_key=minio_secret_key,
                config=Config(signature_version='s3v4'),
                region_name='us-east-1'
            )
            self.minio_bucket = minio_bucket
            self.delta_root = delta_root or "delta-tables"

            self.storage_options = {
                "AWS_ENDPOINT_URL": f"http://{self.minio_endpoint}",
                "AWS_ACCESS_KEY_ID": minio_access_key,
                "AWS_SECRET_ACCESS_KEY": minio_secret_key,
                "AWS_REGION": "us-east-1",
                "AWS_ALLOW_HTTP": "true"
            }
        else:
            self.delta_root = Path(delta_root or 'delta-tables')
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
        """Create a Delta table from a DuckDB table."""
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
        """Read a Delta table into DuckDB."""
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

            # Configure DuckDB for S3 if using MinIO
            if self.use_minio:
                # First install and load httpfs if not already done
                try:
                    self.conn.execute("INSTALL httpfs")
                    self.conn.execute("LOAD httpfs")
                except duckdb.CatalogException:
                    pass  # Already installed and loaded

                # Configure S3 settings
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

            # Create table from first file
            file_paths = []
            for file in files:
                if self.use_minio:
                    file_paths.append(
                        f"s3://{self.minio_bucket}/{self.delta_root}/{delta_path}/{file}"
                        )
                else:
                    file_paths.append(str(_path / file))

            # Create table using UNION ALL for all files
            create_query = f'''
                CREATE OR REPLACE TABLE "{table_name}" AS
                SELECT * FROM parquet_scan('{file_paths[0]}')
            '''
            self.conn.execute(create_query)

            if len(file_paths) > 1:
                for file_path in file_paths[1:]:
                    insert_query = f'''
                        INSERT INTO "{table_name}"
                        SELECT * FROM parquet_scan('{file_path}')
                    '''
                    self.conn.execute(insert_query)

        except Exception as e:
            raise RuntimeError(f"Error reading Delta table: {str(e)}") from e

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
        For better performance
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
