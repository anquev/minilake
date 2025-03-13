"""S3/MinIO storage implementation for Delta Lake."""

import boto3
from botocore.client import Config

from minilake.core.exceptions import ConfigurationError, StorageError
from minilake.storage.delta import DeltaStorage


class S3DeltaStorage(DeltaStorage):
    """Delta Lake storage using S3/MinIO."""

    def __init__(
        self,
        conn,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        delta_root: str = "delta-tables",
        region: str = "eu-east-1",
    ):
        """Initialize S3 Delta storage.

        Args:
            conn: DuckDB connection
            endpoint: S3/MinIO endpoint
            access_key: S3/MinIO access key
            secret_key: S3/MinIO secret key
            bucket: S3/MinIO bucket
            delta_root: Root directory for Delta tables
            region: AWS region
        """
        if not all([endpoint, access_key, secret_key, bucket]):
            raise ConfigurationError("Incomplete S3 configuration")

        # Setup storage options for Delta Lake
        storage_options = {
            "AWS_ENDPOINT_URL": f"http://{endpoint}",
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_REGION": region,
            "AWS_ALLOW_HTTP": "true",
        }

        super().__init__(conn, storage_options)

        self.endpoint = endpoint
        self.bucket = bucket
        self.delta_root = delta_root

        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=f"http://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name=region,
        )

    def _get_delta_path(self, delta_path: str) -> str:
        """Get the full path to a Delta table."""
        return f"s3://{self.bucket}/{self.delta_root}/{delta_path}"

    def _load_delta_files(
        self, files: list[str], delta_path: str, table_name: str
    ) -> None:
        """Load Delta table files into DuckDB."""
        try:
            try:
                self.conn.execute("INSTALL httpfs")
                self.conn.execute("LOAD httpfs")
            except Exception:
                pass

            # Configure S3 connection in DuckDB
            self.conn.execute("SET s3_region='eu-east-1'")
            self.conn.execute(
                f"SET s3_access_key_id='{self.storage_options['AWS_ACCESS_KEY_ID']}'"
            )
            secret_key = self.storage_options['AWS_SECRET_ACCESS_KEY']
            self.conn.execute(f"SET s3_secret_access_key='{secret_key}'")
            self.conn.execute(f"SET s3_endpoint='{self.endpoint}'")
            self.conn.execute("SET s3_use_ssl=false")
            self.conn.execute("SET s3_url_style='path'")

            # Prepare file paths
            file_paths = []
            for file in files:
                file_paths.append(f"{delta_path}/{file}")

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
            raise StorageError(f"Error loading Delta files into DuckDB: {e!s}") from e
