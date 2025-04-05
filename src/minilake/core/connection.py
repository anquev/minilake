"""Database connection management module."""

import os
import threading

import boto3
import duckdb
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from minilake.core.exceptions import ConnectionError

from .exceptions import MinilakeConnectionError


class DBConnection:
    """Thread-safe singleton database connection manager."""

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_connection(
        cls, database: str | None = ":memory:", read_only: bool = False
    ) -> duckdb.DuckDBPyConnection:
        """Get the DuckDB connection instance.

        Args:
            database: Path to database file or :memory: for in-memory database
            read_only: Whether to open the database in read-only mode

        Returns:
            DuckDB connection object
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(database, read_only)
            return cls._instance.conn

    def __init__(self, database: str | None = ":memory:", read_only: bool = False):
        """Initialize a new DuckDB connection.

        Args:
            database: Path to database file or :memory: for in-memory database
            read_only: Whether to open the database in read-only mode
        """
        if DBConnection._instance is not None:
            raise ConnectionError(
                "DBConnection is a singleton. Use get_connection() instead."
            )

        try:
            self.conn = duckdb.connect(database, read_only=read_only)
            self._init_extensions()
            DBConnection._instance = self
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {e}") from e

    def _init_extensions(self) -> None:
        """
        Initialize DuckDB extensions.
        It should already be loaded, but we reload it to be sure.
        """
        extensions = ["httpfs", "parquet", "json"]

        for extension in extensions:
            try:
                self.conn.execute(f"INSTALL {extension}; LOAD {extension};")
            except duckdb.CatalogException:
                pass


def get_connection(
    database: str | None = ":memory:", read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection."""
    return DBConnection.get_connection(database, read_only)


class MinilakeConnection:
    def __init__(self):
        """Initialize connection with MinIO service."""
        try:
            load_dotenv()

            # login vars
            required_vars = [
                "MINIO_ROOT_USER",
                "MINIO_ROOT_PASSWORD",
                "MINIO_DEFAULT_BUCKETS",
            ]

            missing_vars = [var for var in required_vars if not os.getenv(var)]

            if missing_vars:
                raise MinilakeConnectionError(
                    "Missing required environment variables: "
                    f"{', '.join(missing_vars)}. "
                    "Please check your .env file."
                )

            self.s3_client = boto3.client(
                "s3",
                endpoint_url="http://localhost:9000",
                aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
                aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
                aws_session_token=None,
                config=boto3.session.Config(signature_version="s3v4"),
                verify=False,
            )

            self.bucket = os.getenv("MINIO_DEFAULT_BUCKETS").split(",")[0]

            # Test connection
            self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                MaxKeys=1,
            )

        except ClientError as err:
            if err.response["Error"]["Code"] == "InvalidAccessKeyId":
                raise MinilakeConnectionError(
                    "Invalid MinIO credentials. Please check your MINIO_ROOT_USER."
                ) from err
            elif err.response["Error"]["Code"] == "SignatureDoesNotMatch":
                raise MinilakeConnectionError(
                    "Invalid MinIO credentials. Please check your MINIO_ROOT_PASSWORD."
                ) from err
            elif err.response["Error"]["Code"] == "NoSuchBucket":
                raise MinilakeConnectionError(
                    f"MinIO bucket '{self.bucket}' does not exist or is not accessible."
                ) from err
            else:
                raise MinilakeConnectionError(
                    f"MinIO connection error: {err!s}"
                ) from err
        except Exception as err:
            raise MinilakeConnectionError(
                f"Failed to initialize MinIO connections: {err!s}"
            ) from err

    def list_s3_folders(self) -> list[str]:
        """Lists all S3 folders (prefixes) in the configured bucket.

        Returns:
            List[str]: List of folder names without the trailing slash
        """
        try:
            result = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Delimiter="/",
            )

            # Extract common prefixes (folders)
            folders = []
            if "CommonPrefixes" in result:
                folders = [
                    prefix["Prefix"].rstrip("/") for prefix in result["CommonPrefixes"]
                ]

            return sorted(folders)

        except ClientError as err:
            raise MinilakeConnectionError(
                f"Failed to list S3 folders: {err!s}"
            ) from err
