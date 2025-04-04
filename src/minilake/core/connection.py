"""Database connection management module."""

import threading

import boto3
import duckdb
from botocore.exceptions import ClientError

from minilake.config import Config
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
        """Initialize connection with AWS services."""
        try:
            self.s3_client = boto3.client("s3")
            self.config = Config()
            self.bucket = self.config.minio_bucket
        except Exception as err:
            raise MinilakeConnectionError(
                "Failed to initialize AWS connections"
            ) from err

    def list_s3_folders(self) -> list[str]:
        """Lists all S3 folders (prefixes) in the configured bucket.

        Returns:
            List[str]: List of folder names without the trailing slash
        """
        try:
            # Get all objects with delimiter to simulate folder structure
            result = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Delimiter="/",
            )

            # Extract common prefixes (folders)
            folders = []
            if "CommonPrefixes" in result:
                folders = [
                    prefix["Prefix"].rstrip("/")  # Remove trailing slash
                    for prefix in result["CommonPrefixes"]
                ]

            return sorted(folders)  # Return sorted list for better UI experience

        except ClientError as err:
            raise MinilakeConnectionError(
                f"Failed to list S3 folders: {err!s}"
            ) from err
