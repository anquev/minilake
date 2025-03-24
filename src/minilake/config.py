# src/minilake/config.py
"""
This module defines the configuration for connectors and Delta Lake storage.
It includes the necessary parameters for connecting to MinIO and Delta Lake,
as well as methods to retrieve storage options.
"""

import os

from dotenv import load_dotenv


class Config:
    """Configuration for connectors and Delta Lake storage"""

    class DatabaseConfig:
        def __init__(self, path: str = "default.db"):
            self.path = path

    class StorageConfig:
        def __init__(
            self, delta_root: str = "delta-tables", storage_type: str = "local"
        ):
            self.delta_root = delta_root
            self.type = storage_type

    class S3Config:
        def __init__(
            self,
            endpoint: str | None = None,
            access_key: str | None = None,
            secret_key: str | None = None,
            bucket: str | None = None,
            region: str = "eu-east-1",
        ):
            self.endpoint = endpoint
            self.access_key = access_key
            self.secret_key = secret_key
            self.bucket = bucket
            self.region = region

            self.is_configured = all(
                [self.endpoint, self.access_key, self.secret_key, self.bucket]
            )

    def __init__(
        self,
        minio_endpoint: str | None = None,
        minio_access_key: str | None = None,
        minio_secret_key: str | None = None,
        minio_bucket: str | None = None,
        delta_root: str | None = None,
        database: DatabaseConfig | None = None,
    ):
        load_dotenv()

        self.minio_endpoint = minio_endpoint or os.getenv(
            "MINIO_ENDPOINT", "localhost:9000"
        )
        self.minio_access_key = minio_access_key or os.getenv("MINIO_ROOT_USER")
        self.minio_secret_key = minio_secret_key or os.getenv("MINIO_ROOT_PASSWORD")
        self.minio_bucket = (
            minio_bucket or os.getenv("MINIO_DEFAULT_BUCKETS", "").split(",")[0]
        )
        self.delta_root = delta_root or "delta-tables"

        self.use_minio = all(
            [
                self.minio_endpoint,
                self.minio_access_key,
                self.minio_secret_key,
                self.minio_bucket,
            ]
        )

        self.database = database or self.DatabaseConfig()

        # Initialize S3Config
        self.s3 = self.S3Config(
            endpoint=self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            bucket=self.minio_bucket,
        )

        # Initialize StorageConfig
        storage_type = "s3" if self.use_minio else "local"
        self.storage = self.StorageConfig(
            delta_root=self.delta_root, storage_type=storage_type
        )

    @classmethod
    def from_env(cls):
        """Create a Config instance from environment variables.

        Returns:
            Config: A new Config instance initialized from environment variables
        """
        return cls()

    def get_storage_options(self) -> dict[str, str] | None:
        """Obtenir les options de stockage S3 pour Delta Lake."""
        if not self.use_minio:
            return None

        return {
            "AWS_ENDPOINT_URL": f"http://{self.minio_endpoint}",
            "AWS_ACCESS_KEY_ID": self.minio_access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio_secret_key,
            "AWS_REGION": "eu-east-1",
            "AWS_ALLOW_HTTP": "true",
        }
