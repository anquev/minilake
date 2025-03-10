# src/minilake/config.py
"""
This module defines the configuration for connectors and Delta Lake storage.
It includes the necessary parameters for connecting to MinIO and Delta Lake,
as well as methods to retrieve storage options.
"""

from typing import Dict, Optional
import os
from dotenv import load_dotenv

class Config:
    """Configuration for connectors and Delta Lake storage"""

    def __init__(
        self,
        minio_endpoint: Optional[str] = None,
        minio_access_key: Optional[str] = None,
        minio_secret_key: Optional[str] = None,
        minio_bucket: Optional[str] = None,
        delta_root: Optional[str] = None
    ):
        load_dotenv()

        self.minio_endpoint = minio_endpoint or os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = minio_access_key or os.getenv('MINIO_ROOT_USER')
        self.minio_secret_key = minio_secret_key or os.getenv('MINIO_ROOT_PASSWORD')
        self.minio_bucket = minio_bucket or os.getenv('MINIO_DEFAULT_BUCKETS', '').split(',')[0]
        self.delta_root = delta_root or "delta-tables"

        self.use_minio = all([self.minio_endpoint, self.minio_access_key,
                              self.minio_secret_key, self.minio_bucket])

    def get_storage_options(self) -> Optional[Dict[str, str]]:
        """Obtenir les options de stockage S3 pour Delta Lake."""
        if not self.use_minio:
            return None

        return {
            "AWS_ENDPOINT_URL": f"http://{self.minio_endpoint}",
            "AWS_ACCESS_KEY_ID": self.minio_access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio_secret_key,
            "AWS_REGION": "eu-east-1",
            "AWS_ALLOW_HTTP": "true"
        }
