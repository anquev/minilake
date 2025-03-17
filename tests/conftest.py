"""Test configuration and fixtures."""
import os
import time
import boto3
from botocore.client import Config
from dotenv import load_dotenv

import pytest


@pytest.fixture(scope="session")
def minio_server():
    """Setup MinIO test environment using existing Docker Compose service."""
    load_dotenv()

    # Create S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        config=Config(signature_version="s3v4"),
        region_name="eu-east-1",
    )

    # Wait for MinIO to be ready
    max_retries = 5
    for i in range(max_retries):
        try:
            s3_client.list_buckets()
            break
        except Exception:
            if i == max_retries - 1:
                raise
            time.sleep(2)

    # Create test bucket if it doesn't exist
    try:
        s3_client.create_bucket(Bucket="test-bucket")
    except s3_client.exceptions.BucketAlreadyExists:
        pass
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass

    yield 