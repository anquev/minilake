"""Unit tests for API endpoints."""


import duckdb
import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from minilake.api.endpoint.retriever import app
from minilake.storage.s3 import S3Manager


@pytest.fixture
def client():
    """Create a test client."""
    return TestClient(app)


@pytest.fixture(autouse=True)
def setup_test_table():
    """Create a test Delta table before each test."""
    # Create a test configuration
    conn = duckdb.connect(":memory:")
    s3 = S3Manager(
        conn=conn,
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-bucket",
    )

    # Drop table if it exists
    try:
        s3.conn.execute("DROP TABLE IF EXISTS test_table")
    except Exception:
        pass

    # Create DuckDB table first
    s3.conn.execute(
        """
        CREATE TABLE test_table (
            id INTEGER,
            value VARCHAR
        )
    """
    )

    # Insert some test data
    s3.conn.execute(
        """
        INSERT INTO test_table VALUES
        (1, 'test1'),
        (2, 'test2')
    """
    )

    # Convert to Delta table
    schema = pa.schema([("id", pa.int32()), ("value", pa.string())])

    s3.create_table(table_name="test_table", delta_path="test_table", schema=schema)
    yield


def test_retrieve_endpoint(client):
    """Test data retrieval endpoint with valid parameters."""
    response = client.get(
        "/retrieve",
        params={"delta_path": "test_table", "table_name": "test"},
    )
    assert response.status_code == 200
    assert response.json() == {"message": "Data retrieved successfully"}


def test_retrieve_endpoint_with_version(client):
    """Test the retrieve endpoint with version parameter."""
    response = client.get(
        "/retrieve",
        params={
            "delta_path": "test_table",
            "table_name": "test",
            "version": 0,  # First version is 0
        },
    )
    assert response.status_code == 200
    assert response.json() == {"message": "Data retrieved successfully"}


def test_retrieve_endpoint_with_invalid_timestamp(client):
    """Test the retrieve endpoint with invalid timestamp parameter."""
    response = client.get(
        "/retrieve",
        params={
            "delta_path": "test_table",
            "table_name": "test",
            "timestamp": "invalid-timestamp",
        },
    )
    assert response.status_code == 422  # Validation error
