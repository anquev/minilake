"""Tests for Delta storage implementation."""
import os
from pathlib import Path
import pytest
import duckdb
import pyarrow as pa
from dotenv import load_dotenv
from minilake.storage.s3 import S3Manager
from minilake.core.exceptions import StorageError
from deltalake import DeltaTable


@pytest.fixture
def config():
    """Load and return test configuration from environment variables."""
    load_dotenv()
    return type(
        "Config",
        (),
        {
            "minio_endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            "minio_access_key": os.getenv("MINIO_ROOT_USER"),
            "minio_secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
            "minio_bucket": os.getenv("MINIO_DEFAULT_BUCKETS").split(",")[0],
            "delta_root": str(Path("/tmp/delta-tables")),
        },
    )


@pytest.fixture
def delta_storage(config):
    """Create and return a Delta storage instance with a memory database connection."""
    conn = duckdb.connect(":memory:")
    return S3Manager(
        conn=conn,
        endpoint=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        bucket=config.minio_bucket,
        delta_root=config.delta_root,
    )


@pytest.fixture
def cleanup_tables():
    """Fixture to clean up test tables after tests."""
    yield

    conn = duckdb.connect(":memory:")
    for table in ["test_table", "test_table_read", "test_table_partitioned"]:
        try:
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        except:
            pass


def test_init(config, delta_storage):
    """Verify S3Manager initializes with correct configuration values."""
    assert delta_storage.endpoint == config.minio_endpoint
    assert delta_storage.bucket == config.minio_bucket
    assert delta_storage.delta_root == config.delta_root
    assert delta_storage.conn is not None
    assert delta_storage.s3_client is not None


def test_create_table_basic(delta_storage, cleanup_tables):
    """Verify basic Delta table creation with simple schema and data."""
    conn = delta_storage.conn
    
    conn.execute("""
        CREATE TABLE test_table (
            id INTEGER,
            value VARCHAR
        )
    """)
    
    conn.execute("INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')")
    
    schema = pa.schema([
        ('id', pa.int32()),
        ('value', pa.string())
    ])
    
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_table_basic",
        schema=schema
    )
    
    result = conn.execute("SELECT * FROM test_table").fetchall()
    assert len(result) == 2
    assert result[0][0] == 1
    assert result[0][1] == 'test1'
    assert result[1][0] == 2
    assert result[1][1] == 'test2'
    
    info = delta_storage.get_table_info("test_table_basic")
    assert "version" in info
    assert "metadata" in info
    assert "files" in info
    assert len(info["files"]) > 0


def test_create_table_with_schema(delta_storage, cleanup_tables):
    """Verify Delta table creation with complex schema including timestamps."""
    conn = delta_storage.conn
    schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
        ('age', pa.int32()),
        ('created_at', pa.timestamp('us'))
    ])
    
    conn.execute("""
        CREATE TABLE test_table (
            id INTEGER,
            name VARCHAR,
            age INTEGER,
            created_at TIMESTAMP
        )
    """)
    
    conn.execute("""
        INSERT INTO test_table VALUES 
        (1, 'John', 30, '2024-01-01 00:00:00'),
        (2, 'Jane', 25, '2024-01-02 00:00:00')
    """)
    
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_table_schema",
        schema=schema
    )
    
    info = delta_storage.get_table_info("test_table_schema")
    assert len(info["files"]) > 0
    
    delta_schema = info["schema"]
    assert len(delta_schema["fields"]) == 4
    
    field_names = [field["name"] for field in delta_schema["fields"]]
    assert "id" in field_names
    assert "name" in field_names
    assert "age" in field_names
    assert "created_at" in field_names


def test_create_table_with_partitioning(delta_storage, cleanup_tables):
    """Verify Delta table creation with partition columns for data distribution."""
    conn = delta_storage.conn
    schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
        ('age', pa.int32()),
        ('created_at', pa.timestamp('us'))
    ])

    conn.execute("""
        CREATE TABLE test_table (
            id INTEGER,
            name VARCHAR,
            age INTEGER,
            created_at TIMESTAMP
        )
    """)
    
    conn.execute("""
        INSERT INTO test_table VALUES 
        (1, 'John', 30, '2024-01-01 00:00:00'),
        (2, 'Jane', 25, '2024-01-02 00:00:00'),
        (3, 'Bob', 30, '2024-01-03 00:00:00'),
        (4, 'Alice', 25, '2024-01-04 00:00:00')
    """)
    
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_table_partitioned",
        schema=schema,
        partition_by=["age"]
    )

    # Open the table directly to verify partitioning
    _path = delta_storage._get_delta_path("test_table_partitioned")
    dt = DeltaTable(str(_path), storage_options=delta_storage.storage_options)
    
    # Check that table has partition columns
    partition_cols = dt.metadata().partition_columns
    assert "age" in partition_cols
    
    # Also check we can read data back correctly
    delta_storage.read_to_duckdb(
        delta_path="test_table_partitioned", 
        table_name="test_partition_read"
    )
    result = conn.execute("SELECT DISTINCT age FROM test_partition_read ORDER BY age").fetchall()
    assert len(result) == 2
    assert result[0][0] == 25
    assert result[1][0] == 30


def test_read_to_duckdb(delta_storage, cleanup_tables):
    """Verify reading Delta table back into DuckDB preserves data integrity."""
    conn = delta_storage.conn
    
    conn.execute("""
        CREATE TABLE test_table (
            id INTEGER,
            value VARCHAR
        )
    """)
    
    conn.execute("INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')")
    
    schema = pa.schema([
        ('id', pa.int32()),
        ('value', pa.string())
    ])

    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_table_read_test",
        schema=schema
    )
    
    # Clean up any existing read table
    try:
        conn.execute("DROP TABLE IF EXISTS test_table_read")
    except:
        pass
    
    delta_storage.read_to_duckdb(
        delta_path="test_table_read_test",
        table_name="test_table_read"
    )
    
    result = conn.execute("SELECT * FROM test_table_read ORDER BY id").fetchall()
    assert len(result) == 2
    assert result[0][0] == 1
    assert result[0][1] == 'test1'
    assert result[1][0] == 2
    assert result[1][1] == 'test2'


def test_error_handling(delta_storage):
    """Verify proper error handling for non-existent resources."""
    with pytest.raises(StorageError) as excinfo:
        delta_storage.read_to_duckdb(
            delta_path="nonexistent_table",
            table_name="test_table"
        )
    assert "Error reading Delta table" in str(excinfo.value)
    
    with pytest.raises(StorageError) as excinfo:
        delta_storage.get_table_info("nonexistent_table")
    assert "Error getting table info" in str(excinfo.value)
