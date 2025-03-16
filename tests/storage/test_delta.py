"""Unit tests for Delta storage implementation."""
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pytest

from minilake.config import Config
from minilake.storage.delta import DeltaStorage


def create_sample_data() -> pd.DataFrame:
    """Create sample data for tests."""
    return pd.DataFrame({
        'id': range(1, 6),
        'name': ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon'],
        'value': [10.0, 20.0, 30.0, 40.0, 50.0],
        'date': pd.date_range('2024-01-01', periods=5)
    }).astype({
        'id': 'int64',
        'name': 'string',
        'value': 'float64',
        'date': 'datetime64[ns]'
    })


@pytest.fixture
def config(tmp_path) -> Config:
    """Create a test configuration."""
    delta_root = tmp_path / "delta_test"
    delta_root.mkdir()
    return Config(delta_root=str(delta_root))


@pytest.fixture
def duckdb_conn():
    """Create a DuckDB connection with sample data."""
    conn = duckdb.connect(':memory:')
    df = create_sample_data()
    conn.register('sample_df', df)
    conn.execute('CREATE TABLE test_table AS SELECT * FROM sample_df')
    yield conn
    conn.close()


@pytest.fixture
def delta_storage(duckdb_conn, config):
    """Create a DeltaStorage instance."""
    return DeltaStorage(config=config)


def test_init(delta_storage, config):
    """Test DeltaStorage initialization."""
    assert isinstance(delta_storage.config, Config)
    assert Path(delta_storage.config.delta_root).exists()


def test_create_table_basic(delta_storage, duckdb_conn):
    """Test basic table creation."""
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_basic",
        conn=duckdb_conn
    )

    info = delta_storage.get_table_info("test_basic")
    assert info['version'] == 0
    assert len(info['files']) > 0


def test_create_table_with_schema(delta_storage, duckdb_conn):
    """Test table creation with custom schema."""
    schema = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string()),
        ('value', pa.float64()),
        ('date', pa.timestamp('ns'))
    ])

    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_schema",
        schema=schema,
        conn=duckdb_conn
    )

    info = delta_storage.get_table_info("test_schema")
    assert 'fields' in info['schema']


def test_create_table_with_partitioning(delta_storage, duckdb_conn):
    """Test table creation with partitioning."""
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_partitioned",
        partition_by=["date"],
        conn=duckdb_conn
    )

    info = delta_storage.get_table_info("test_partitioned")
    assert info['version'] == 0


def test_read_to_duckdb(delta_storage, duckdb_conn):
    """Test reading Delta table into DuckDB."""
    # create a table
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_read",
        conn=duckdb_conn
    )

    # read it back
    delta_storage.read_to_duckdb(
        delta_path="test_read",
        table_name="read_test",
        conn=duckdb_conn
    )

    result = duckdb_conn.execute("""
        SELECT COUNT(*) as cnt FROM read_test
    """).fetchone()[0]
    assert result == 5
