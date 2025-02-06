"""Unit tests for DeltaStorage class."""
import pytest
import duckdb
import pandas as pd
import pyarrow as pa
from pathlib import Path
import shutil
from datetime import datetime
from minilake.services.delta_storage import DeltaStorage


def create_sample_data():
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
def duckdb_conn():
    """Create a DuckDB connection with sample data."""
    conn = duckdb.connect(':memory:')
    df = create_sample_data()
    conn.register('sample_df', df)
    conn.execute('CREATE TABLE test_table AS SELECT * FROM sample_df')
    return conn


@pytest.fixture
def delta_root(tmp_path):
    """Create a temporary directory for Delta tables."""
    delta_path = tmp_path / "delta_test"
    delta_path.mkdir()
    return delta_path


@pytest.fixture
def delta_storage(duckdb_conn, delta_root):
    """Create a DeltaStorage instance."""
    storage = DeltaStorage(duckdb_conn, str(delta_root))
    yield storage
    duckdb_conn.close()


def test_init(delta_storage, delta_root):
    """Test DeltaStorage initialization."""
    assert delta_storage.conn is not None
    assert Path(delta_storage.delta_root) == delta_root
    assert Path(delta_storage.delta_root).exists()


def test_create_table_basic(delta_storage):
    """Test basic table creation."""
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_basic"
    )
    
    info = delta_storage.get_table_info("test_basic")
    assert info['version'] == 0
    assert len(info['files']) > 0


def test_create_table_with_schema(delta_storage):
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
        schema=schema
    )
    
    info = delta_storage.get_table_info("test_schema")
    assert 'fields' in info['schema']


def test_create_table_with_partitioning(delta_storage):
    """Test table creation with partitioning."""
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_partitioned",
        partition_by=["date"]
    )
    
    info = delta_storage.get_table_info("test_partitioned")
    assert info['version'] == 0


def test_simple_read(delta_storage):
    """Test basic table reading."""
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_read"
    )
    
    delta_storage.read_to_duckdb(
        delta_path="test_read",
        table_name="read_test"
    )
    
    result = delta_storage.conn.execute("""
        SELECT COUNT(*) as cnt FROM read_test
    """).fetchone()[0]
    assert result == 5


def test_table_info(delta_storage):
    """Test getting table information."""
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_info"
    )
    
    info = delta_storage.get_table_info("test_info")
    assert isinstance(info, dict)
    assert 'version' in info
    assert 'metadata' in info
    assert 'files' in info
    assert 'history' in info
    assert 'schema' in info


def test_optimize(delta_storage):
    """Test table optimization."""
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_optimize",
        partition_by=["date"]
    )
    
    delta_storage.optimize(
        delta_path="test_optimize",
        zorder_by=["id"]
    )
    
    info = delta_storage.get_table_info("test_optimize")
    assert len(info['history']) >= 1


def test_error_handling(delta_storage):
    """Test error handling for various scenarios."""
    with pytest.raises(Exception):
        delta_storage.read_to_duckdb(
            delta_path="nonexistent",
            table_name="should_fail"
        )


def test_append_mode(delta_storage):
    """Test appending data to existing table."""
    # Create initial table
    delta_storage.create_table(
        table_name="test_table",
        delta_path="test_append"
    )
    
    # Create new data
    new_data = pd.DataFrame({
        'id': range(6, 8),
        'name': ['Zeta', 'Eta'],
        'value': [60.0, 70.0],
        'date': pd.date_range('2024-01-06', periods=2)
    }).astype({
        'id': 'int64',
        'name': 'string',
        'value': 'float64',
        'date': 'datetime64[ns]'
    })
    
    delta_storage.conn.register('new_data', new_data)
    delta_storage.conn.execute('CREATE TABLE append_data AS SELECT * FROM new_data')
    
    delta_storage.create_table(
        table_name="append_data",
        delta_path="test_append",
        mode="append"
    )
    
    delta_storage.read_to_duckdb(
        delta_path="test_append",
        table_name="final_data"
    )
    
    result = delta_storage.conn.execute("""
        SELECT COUNT(*) as cnt FROM final_data
    """).fetchone()[0]
    assert result == 7  # 5 original + 2 appended rows