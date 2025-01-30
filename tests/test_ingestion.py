import pytest
import pandas as pd
import polars as pl
import json
from pathlib import Path
import duckdb
from minilake.ingestion.file_ingestion import DataIngestion

@pytest.fixture
def data_ingestion():
    return DataIngestion()

def test_ingest_parquet(tmp_path, data_ingestion):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    file_path = tmp_path / "test.parquet"
    df.to_parquet(file_path)
    data_ingestion.ingest_file(file_path, "test_table")
    result = data_ingestion.query("SELECT * FROM test_table")
    pd.testing.assert_frame_equal(result, df)

def test_ingest_csv(tmp_path, data_ingestion):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    file_path = tmp_path / "test.csv"
    df.to_csv(file_path, index=False)
    data_ingestion.ingest_file(file_path, "test_table")
    result = data_ingestion.query("SELECT * FROM test_table")
    pd.testing.assert_frame_equal(result, df)

def test_ingest_csv_with_schema(tmp_path, data_ingestion):
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    file_path = tmp_path / "test.csv"
    df.to_csv(file_path, index=False)
    schema = {"id": "INTEGER", "name": "VARCHAR"}
    data_ingestion.ingest_file(file_path, "users", schema=schema)
    table_info = data_ingestion.get_table_info("users")
    assert table_info["column_name"].tolist() == ["id", "name"]
    assert table_info["column_type"].tolist() == ["INTEGER", "VARCHAR"]

def test_ingest_json(tmp_path, data_ingestion):
    data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    file_path = tmp_path / "test.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
    data_ingestion.ingest_file(file_path, "test_table")
    result = data_ingestion.query("SELECT * FROM test_table")
    expected = pd.DataFrame(data)
    pd.testing.assert_frame_equal(result, expected)

def test_ingest_json_with_schema(tmp_path, data_ingestion):
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    file_path = tmp_path / "test.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
    schema = {"id": "INTEGER", "name": "VARCHAR"}
    data_ingestion.ingest_file(file_path, "users", schema=schema)
    table_info = data_ingestion.get_table_info("users")
    assert table_info["column_name"].tolist() == ["id", "name"]
    assert table_info["column_type"].tolist() == ["INTEGER", "VARCHAR"]

def test_ingest_excel(tmp_path, data_ingestion):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    file_path = tmp_path / "test.xlsx"
    df.to_excel(file_path, index=False)
    data_ingestion.ingest_file(file_path, "test_table")
    result = data_ingestion.query("SELECT * FROM test_table")
    pd.testing.assert_frame_equal(result, df)

def test_ingest_dataframe_pandas(data_ingestion):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    data_ingestion._ingest_dataframe(df, "test_table", schema=None)
    result = data_ingestion.query("SELECT * FROM test_table")
    pd.testing.assert_frame_equal(result, df)

def test_ingest_dataframe_with_schema_and_batch(data_ingestion):
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
    schema = {"id": "INTEGER", "name": "VARCHAR"}
    data_ingestion._ingest_dataframe(df, "users", schema=schema, batch_size=2)
    result = data_ingestion.query("SELECT * FROM users")
    assert len(result) == 3

def test_ingest_dataframe_polars(data_ingestion):
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    data_ingestion._ingest_dataframe(df, "test_table", schema=None)
    result = data_ingestion.query("SELECT * FROM test_table")
    assert isinstance(result, pd.DataFrame)
    pl_result = data_ingestion.query("SELECT * FROM test_table", output_format=pl.DataFrame)
    assert isinstance(pl_result, pl.DataFrame)
    assert pl_result.shape == (2, 2)

def test_query_polars_output(data_ingestion):
    data_ingestion.conn.execute("CREATE TABLE test AS SELECT 1 AS a, 'x' AS b")
    result = data_ingestion.query("SELECT * FROM test", output_format=pl.DataFrame)
    assert isinstance(result, pl.DataFrame)
    assert result.to_dict(as_series=False) == {"a": [1], "b": ["x"]}

def test_ingest_file_not_found(data_ingestion):
    with pytest.raises(FileNotFoundError):
        data_ingestion.ingest_file("nonexistent.parquet", "test_table")

def test_unsupported_format(tmp_path, data_ingestion):
    file_path = tmp_path / "test.xyz"
    file_path.touch()
    with pytest.raises(ValueError) as excinfo:
        data_ingestion.ingest_file(file_path, "test_table")
    assert "Unsupported file format" in str(excinfo.value)

def test_get_table_info(data_ingestion):
    data_ingestion.conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
    table_info = data_ingestion.get_table_info("test")
    assert table_info["column_name"].tolist() == ["id", "name"]
    assert table_info["column_type"].tolist() == ["INTEGER", "VARCHAR"]

def test_close(data_ingestion):
    data_ingestion.close()
    with pytest.raises(duckdb.ConnectionException):
        data_ingestion.conn.execute("SELECT 1")