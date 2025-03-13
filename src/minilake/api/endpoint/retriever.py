"""
API endpoint for data retrieval communicating with S3.
"""

import datetime

import duckdb
from fastapi import FastAPI

from minilake import S3Manager

app = FastAPI()
conn = duckdb.connect(database=":memory:", read_only=False)
s3 = S3Manager()


@app.get("/retrieve")
def retrieve_data(
    delta_path: str,
    table_name: str,
    version: int | None = None,
    timestamp: str | datetime.datetime | None = None,
) -> dict[str, str]:
    """Retrieve data from a Delta table.

    Args:
        delta_path: Path to the Delta table
        table_name: Name of the table to create
        version: Optional version number to retrieve
        timestamp: Optional timestamp to retrieve data at

    Returns:
        Dict containing a success message
    """
    s3._to_duckdb(delta_path, table_name, version, timestamp)
    return {"message": "Data retrieved successfully"}
