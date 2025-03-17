"""
API endpoint for data retrieval communicating with S3.
"""

import datetime

import duckdb
from fastapi import FastAPI, HTTPException

from minilake import S3Manager
from minilake.config import Config

app = FastAPI()
conn = duckdb.connect(database=":memory:", read_only=False)

config = Config()
s3 = S3Manager(
    conn=conn,
    endpoint=config.minio_endpoint,
    access_key=config.minio_access_key,
    secret_key=config.minio_secret_key,
    bucket=config.minio_bucket,
)


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
    # Convert timestamp string to datetime
    if isinstance(timestamp, str):
        try:
            timestamp = datetime.datetime.fromisoformat(timestamp)
        except ValueError as err:
            raise HTTPException(
                status_code=422,
                detail="Invalid timestamp format. Use ISO format "
                "(e.g., '2024-01-01T00:00:00')"
            ) from err

    # Only pass version to read_to_duckdb since timestamp is not supported
    s3.read_to_duckdb(delta_path, table_name, version=version)
    return {"message": "Data retrieved successfully"}
