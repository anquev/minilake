"""
API endpoint for data retrieval communicating with S3.
"""

from typing import Optional, Union
import datetime
import duckdb
from fastapi import FastAPI
from minilake import S3Manager

app = FastAPI()
conn = duckdb.connect(database=':memory:', read_only=False)
s3 = S3Manager()

class Retrieval:
    @app.get("/retrieve")
    def retrieve_data(
        delta_path: str,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[Union[str, datetime]] = None
    ) -> None:
        """Retrieve data from a Delta table."""
        s3._to_duckdb(delta_path, table_name, version, timestamp)

        return {"message": "Data retrieved successfully"}

