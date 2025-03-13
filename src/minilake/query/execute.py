"""SQL query execution functionality."""

from typing import Any

import duckdb
import pandas as pd
import polars as pl

from minilake.core.connection import get_connection
from minilake.core.exceptions import QueryError
from minilake.storage.factory import create_storage


class QueryExecutor:
    """Execute SQL queries against Delta tables."""

    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None):
        """Initialize the query executor.

        Args:
            conn: Optional DuckDB connection
        """
        self.conn = conn or get_connection()
        self.storage = create_storage()

    def execute_query(
        self, query: str, output_format: str = "pandas", **kwargs: Any
    ) -> pd.DataFrame | pl.DataFrame:
        """Execute a SQL query.

        Args:
            query: SQL query to execute
            output_format: Output format ("pandas" or "polars")
            kwargs: Additional parameters

        Returns:
            Query results as a DataFrame

        Raises:
            QueryError: If the query fails
        """
        try:
            if output_format.lower() == "pandas":
                return self.conn.execute(query).df()
            elif output_format.lower() == "polars":
                return self.conn.execute(query).pl()
            else:
                raise QueryError(f"Unsupported output format: {output_format}")
        except Exception as e:
            raise QueryError(f"Error executing query: {e!s}") from e

    def query_delta_table(
        self,
        delta_path: str,
        query: str,
        temp_table: str | None | None = None,
        version: int | None = None,
        timestamp: str | None = None,
        output_format: str = "pandas",
        **kwargs: Any,
    ) -> pd.DataFrame | pl.DataFrame:
        """Query a Delta table.

        Args:
            delta_path: Path to the Delta table
            query: SQL query to execute
            temp_table: Name of temporary table (generated if None)
            version: Optional specific version to query
            timestamp: Optional timestamp to query data as of
            output_format: Output format ("pandas" or "polars")
            kwargs: Additional parameters

        Returns:
            Query results as a DataFrame (Pandas or Polars)

        Raises:
            QueryError: Query failed
        """
        if temp_table is None:
            temp_table = f"temp_{delta_path.replace('/', '_')}_{id(self)}"

        try:
            # Loading Delta table into DuckDB
            self.storage.read_to_duckdb(delta_path, temp_table, version, timestamp)

            # Ensure the table name is properly referenced
            if query.strip().upper().startswith("SELECT"):
                modified_query = query.replace("FROM delta_table", f"FROM {temp_table}")
                if modified_query == query:  # Not replaced
                    modified_query = query
            else:
                modified_query = query

            # Executing query
            result = self.execute_query(modified_query, output_format, **kwargs)

            return result
        except Exception as e:
            raise QueryError(f"Error querying Delta table: {e!s}") from e
        finally:
            try:
                self.conn.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
            except Exception:
                pass
