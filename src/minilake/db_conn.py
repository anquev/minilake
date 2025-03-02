# src/minilake/db.py
"""
This module creates a connexion to a DuckDB database and initializes the required extensions.
"""
from typing import Optional
import duckdb

class DBConnection:
    """Class to manage connection to DuckDB database. Ensuring only one connection is created."""

    _instance = None

    @classmethod
    def get_connection(conn_cls, database: Optional[str] = ':memory:') -> duckdb.DuckDBPyConnection:
        """Create a connection to the database."""
        if conn_cls._instance is None: # conn_cls is the class itself
            conn_cls._instance = conn_cls(database)
        return conn_cls._instance.conn

    def __init__(self, database: Optional[str] = ':memory:'):
        if DBConnection._instance is not None:
            raise RuntimeError("DBConnection is a singleton class. Use get_connection() instead.")
        self.conn = duckdb.connect(database)
        self._init_extensions()
        DBConnection._instance = self

    def _init_extensions(self):
        """Initialize extensions."""
        try:
            self.conn.execute("INSTALL httpfs; LOAD httpfs;")
            self.conn.execute("INSTALL parquet; LOAD parquet;")
            self.conn.execute("INSTALL json; LOAD json;")
        except duckdb.CatalogException:
            pass
