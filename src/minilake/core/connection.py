"""Database connection management module."""
from typing import Optional, Dict, Any
import threading
import duckdb
from minilake.core.exceptions import ConnectionError

class DBConnection:
    """Thread-safe singleton database connection manager."""

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_connection(cls, database: Optional[str] = ':memory:',
                      read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """Get the DuckDB connection instance.

        Args:
            database: Path to database file or :memory: for in-memory database
            read_only: Whether to open the database in read-only mode

        Returns:
            DuckDB connection object
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(database, read_only)
            return cls._instance.conn

    def __init__(self, database: Optional[str] = ':memory:', read_only: bool = False):
        """Initialize a new DuckDB connection.

        Args:
            database: Path to database file or :memory: for in-memory database
            read_only: Whether to open the database in read-only mode
        """
        if DBConnection._instance is not None:
            raise ConnectionError(
                "DBConnection is a singleton. Use get_connection() instead."
            )

        try:
            self.conn = duckdb.connect(database, read_only=read_only)
            self._init_extensions()
            DBConnection._instance = self
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}") from e

    def _init_extensions(self) -> None:
        """
        Initialize DuckDB extensions. 
        It should already be loaded, but we reload it to be sure.
        """
        extensions = ["httpfs", "parquet", "json"]

        for extension in extensions:
            try:
                self.conn.execute(f"INSTALL {extension}; LOAD {extension};")
            except duckdb.CatalogException:
                pass

def get_connection(database: Optional[str] = ':memory:',
                  read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection."""
    return DBConnection.get_connection(database, read_only)
 