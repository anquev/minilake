"""Delta Lake storage implementation."""
from typing import Optional, Union, List, Dict, Any
from datetime import datetime
from abc import abstractmethod

import json
import duckdb
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

from minilake.core.exceptions import StorageError
from minilake.storage.base import StorageInterface

class DeltaStorage(StorageInterface):
    """Base Delta Lake storage implementation.

    This class implements common Delta Lake operations regardless of the underlying
    storage system (local or S3).
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, storage_options: Optional[Dict[str, str]] = None):
        """Initialize Delta Storage.

        Args:
            conn: DuckDB connection
            storage_options: Options for Delta Lake storage
        """
        self.conn = conn
        self.storage_options = storage_options

    @abstractmethod
    def _get_delta_path(self, delta_path: str) -> str:
        """Get the full path to a Delta table.
        
        Args:
            delta_path: Relative path to the Delta table
            
        Returns:
            Full path to the Delta table
        """
        pass

    def create_table(self, table_name: str, delta_path: str, 
                    partition_by: Optional[List[str]] = None,
                    schema: Optional[pa.Schema] = None, 
                    mode: str = "overwrite") -> None:
        """Create Delta table from DuckDB table."""
        try:
            _path = self._get_delta_path(delta_path)

            table = self.conn.execute(f'SELECT * FROM "{table_name}"').arrow()

            if schema:
                table = table.cast(schema)

            write_deltalake(
                str(_path),
                table,
                mode=mode,
                partition_by=partition_by,
                storage_options=self.storage_options
            )
        except Exception as e:
            raise StorageError(f"Error creating Delta table: {str(e)}") from e

    def read_to_duckdb(self, delta_path: str, table_name: str,
                      version: Optional[int] = None,
                      timestamp: Optional[Union[str, datetime]] = None) -> None:
        """Read a Delta table into DuckDB."""
        try:
            _path = self._get_delta_path(delta_path)

            dt_args = {
                "table_uri": str(_path),
                "storage_options": self.storage_options
            }

            if version is not None:
                dt_args["version"] = version
            elif timestamp is not None:
                dt_args["timestamp"] = timestamp

            dt = DeltaTable(**dt_args)

            files = dt.files()
            if not files:
                raise StorageError("No files found in Delta table")

            self._load_delta_files(files, _path, table_name)

        except Exception as e:
            raise StorageError(f"Error reading Delta table: {str(e)}") from e

    def get_table_info(self, delta_path: str) -> Dict[str, Any]:
        """Get information about a Delta table."""
        try:
            _path = self._get_delta_path(delta_path)

            dt = DeltaTable(str(_path), storage_options=self.storage_options)

            schema_str = json.loads(dt.schema().to_json())

            return {
                'version': dt.version(),
                'metadata': dt.metadata(),
                'files': dt.files(),
                'history': dt.history(),
                'schema': schema_str
            }
        except Exception as e:
            raise StorageError(f"Error getting table info: {str(e)}") from e

    def vacuum(self, delta_path: str, retention: Optional[int] = 168) -> None:
        """Clean up old versions of a Delta table."""
        try:
            _path = self._get_delta_path(delta_path)

            dt = DeltaTable(str(_path), storage_options=self.storage_options)

            if retention is not None and retention < 168:
                retention = 168  # set to 7 days minimum

            dt.vacuum(retention_hours=retention)
        except Exception as e:
            raise StorageError(f"Error vacuuming Delta table: {str(e)}") from e

    def optimize(self, delta_path: str, zorder_by: Optional[List[str]] = None) -> None:
        """Optimize a Delta table."""
        try:
            _path = self._get_delta_path(delta_path)

            dt = DeltaTable(str(_path), storage_options=self.storage_options)

            if zorder_by:
                dt.optimize.compact()
                dt.optimize.z_order(zorder_by)
            else:
                dt.optimize.compact()
        except Exception as e:
            raise StorageError(f"Error optimizing Delta table: {str(e)}") from e

    def _load_delta_files(self, files: List[str], delta_path: str, table_name: str) -> None:
        """Load Delta table files into DuckDB."""
        pass  # Implementation depends on storage type
