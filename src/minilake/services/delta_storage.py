"""DeltaStorage module for managing Delta Lake operations with DuckDB integration."""
from pathlib import Path
from typing import Optional, Union, List, Dict
from datetime import datetime
import duckdb
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa


class DeltaStorage:
    """Manages Delta Lake storage operations with DuckDB integration."""

    def __init__(self,
                 duckdb_conn: duckdb.DuckDBPyConnection,
                 delta_root: Optional[str] = None):
        """Initialize DeltaStorage instance."""
        self.conn = duckdb_conn
        self.delta_root = Path(delta_root or 'delta-tables')
        self.delta_root.mkdir(parents=True, exist_ok=True)

    def create_table(
            self,
            table_name: str,
            delta_path: str,
            partition_by: Optional[List[str]] = None,
            schema: Optional[pa.Schema] = None,
            mode: str = "overwrite"
    ) -> None:
        """Create a Delta table from a DuckDB table.

        Args:
            table_name: Name of the source table in DuckDB
            delta_path: Path where the Delta table will be created
            partition_by: List of columns to partition by
            schema: PyArrow schema for the table
            mode: Write mode ('overwrite' or 'append')
        """
        _path = self.delta_root / delta_path

        table = self.conn.execute(f'SELECT * FROM "{table_name}"').arrow()

        if schema:
            table = table.cast(schema)

        write_deltalake(
            str(_path),
            table,
            mode=mode,
            partition_by=partition_by
        )

    def read_to_duckdb(
        self,
        delta_path: str,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[Union[str, datetime]] = None
    ) -> None:
        """Read a Delta table into DuckDB."""
        _path = self.delta_root / delta_path

        try:
            dt_args = {"table_uri": str(_path)}
            if version is not None:
                dt_args["version"] = version
            elif timestamp is not None:
                dt_args["timestamp"] = timestamp

            dt = DeltaTable(**dt_args)

            files = dt.files()
            if not files:
                raise RuntimeError("No files found in Delta table")

            first_file = str(_path / files[0])
            self.conn.execute(f'''
                CREATE OR REPLACE TABLE "{table_name}" AS
                SELECT * FROM parquet_scan(?)
            ''', [first_file])

            if len(files) > 1:
                for file in files[1:]:
                    file_path = str(_path / file)
                    self.conn.execute(f'''
                        INSERT INTO "{table_name}"
                        SELECT * FROM parquet_scan(?)
                    ''', [file_path])

        except Exception as e:
            raise RuntimeError(f"Error reading Delta table: {str(e)}") from e

    def get_table_info(self, delta_path: str) -> Dict:
        """Get information about a Delta table."""
        _path = self.delta_root / delta_path
        dt = DeltaTable(str(_path))

        import json
        schema_str = json.loads(dt.schema().to_json())

        return {
            'version': dt.version(),
            'metadata': dt.metadata(),
            'files': dt.files(),
            'history': dt.history(),
            'schema': schema_str
        }

    def vacuum(
            self,
            delta_path: str,
            retention: Optional[int] = 168
    ) -> None:
        """Clean up old versions of a Delta table."""
        _path = self.delta_root / delta_path
        dt = DeltaTable(str(_path))

        if retention is not None and retention < 168:
            retention = 168 # 7 days

        dt.vacuum(retention_hours=retention)

    def optimize(
            self,
            delta_path: str,
            zorder_by: Optional[List[str]] = None
    ) -> None:
        """Optimize a Delta table."""
        _path = self.delta_root / delta_path
        dt = DeltaTable(str(_path))

        if zorder_by:
            dt.optimize.compact()
            dt.optimize.z_order(zorder_by)
        else:
            dt.optimize.compact()
