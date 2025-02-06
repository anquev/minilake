"""DeltaStorage module for managing Delta Lake operations with DuckDB integration."""
from pathlib import Path
from typing import Optional, Union, List, Dict
import duckdb
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
from datetime import datetime


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
            mode: str = "overwrite",
            schema: Optional[pa.Schema] = None,
            properties: Optional[Dict[str, str]] = None
    ) -> None:
        """Create a Delta table from a DuckDB table."""
        _path = self.delta_root / delta_path
        
        if properties and properties.get("delta.appendOnly") == "true" and mode != "append":
            mode = "append"
        
        table = self.conn.execute(f'SELECT * FROM "{table_name}"').arrow()
        
        if schema:
            table = table.cast(schema)
        
        write_deltalake(
            str(_path),
            table,
            mode=mode,
            partition_by=partition_by
        )

    def read_to_duckdb(self, 
                    delta_path: str, 
                    table_name: str, 
                    version: Optional[int] = None,
                    timestamp: Optional[Union[str, datetime]] = None
                    ) -> None:
        try:
            dt = DeltaTable(str(self.delta_root / delta_path))
            
            table = dt.scan().to_pyarrow()
                
            self.conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM arrow_table($1)', [table])
        except Exception as e:
            raise RuntimeError(f"Error reading Delta table: {str(e)}") from e

    def get_table_info(self, delta_path: str) -> Dict:
        """Get information about a Delta table."""
        _path = self.delta_root / delta_path
        dt = DeltaTable(str(_path))
        
        schema_str = dt.schema().json()
        
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
            retention = 168
            
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
