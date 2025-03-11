"""Local filesystem storage implementation for Delta Lake."""
from pathlib import Path
from typing import List

from minilake.storage.delta import DeltaStorage
from minilake.core.exceptions import StorageError

class LocalDeltaStorage(DeltaStorage):
    """Delta Lake storage using local filesystem."""

    def __init__(self, conn, delta_root: str = "delta-tables"):
        """Initialize local Delta storage.
        
        Args:
            conn: DuckDB connection
            delta_root: Root directory for Delta tables
        """
        super().__init__(conn)
        self.delta_root = Path(delta_root)
        self.delta_root.mkdir(parents=True, exist_ok=True)

    def _get_delta_path(self, delta_path: str) -> Path:
        """Get the full path to a Delta table."""
        return self.delta_root / delta_path

    def _load_delta_files(self, files: List[str], delta_path: Path, table_name: str) -> None:
        """Load Delta table files into DuckDB."""
        try:
            file_paths = []
            for file in files:
                file_paths.append(str(delta_path / file))

            # Create table from first file
            create_query = f'''
                CREATE OR REPLACE TABLE "{table_name}" AS
                SELECT * FROM parquet_scan('{file_paths[0]}')
            '''
            self.conn.execute(create_query)

            # Add data from other files
            if len(file_paths) > 1:
                for file_path in file_paths[1:]:
                    insert_query = f'''
                        INSERT INTO "{table_name}"
                        SELECT * FROM parquet_scan('{file_path}')
                    '''
                    self.conn.execute(insert_query)

        except Exception as e:
            raise StorageError(f"Error loading Delta files into DuckDB: {str(e)}") from e
