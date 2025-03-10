"""SQL Data retriever from S3."""
from typing import Optional, Union, List, Dict, Any
import pandas as pd
import polars as pl
from minilake.services.s3_manager import S3Manager

class QueryTable:
    """Querying Delta Lake tables using DuckDB SQL."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None
    ):
        """Initialize QueryTable with S3/MinIO credentials.

        Args:
            endpoint: S3/MinIO endpoint URL
            access_key: S3/MinIO access key
            secret_key: S3/MinIO secret key
            bucket: S3/MinIO bucket name
        """
        self.s3_manager = S3Manager(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket=bucket
        )

    def query(
        self,
        delta_path: str,
        sql_query: str,
        output_format: Union[pd.DataFrame, pl.DataFrame] = pd.DataFrame
        ) -> Union[pd.DataFrame, pl.DataFrame]:
        """Execute a SQL query against a Delta table.
        
        Args:
            delta_path: Path to the Delta table in S3
            sql_query: SQL query to execute (DuckDB syntax)
            output_format: Format of the output (pandas or polars)

            -> have to think about a better way to handle versions 

        Returns:
            Query results in the specified format
        """
        temp_table = f"temp_{delta_path.replace('/', '_')}"

        try:
            if output_format == pd.DataFrame:
                result = self.s3_manager.ingestion.conn.execute(sql_query).df()
            else:  # pl.DataFrame
                result = self.s3_manager.ingestion.conn.execute(sql_query).pl()
            return result

        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {str(e)}") from e

        finally:
            self.s3_manager.ingestion.conn.execute(
                f'DROP TABLE IF EXISTS "{temp_table}"'
            )

    def get_table_metadata(self, delta_path: str) -> Dict[str, Any]:
        """Get metadata about a Delta table.

        Args:
            delta_path: Path to the Delta table in S3

        Returns:
            Dictionary containing table metadata including schema, partitioning, etc.
        """
        return self.s3_manager.get_delta_info(delta_path)

    def list_columns(self, delta_path: str) -> List[str]:
        """Get list of columns in a Delta table.

        Args:
            delta_path: Path to the Delta table in S3

        Returns:
            List of column names
        """
        metadata = self.get_table_metadata(delta_path)
        return [field["name"] for field in metadata["schema"]["fields"]]

    def sample_data(
        self,
        delta_path: str,
        n: int = 10,
        seed: Optional[int] = None
    ) -> pd.DataFrame:
        """Get a sample of rows from a Delta table.

        Args:
            delta_path: Path to the Delta table in S3
            n: Number of rows to sample
            seed: Random seed for reproducible sampling

        Returns:
            Pandas DataFrame containing sampled rows
        """
        _sampler = f"USING SAMPLE {seed}" if seed is not None else ""
        sql = f"SELECT * FROM delta_table USING SAMPLE {n} ROWS {_sampler}"
        return self.query(delta_path, sql)

    def close(self):
        """Close connection."""
        self.s3_manager.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
