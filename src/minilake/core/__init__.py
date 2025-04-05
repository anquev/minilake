import io

import pandas as pd

from .connection import MinilakeConnection
from .exceptions import MinilakeConnectionError


class MinilakeCore:
    def __init__(self):
        self.connection = MinilakeConnection()

    def list_s3_folders(self) -> list[str]:
        """Get list of available S3 folders."""
        return self.connection.list_s3_folders()

    def list_tables(self, folder: str) -> list[str]:
        """List all tables in the specified folder.

        Args:
            folder: The S3 folder/prefix to list tables from

        Returns:
            List[str]: List of table names in the folder
        """
        try:
            result = self.connection.s3_client.list_objects_v2(
                Bucket=self.connection.bucket,
                Prefix=f"{folder}/",
                Delimiter="/",
            )

            tables = []
            if "Contents" in result:
                tables = [
                    obj["Key"].split("/")[-1].replace(".parquet", "")
                    for obj in result["Contents"]
                    if obj["Key"].endswith(".parquet")
                ]

            return sorted(tables) if tables else ["No tables found"]

        except Exception as err:
            raise MinilakeConnectionError(
                f"Failed to list tables in folder '{folder}': {err!s}"
            ) from err

    def get_table_preview(self, folder: str, table: str) -> pd.DataFrame:
        """Get a preview of the table data.

        Args:
            folder: The S3 folder/prefix containing the table
            table: The name of the table to preview

        Returns:
            pd.DataFrame: DataFrame containing the first 10 rows of the table
        """
        try:
            s3_path = f"{folder}/{table}.parquet"

            response = self.connection.s3_client.get_object(
                Bucket=self.connection.bucket, Key=s3_path
            )

            parquet_data = response["Body"].read()
            df = pd.read_parquet(io.BytesIO(parquet_data))

            return df.head(10)

        except Exception as err:
            raise MinilakeConnectionError(
                f"Failed to preview table '{table}' in folder '{folder}': {err!s}"
            ) from err
