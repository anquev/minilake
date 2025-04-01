from .connection import MinilakeConnection


class MinilakeCore:
    def __init__(self):
        self.connection = MinilakeConnection()

    def list_s3_folders(self) -> list[str]:
        """
        Get list of available S3 folders.

        Returns:
            List[str]: List of folder names
        """
        return self.connection.list_s3_folders()
