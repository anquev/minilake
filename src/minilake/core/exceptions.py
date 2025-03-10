"""Exceptions for MiniLake."""

class MiniLakeError(Exception):
    """Base exception for all MiniLake errors."""
    pass

class ConfigurationError(MiniLakeError):
    """Error in configuration."""
    pass

class ConnectionError(MiniLakeError):
    """Error in database connection."""
    pass

class StorageError(MiniLakeError):
    """Error in storage operations."""
    pass

class IngestionError(MiniLakeError):
    """Error during data ingestion."""
    pass

class QueryError(MiniLakeError):
    """Error during query execution."""
    pass