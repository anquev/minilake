"""Factory for creating storage instances."""

from minilake.config import Config
from minilake.core.connection import get_connection
from minilake.core.exceptions import ConfigurationError
from minilake.storage.base import StorageInterface
from minilake.storage.local import LocalDeltaStorage
from minilake.storage.s3 import S3DeltaStorage


def create_storage(config: Config | None = None) -> StorageInterface:
    """Create a storage instance based on configuration.

    Args:
        config: MiniLake configuration

    Returns:
        Storage implementation instance

    Raises:
        ConfigurationError: WHen configuration is invalid
    """
    if config is None:
        config = Config.from_env()

    conn = get_connection(config.database.path)

    if config.storage.type == "local":
        return LocalDeltaStorage(conn=conn, delta_root=config.storage.delta_root)
    elif config.storage.type == "s3":
        if not config.s3.is_configured:
            raise ConfigurationError(
                "S3 storage type selected but configuration is incomplete"
            )

        return S3DeltaStorage(
            conn=conn,
            endpoint=config.s3.endpoint,
            access_key=config.s3.access_key,
            secret_key=config.s3.secret_key,
            bucket=config.s3.bucket,
            delta_root=config.storage.delta_root,
            region=config.s3.region,
        )
    else:
        raise ConfigurationError(f"Unknown storage type: {config.storage.type}")
