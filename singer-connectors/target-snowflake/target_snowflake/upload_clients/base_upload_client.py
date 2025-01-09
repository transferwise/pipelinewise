"""
Base class for upload clients
"""
from abc import ABC, abstractmethod
from singer import get_logger


class BaseUploadClient(ABC):
    """
    Abstract class for upload clients
    """
    def __init__(self, connection_config):
        self.connection_config = connection_config
        self.logger = get_logger('target_snowflake')

    @abstractmethod
    def upload_file(self, file: str, stream: str, temp_dir: str = None) -> None:
        """
        Upload file
        """

    @abstractmethod
    def delete_object(self, stream: str, key: str) -> None:
        """
        Delete object
        """

    @abstractmethod
    def copy_object(self, copy_source: str, target_bucket: str, target_key: str, target_metadata: dict) -> None:
        """
        Copy object
        """
