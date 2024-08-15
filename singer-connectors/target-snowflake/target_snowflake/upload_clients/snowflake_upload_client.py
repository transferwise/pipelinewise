"""
Snowflake Upload Client
"""
import os

from .base_upload_client import BaseUploadClient


class SnowflakeUploadClient(BaseUploadClient):
    """Snowflake Upload Client class"""

    def __init__(self, connection_config, dblink):
        super().__init__(connection_config)
        self.dblink = dblink

    def upload_file(self, file, stream, temp_dir = None):
        """Upload file to an internal snowflake stage"""
        # Generating key in S3 bucket
        key = os.path.basename(file)
        normfile = os.path.normpath(file).replace('\\', '/')

        compression = '' if self.connection_config.get('no_compression', '') else "SOURCE_COMPRESSION=GZIP"
        stage = self.dblink.get_stage_name(stream)

        self.logger.info('Target internal stage: %s, local file: %s, key: %s', stage, normfile, key)
        cmd = f"PUT 'file://{normfile}' '@{stage}' {compression}"
        self.logger.info(cmd)

        with self.dblink.open_connection() as connection:
            connection.cursor().execute(cmd)

        return key

    def delete_object(self, stream: str, key: str) -> None:
        """Delete object form internal snowflake stage"""
        self.logger.info('Deleting %s from internal snowflake stage', key)
        stage = self.dblink.get_stage_name(stream)

        with self.dblink.open_connection() as connection:
            connection.cursor().execute(f"REMOVE '@{stage}/{key}'")

    def copy_object(self, copy_source: str, target_bucket: str, target_key: str, target_metadata: dict) -> None:
        raise NotImplementedError(
            "Copying objects is not supported with a Snowflake upload client.")
