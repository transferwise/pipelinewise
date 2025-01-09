"""Enums used by pipelinewise-target-snowflake"""
from enum import Enum, unique
from types import ModuleType
from typing import Callable

import target_snowflake.file_formats
from target_snowflake.exceptions import FileFormatNotFoundException, InvalidFileFormatException

# Supported types for file formats.
@unique
class FileFormatTypes(str, Enum):
    """Enum of supported file format types"""

    CSV = 'csv'
    PARQUET = 'parquet'

    @staticmethod
    def list():
        """List of supported file type values"""
        return list(map(lambda c: c.value, FileFormatTypes))


# pylint: disable=too-few-public-methods
class FileFormat:
    """File Format class"""

    def __init__(self, file_format: str, query_fn: Callable, file_format_type: FileFormatTypes=None):
        """Find the file format in Snowflake, detect its type and
        initialise file format specific functions"""
        if file_format_type:
            self.file_format_type = file_format_type
        else:
            # Detect file format type by querying it from Snowflake
            self.file_format_type = self._detect_file_format_type(file_format, query_fn)

        self.formatter = self._get_formatter(self.file_format_type)

    @classmethod
    def _get_formatter(cls, file_format_type: FileFormatTypes) -> ModuleType:
        """Get the corresponding file formatter implementation based
        on the FileFormatType parameter

        Params:
            file_format_type: FileFormatTypes enum item

        Returns:
            ModuleType implementation of the file ormatter
        """
        formatter = None

        if file_format_type == FileFormatTypes.CSV:
            formatter = target_snowflake.file_formats.csv
        elif file_format_type == FileFormatTypes.PARQUET:
            formatter = target_snowflake.file_formats.parquet
        else:
            raise InvalidFileFormatException(f"Not supported file format: '{file_format_type}")

        return formatter

    @classmethod
    def _detect_file_format_type(cls, file_format: str, query_fn: Callable) -> FileFormatTypes:
        """Detect the type of an existing snowflake file format object

        Params:
            file_format: File format name
            query_fn: A callable function that can run SQL queries in an active Snowflake session

        Returns:
            FileFormatTypes enum item
        """
        file_format_name = file_format.split('.')[-1]
        file_formats_in_sf = query_fn(f"SHOW FILE FORMATS LIKE '{file_format_name}'")

        if len(file_formats_in_sf) == 1:
            file_format = file_formats_in_sf[0]
            try:
                file_format_type = FileFormatTypes(file_format['type'].lower())
            except ValueError as ex:
                raise InvalidFileFormatException(
                    f"Not supported named file format {file_format_name}. Supported file formats: {FileFormatTypes}") \
                    from ex
        else:
            raise FileFormatNotFoundException(
                f"Named file format not found: {file_format}")

        return file_format_type
