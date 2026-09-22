"""Enums used by pipelinewise-target-snowflake"""
import json
import re

from enum import Enum, unique
from types import ModuleType
from typing import Callable

from target_snowflake.exceptions import FileFormatNotFoundException, InvalidFileFormatException
from target_snowflake.file_formats import csv
from target_snowflake.file_formats.csv import REQUIRED_FILE_FORMAT_OPTIONS
from target_snowflake.managed_iceberg import sql_string_literal


# Supported types for file formats.
@unique
class FileFormatTypes(str, Enum):
    """Enum of supported file format types"""

    CSV = 'csv'


class FileFormat:
    """File Format class"""

    def __init__(self, file_format: str, query_fn: Callable, file_format_type: FileFormatTypes=None):
        """Initialize the formatter, discovering and validating its Snowflake format.

        An explicit file_format_type reuses startup validation and avoids repeating
        metadata discovery for each stream.
        """
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
            ModuleType implementation of the file formatter
        """
        if file_format_type == FileFormatTypes.CSV:
            return csv

        raise InvalidFileFormatException(f"Not supported file format: '{file_format_type}'")

    @staticmethod
    def _parse_file_format_name(file_format: str) -> list[str]:
        """Resolve quoted and unquoted components without splitting quoted dots."""
        identifier = r'(?:[A-Za-z_][A-Za-z0-9_$]*|"(?:[^"]|"")+")'
        public_schema = re.fullmatch(rf'\s*{identifier}\s*\.\s*\.\s*{identifier}\s*', file_format)
        if not public_schema and not re.fullmatch(
            rf'\s*{identifier}(?:\s*\.\s*{identifier}){{0,2}}\s*', file_format
        ):
            raise InvalidFileFormatException(f'Invalid named file format identifier: {file_format}')
        components = [
            part[1:-1].replace('""', '"') if part.startswith('"') else part.upper()
            for part in re.findall(identifier, file_format)
        ]
        if public_schema:
            components.insert(1, 'PUBLIC')
        return components

    @classmethod
    def _detect_file_format_type(cls, file_format: str, query_fn: Callable) -> FileFormatTypes:
        """Detect the type of an existing snowflake file format object

        Params:
            file_format: File format name
            query_fn: A callable function that can run SQL queries in an active Snowflake session

        Returns:
            FileFormatTypes enum item
        """
        components = cls._parse_file_format_name(file_format)
        file_format_name = components[-1]
        # SHOW's LIKE pattern consumes backslash escapes in addition to SQL literal escaping.
        pattern = sql_string_literal(file_format_name.replace('\\', '\\\\'))
        scope = '.'.join('"' + part.replace('"', '""') + '"' for part in components[:-1])
        scope_clause = f' {scope}' if scope else ''
        metadata = query_fn(f'SHOW FILE FORMATS LIKE {pattern} IN SCHEMA{scope_clause}')
        # LIKE is case-insensitive and treats underscores/percent signs as wildcards.
        file_formats_in_sf = [
            row for row in metadata
            if all(row.get(key) == value for key, value in zip(
                ('name', 'schema_name', 'database_name'), reversed(components)
            ))
        ]

        if len(file_formats_in_sf) == 1:
            file_format_metadata = file_formats_in_sf[0]
            try:
                file_format_type = FileFormatTypes(file_format_metadata['type'].lower())
            except ValueError as ex:
                raise InvalidFileFormatException(
                    f'Named file format {file_format} has unsupported type '
                    f"{file_format_metadata['type']!r}; target-snowflake supports only CSV staging"
                ) from ex

            cls._validate_csv_options(file_format, file_format_metadata)
        else:
            raise FileFormatNotFoundException(
                f"Named file format not found: {file_format}")

        return file_format_type

    @staticmethod
    def _validate_csv_options(file_format_name: str, file_format_metadata: dict) -> None:
        """Require the Snowflake CSV dialect emitted by the connector."""
        raw_options = next(
            (
                value
                for key, value in file_format_metadata.items()
                if key.lower() == 'format_options'
            ),
            None,
        )

        try:
            options = json.loads(raw_options) if isinstance(raw_options, str) else raw_options
        except json.JSONDecodeError as ex:
            raise InvalidFileFormatException(
                f"Named CSV file format {file_format_name} returned invalid format_options metadata"
            ) from ex

        if not isinstance(options, dict):
            raise InvalidFileFormatException(
                f"Named CSV file format {file_format_name} did not return format_options metadata"
            )

        normalized_options = {str(key).upper(): value for key, value in options.items()}
        mismatches = [
            f'{name}={normalized_options.get(name)!r} (expected {expected!r})'
            for name, expected in REQUIRED_FILE_FORMAT_OPTIONS.items()
            if normalized_options.get(name) != expected
        ]
        if mismatches:
            raise InvalidFileFormatException(
                f"Named CSV file format {file_format_name} is incompatible with target-snowflake: "
                f"{'; '.join(mismatches)}. Configure the named format with NULL_IF=(), "
                "ESCAPE='\\\\', FIELD_OPTIONALLY_ENCLOSED_BY='\"', MULTI_LINE=TRUE, "
                'EMPTY_FIELD_AS_NULL=TRUE, and the standard comma/LF delimiters.'
            )
