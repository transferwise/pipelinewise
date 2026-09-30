"""Identify PipelineWise historical columns without accepting unrelated drift."""

from datetime import datetime, timezone
import re


_VERSION_SUFFIX = re.compile(r'_(?:\d{8}_\d{4}|\d{8}_\d{6}_\d{6})$')


def versioned_column_name(name):
    """Keep the complete version suffix within Snowflake's identifier limit."""
    suffix = datetime.now(timezone.utc).strftime('_%Y%m%d_%H%M%S_%f')
    return name[:255 - len(suffix)] + suffix


def is_versioned_column(name, source_names):
    """Recognize an archive only when its original column remains in the source."""
    match = _VERSION_SUFFIX.search(name)
    if match is None:
        return False
    suffix_size = len(match.group())
    return any(name[:match.start()] == original[:255 - suffix_size] for original in source_names)


def has_column_versions(column_name, source_names, actual_names):
    """Exclude real source columns when identifying a column's historical data."""
    return any(
        name not in source_names and is_versioned_column(name, (column_name,))
        for name in actual_names
    )


def is_numeric_version_change(actual_type, expected_type):
    """Limit automatic bulk versioning to decimal evolution on numeric columns."""
    numeric = ('NUMBER(', 'NUMERIC(', 'DECIMAL(')
    return (
        (expected_type.startswith(numeric) or expected_type in ('FLOAT', 'DOUBLE'))
        and (actual_type.startswith(numeric) or actual_type in ('FLOAT', 'DOUBLE'))
        and actual_type != expected_type
    )
