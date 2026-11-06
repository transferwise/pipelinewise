"""Align MariaDB/MySQL FLOAT equality conditions with exact FLOAT extraction."""

import math
import os
import stat
import tempfile
from typing import Dict, Optional, Set

from pipelinewise.fastsync.commons.source_transformations import float32_literal

from . import utils


def _is_float_column(column_metadata: Dict, column_schema) -> bool:
    """Match tap-mysql's promotion rule: discovery's datatype is FLOAT and the schema is a plain JSON number."""
    if column_metadata.get('datatype') != 'float' or not isinstance(column_schema, dict):
        return False
    types = column_schema.get('type')
    types = types if isinstance(types, list) else [types]
    return (
        set(types) in ({'number'}, {'null', 'number'})
        and 'format' not in column_schema
        and 'multipleOf' not in column_schema
    )


def float_columns_by_stream(catalog: Dict) -> Dict[str, Set[str]]:
    """Map every catalog stream ID to the lowercase names of its FLOAT columns.

    Args:
        catalog: Singer catalog (properties) produced by tap-mysql discovery.

    Returns:
        Stream ID to lowercase FLOAT column names; a stream without FLOAT columns maps to an empty set.
    """
    columns = {}
    streams = catalog.get('streams') if isinstance(catalog, dict) else None
    for stream in streams if isinstance(streams, list) else []:
        if not isinstance(stream, dict) or not isinstance(stream.get('tap_stream_id'), str):
            continue
        stream_columns = columns.setdefault(stream['tap_stream_id'], set())
        schema = stream.get('schema') if isinstance(stream.get('schema'), dict) else {}
        properties = schema.get('properties') if isinstance(schema.get('properties'), dict) else {}
        for entry in stream.get('metadata') or []:
            if not isinstance(entry, dict):
                continue
            breadcrumb = entry.get('breadcrumb') or []
            if (
                len(breadcrumb) == 2
                and breadcrumb[0] == 'properties'
                and isinstance(breadcrumb[1], str)
                and _is_float_column(entry.get('metadata') or {}, properties.get(breadcrumb[1]))
            ):
                stream_columns.add(breadcrumb[1].lower())
    return columns


def _stream_float_columns(columns_by_stream: Dict[str, Set[str]], tap_stream_name) -> Set[str]:
    """Return the FLOAT columns a rule's stream name refers to.

    transform-field matches the exact stream, FastSync-to-PostgreSQL any stream that differs only by case.
    """
    if not isinstance(tap_stream_name, str):
        return set()
    if tap_stream_name in columns_by_stream:
        return columns_by_stream[tap_stream_name]
    folded = tap_stream_name.lower()
    return set().union(*(columns for stream_id, columns in columns_by_stream.items() if stream_id.lower() == folded))


def _rounded_equals(condition, float_columns: Set[str]) -> Optional[float]:
    """Return the FLOAT-rounded equality literal, or None when the condition keeps its value."""
    if not isinstance(condition, dict) or condition.get('field_path'):
        return None
    column = condition.get('column')
    # MariaDB/MySQL column names are case-insensitive, and FastSync-to-PostgreSQL lowercases them.
    if not isinstance(column, str) or column.lower() not in float_columns:
        return None
    value = condition.get('equals')
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    # NaN never equals itself, so it would look changed on every rewrite.
    if isinstance(value, float) and not math.isfinite(value):
        return None
    rounded = float32_literal(value)
    return None if rounded == value else rounded


def round_float_equals(transformation_config: Dict, catalog: Dict) -> bool:
    """Round numeric equality literals on FLOAT columns to the nearest FLOAT, in place.

    Exact FLOAT extraction emits values such as 0.30000001192092896, so Singer transform-field and
    FastSync-to-PostgreSQL post-load SQL need the same literal to keep matching a stored 0.3.

    Args:
        transformation_config: Parsed transformation.json content.
        catalog: Singer catalog used to identify FLOAT columns.

    Returns:
        True if any literal changed.
    """
    columns_by_stream = float_columns_by_stream(catalog)
    changed = False
    transformations = transformation_config.get('transformations') if isinstance(transformation_config, dict) else None
    for transformation in transformations if isinstance(transformations, list) else []:
        if not isinstance(transformation, dict) or not isinstance(transformation.get('when'), list):
            continue
        float_columns = _stream_float_columns(columns_by_stream, transformation.get('tap_stream_name'))
        for condition in transformation['when']:
            rounded = _rounded_equals(condition, float_columns)
            if rounded is not None:
                condition['equals'] = rounded
                changed = True
    return changed


def round_transformation_file(transformation_file: Optional[str], catalog: Dict) -> bool:
    """Rewrite a tap's transformation.json when FLOAT equality literals need rounding.

    Args:
        transformation_file: Path to the generated transformation.json; a missing file is skipped.
        catalog: Singer catalog whose metadata identifies FLOAT columns.

    Returns:
        True if the file was rewritten.
    """
    if not transformation_file:
        return False
    transformation_config = utils.load_json(transformation_file)
    if not round_float_equals(transformation_config, catalog):
        return False
    # Replace atomically so a concurrent reader never sees a partly written file.
    fd, temp_path = tempfile.mkstemp(prefix='.transformation.', suffix='.json',
                                     dir=os.path.dirname(os.path.abspath(transformation_file)))
    os.close(fd)
    try:
        os.chmod(temp_path, stat.S_IMODE(os.stat(transformation_file).st_mode))
        utils.save_json(transformation_config, temp_path)
        os.replace(temp_path, transformation_file)
    except BaseException:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise
    return True
