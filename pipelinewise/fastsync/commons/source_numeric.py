"""Preserve declared SQL decimal dimensions in bulk replication."""

import re
import logging

from singer.decimal_support import decimal_schema, decimal_sql_type, postgres_numeric_scale
from .partial_sync_boundary import PartialSyncBoundaryError


LOGGER = logging.getLogger(__name__)


def mysql_decimal_type(column_type, target, *, is_key=False, postgres_version=None):
    """Read MySQL's complete declaration, including unsigned/zerofill modifiers."""
    match = re.fullmatch(
        r'(?:decimal|numeric)\((\d+),(\d+)\)(?: unsigned)?(?: zerofill)?',
        re.sub(r'\s*,\s*', ',', str(column_type).strip().lower()),
    )
    if match is None:
        raise ValueError(f'Cannot determine decimal precision and scale from {column_type!r}')
    precision, scale = map(int, match.groups())
    return decimal_sql_type(
        decimal_schema(precision, scale), target,
        is_key=is_key, postgres_version=postgres_version, source='mysql',
    )


def postgres_decimal_type(precision, scale, target, *, is_key=False, postgres_version=None):
    """Use the same destination choice as Singer, including signed PostgreSQL scale."""
    return decimal_sql_type(
        decimal_schema(precision, postgres_numeric_scale(scale)), target,
        is_key=is_key, postgres_version=postgres_version, source='postgres',
    )


def decimal_text_expression(expression, dialect):
    """Canonicalize decimal text keys without stripping significant integer zeroes."""
    cast = f'CAST({expression} AS {"TEXT" if dialect == "postgres" else "CHAR"})'
    return (f"CASE WHEN POSITION('.' IN {cast}) > 0 "
            f"THEN TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM {cast})) ELSE {cast} END")


def warn_decimal_mapping(name, declaration, target_type):
    """Expose any compatible or fallback mapping without logging row values."""
    if re.sub(r'\s', '', declaration.upper()).replace('DECIMAL', 'NUMERIC') != target_type:
        LOGGER.warning('Column %s maps source %s to %s', name, declaration, target_type)


def require_exact_decimal_boundary(name, source_type):
    """FLOAT and text decimal fallbacks cannot preserve numeric range ordering."""
    if source_type in ('numeric', 'decimal'):
        raise PartialSyncBoundaryError(
            f'PartialSync decimal boundary column {name} cannot use a FLOAT or text fallback; run FullSync instead'
        )


def postgres_float_expression(expression):
    """Match target FLOAT saturation before a source-side transformation casts numeric."""
    numeric = f'({expression})'
    limit = '1.7976931348623157e308'
    return (
        f"CASE WHEN {numeric}::text IN ('NaN', 'Infinity', '-Infinity') THEN {numeric}::double precision "
        f'WHEN {numeric} > {limit}::numeric THEN {limit}::double precision '
        f'WHEN {numeric} < -{limit}::numeric THEN -{limit}::double precision '
        f'WHEN abs({numeric}) < 2.4703282292062328e-324::numeric THEN 0::double precision '
        f'ELSE {numeric}::double precision END'
    )
