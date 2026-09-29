"""Shared Snowflake type definitions for FastSync."""

import re


SNOWFLAKE_MAX_VARCHAR_LENGTH = 134217728
SNOWFLAKE_MAX_VARCHAR = f'VARCHAR({SNOWFLAKE_MAX_VARCHAR_LENGTH})'

_NATIVE_TYPE_ALIASES = {
    **dict.fromkeys(
        ('DECIMAL', 'NUMERIC', 'FIXED', 'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT'), 'NUMBER',
    ),
    **dict.fromkeys(('CHAR', 'CHARACTER', 'STRING', 'TEXT', 'CHARACTER VARYING'), 'VARCHAR'),
    **dict.fromkeys(('DOUBLE', 'DOUBLE PRECISION', 'REAL', 'FLOAT4', 'FLOAT8'), 'FLOAT'),
    'VARBINARY': 'BINARY',
    'BOOL': 'BOOLEAN',
    'DATETIME': 'TIMESTAMP_NTZ',
    'TIMESTAMP': 'TIMESTAMP_NTZ',
}
_NATIVE_TYPE_DEFAULTS = {
    'NUMBER': (38, 0),
    'VARCHAR': (16777216,),
    'BINARY': (8388608,),
    'TIME': (9,),
    'TIMESTAMP_NTZ': (9,),
    'TIMESTAMP_LTZ': (9,),
    'TIMESTAMP_TZ': (9,),
}


def canonical_native_type(data_type: str) -> str:
    """Normalize Snowflake SQL aliases while retaining native width and precision."""
    if not isinstance(data_type, str):
        raise ValueError('Snowflake column type must be a string')
    normalized = re.sub(r'\s+', ' ', data_type.strip().upper())
    match = re.fullmatch(r'([A-Z][A-Z_ ]*?)(?:\s*\((\d+(?:\s*,\s*\d+)*)\))?', normalized)
    if match is None:
        raise ValueError(f'Invalid Snowflake column type: {data_type!r}')
    declared_base, parameters = match.groups()
    base = _NATIVE_TYPE_ALIASES.get(declared_base, declared_base)
    if base not in _NATIVE_TYPE_DEFAULTS and base not in ('FLOAT', 'BOOLEAN', 'DATE', 'VARIANT', 'OBJECT', 'ARRAY'):
        raise ValueError(f'Unsupported Snowflake column type: {data_type!r}')
    values = tuple(int(value) for value in parameters.split(',')) if parameters else ()
    if not values and declared_base in ('CHAR', 'CHARACTER'):
        values = (1,)
    values = values or _NATIVE_TYPE_DEFAULTS.get(base, ())
    if base == 'NUMBER' and len(values) == 1:
        values += (0,)
    _validate_native_type_parameters(base, values)
    suffix = f'({",".join(str(value) for value in values)})' if values else ''
    return f'{base}{suffix}'


def _validate_native_type_parameters(base, values):
    if base == 'NUMBER':
        valid = len(values) == 2 and 1 <= values[0] <= 38 and 0 <= values[1] <= min(values[0], 37)
    elif base in ('VARCHAR', 'BINARY'):
        valid = len(values) == 1 and values[0] > 0
    elif base in ('TIME', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'TIMESTAMP_LTZ'):
        valid = len(values) == 1 and 0 <= values[0] <= 9
    else:
        valid = not values
    if not valid:
        raise ValueError(f'Invalid Snowflake {base} type parameters: {values!r}')


def canonical_native_metadata_type(metadata: dict) -> str:
    """Normalize SHOW COLUMNS type metadata without guessing omitted dimensions."""
    metadata = {key.lower(): value for key, value in metadata.items()}
    declared = metadata.get('type')
    canonical = canonical_native_type(declared)
    base = canonical.split('(', maxsplit=1)[0]
    dimension_keys = {
        'NUMBER': ('precision', 'scale'),
        'VARCHAR': ('length',),
        'BINARY': ('length',),
        'TIME': ('scale',),
        'TIMESTAMP_NTZ': ('scale',),
        'TIMESTAMP_LTZ': ('scale',),
        'TIMESTAMP_TZ': ('scale',),
    }.get(base, ())
    if not dimension_keys:
        return canonical
    values = tuple(metadata.get(key) for key in dimension_keys)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
        raise ValueError(f'Snowflake {base} metadata requires {", ".join(dimension_keys)}')
    return canonical_native_type(f'{base}({",".join(str(value) for value in values)})')
