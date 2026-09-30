"""Exact decimal schemas and values shared by PipelineWise components."""

from decimal import Decimal, InvalidOperation
import math
import re


_DECIMAL_TEXT = re.compile(r'^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$')


def _schema_dict(schema):
    return schema.to_dict() if hasattr(schema, 'to_dict') else schema


def is_decimal_schema(schema):
    """Distinguish declared decimals from legacy decimal-formatted strings."""
    schema = _schema_dict(schema)
    return isinstance(schema, dict) and schema.get('format') == 'singer.decimal' and \
        isinstance(schema.get('decimal'), dict)


def _dimensions(schema):
    schema = _schema_dict(schema)
    if not is_decimal_schema(schema):
        raise ValueError('Expected an explicit decimal schema')
    types = schema.get('type', [])
    if isinstance(types, str):
        types = [types]
    if 'string' not in types or any(item not in ('null', 'string') for item in types):
        raise ValueError('Decimal schemas must use string transport')
    dimensions = schema['decimal']
    if set(dimensions) != {'precision', 'scale'}:
        raise ValueError('Decimal schemas require precision and scale')
    precision, scale = dimensions['precision'], dimensions['scale']
    if precision is None and scale is None:
        return precision, scale
    if type(precision) is not int or type(scale) is not int or precision < 1:
        raise ValueError('Decimal precision and scale must be integers, or both null')
    return precision, scale


def decimal_schema(precision, scale, target=None):
    """Create a nullable decimal schema without encoding numeric values as floats."""
    schema = {
        'type': ['null', 'string'],
        'format': 'singer.decimal',
        'decimal': {'precision': precision, 'scale': scale},
    }
    _dimensions(schema)
    if target is not None:
        decimal_sql_type(schema, target)
    return schema


def decimal_sql_type(schema, target, postgres_version=None, is_key=False, source=None):
    """Choose a stable numeric type from source dimensions and target capabilities."""
    precision, scale = _dimensions(schema)
    if target in ('snowflake', 'target-snowflake'):
        if is_key and source == 'postgres':
            return 'VARCHAR(134217728)'
        if precision is not None:
            precision, scale = max(precision - min(scale, 0), scale), max(scale, 0)
        if precision is None or precision > 38 or scale > 37:
            return 'VARCHAR(134217728)' if is_key else 'FLOAT'
    elif target in ('postgres', 'postgresql', 'target-postgres'):
        if precision is None or precision > 1000 or not -1000 <= scale <= 1000:
            return 'NUMERIC'
        if postgres_version is not None and postgres_version < 150000 and not 0 <= scale <= precision:
            return 'NUMERIC'
    else:
        raise ValueError(f'Unsupported decimal target: {target}')
    return f'NUMERIC({precision},{scale})'


def postgres_numeric_scale(scale):
    """Decode PostgreSQL's unsigned information_schema representation of signed scale."""
    if scale is None or scale < 0:
        return scale
    return ((int(scale) & 2047) ^ 1024) - 1024


def schema_has_decimals(schema):
    """Inspect a schema once so ordinary records avoid decimal validation walks."""
    schema = _schema_dict(schema)
    if not isinstance(schema, dict):
        return False
    if is_decimal_schema(schema):
        return True
    children = list(schema.get('properties', {}).values())
    items = schema.get('items')
    if items is not None:
        children.extend(items if isinstance(items, list) else [items])
    for keyword in ('allOf', 'anyOf', 'oneOf'):
        children.extend(schema.get(keyword, []))
    return any(schema_has_decimals(child) for child in children)


def decimal_bookmark(value, schema=None):
    """Replay below a legacy float boundary; acknowledge exact strings after loading."""
    if isinstance(value, float):
        previous = math.nextafter(value, -math.inf)
        if not math.isfinite(value) or not math.isfinite(previous):
            return None
        return str(Decimal.from_float(previous))
    if value is not None and not Decimal(decimal_to_string(value, decimal_schema(None, None))).is_finite():
        return None
    return decimal_to_string(value, schema)


def decimal_sort_key(value):
    """Match PostgreSQL numeric ordering, where NaN sorts above positive infinity."""
    number = Decimal(decimal_to_string(value, decimal_schema(None, None)))
    return (number.is_nan(), Decimal(0) if number.is_nan() else number)


def decimal_canonical_string(value):
    """Use lossless plain text for decimal identities that cannot fit target numbers."""
    if value is None:
        return None
    number = Decimal(decimal_to_string(value, decimal_schema(None, None)))
    if number.is_zero():
        return '0'
    text = format(number, 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text


def snowflake_float_expression(expression):
    """Retain rows by saturating numeric overflow instead of producing NULL or failing."""
    limit = '1.7976931348623157e308'
    converted = f'TRY_TO_DOUBLE({expression})'
    return (
        f'CASE WHEN {expression} IS NULL THEN NULL '
        f"WHEN {expression} IN ('NaN', 'Infinity', '-Infinity', '+Infinity') THEN {converted} "
        f'WHEN {converted} > {limit} THEN {limit} WHEN {converted} < -{limit} THEN -{limit} '
        f"ELSE IFNULL({converted}, IFF(SUBSTR({expression}, 1, 1) = '-', -{limit}, {limit})) END"
    )


def _validate_value(value, schema):
    precision, scale = _dimensions(schema)
    if value.is_nan():
        return
    if not value.is_finite():
        if precision is not None:
            raise ValueError('Bounded decimals require finite values')
        return
    if precision is None or value.is_zero():
        return
    _, digits, exponent = value.as_tuple()
    digits = list(digits)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    if exponent < -scale or len(digits) + exponent > precision - scale:
        raise ValueError(f'Decimal value does not fit NUMERIC({precision},{scale}) exactly')


def decimal_to_string(value, schema=None):
    """Preserve exact decimal digits; never accept an already-rounded float."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, str)):
        raise ValueError('Decimal values must be Decimal, integer or decimal text')
    text = str(value)
    try:
        numeric = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError('Invalid decimal text') from exc
    if numeric.is_finite() and not _DECIMAL_TEXT.fullmatch(text):
        raise ValueError('Invalid decimal text')
    if not numeric.is_finite() and text not in ('NaN', 'Infinity', '-Infinity', '+Infinity'):
        raise ValueError('Unsupported nonfinite decimal text')
    if schema is not None:
        _validate_value(numeric, schema)
    elif not numeric.is_finite():
        raise ValueError('Nonfinite decimals require an explicit unbounded schema')
    return text


def validate_decimal_record(record, schema):
    """Validate declared decimal fields recursively without decimal arithmetic."""
    schema = _schema_dict(schema)
    if not isinstance(schema, dict):
        return
    if is_decimal_schema(schema):
        if record is not None and not isinstance(record, str):
            raise ValueError('Decimal records must carry decimal strings')
        decimal_to_string(record, schema)
    elif isinstance(record, dict):
        for name, child_schema in schema.get('properties', {}).items():
            if name in record:
                validate_decimal_record(record[name], child_schema)
    elif isinstance(record, list):
        for item in record:
            validate_decimal_record(item, schema.get('items', {}))


def decimal_key(value):
    """Canonicalize equal decimal keys without using context-limited arithmetic."""
    if value is None:
        return None
    numeric = Decimal(decimal_to_string(value, decimal_schema(None, None)))
    if not numeric.is_finite():
        return str(numeric)
    if numeric.is_zero():
        return '0'
    sign, digits, exponent = numeric.as_tuple()
    digits = list(digits)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    coefficient = ''.join(str(digit) for digit in digits)
    return f'{"-" if sign else ""}{coefficient}E{exponent}'
