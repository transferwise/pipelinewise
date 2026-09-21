"""Compile Snowflake FastSync transformations into source-only projections."""

import math
import re

from .transform_utils import TransformationType
from .snowflake_types import canonical_native_type
from .snowflake_iceberg_versions import managed_iceberg_version_spec


class UnsupportedSourceTransformation(ValueError):
    """A transformation cannot be reproduced safely by the source engine."""


_NULLABLE_TYPES = {
    'VARCHAR', 'NUMBER', 'FLOAT', 'BOOLEAN', 'DATE', 'TIME',
    'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'BINARY', 'VARIANT', 'ARRAY', 'OBJECT',
}
_SUPPORTED_TYPES = {
    'SET-NULL': _NULLABLE_TYPES,
    'MASK-NUMBER': {'NUMBER', 'FLOAT'},
    'MASK-DATE': {'DATE', 'TIMESTAMP_NTZ'},
    'MASK-HIDDEN': {'VARCHAR'},
    'HASH': {'VARCHAR'},
    **{f'HASH-SKIP-FIRST-{count}': {'VARCHAR'} for count in range(1, 10)},
    **{f'MASK-STRING-SKIP-ENDS-{count}': {'VARCHAR'} for count in range(1, 10)},
}
_INTEGER_SOURCE_TYPES = {
    'smallint', 'integer', 'bigint', 'int', 'tinyint', 'mediumint', 'smallserial', 'serial', 'bigserial',
    'bit varying', 'varbit',
}


def _identifier(name, dialect):
    if not isinstance(name, str) or not name or '\0' in name:
        raise UnsupportedSourceTransformation('Source transformation identifiers must be non-empty strings')
    quote = '"' if dialect == 'postgres' else '`'
    return quote + name.replace(quote, quote * 2) + quote


def _literal(value, dialect):
    encoded = value.encode('utf-8').hex()
    if '\0' in value:
        raise UnsupportedSourceTransformation('NUL is unsupported in transformation conditions')
    if dialect == 'postgres':
        return f"convert_from(decode('{encoded}', 'hex'), 'UTF8')"
    return f"CONVERT(X'{encoded}' USING utf8mb4)"


def _kind(column):
    base = column['target_type'].split('(', 1)[0]
    return {
        'VARCHAR': 'text', 'NUMBER': 'number', 'FLOAT': 'number',
        'DATE': 'date', 'TIMESTAMP_NTZ': 'date', 'BOOLEAN': 'boolean',
    }.get(base, base.lower())


def _mapped_column(column, iceberg_version):
    """Use the DDL mapping, never infer a destination type from a transformation."""
    name = column['column_name']
    if not isinstance(column.get('data_type'), str) or not column['data_type']:
        raise UnsupportedSourceTransformation(f'Missing source type for column {name!r}')
    try:
        target_type = column.get('target_type')
        if iceberg_version is not None:
            target_type = managed_iceberg_version_spec(iceberg_version).canonical_fastsync_type(target_type)
        # Normalize aliases such as Iceberg DOUBLE to the same operation family.
        target_type = canonical_native_type(target_type)
    except (ValueError, TypeError) as exc:
        raise UnsupportedSourceTransformation(
            f'Unsupported mapped target type {column.get("target_type")!r} for column {name!r}'
        ) from exc
    return dict(column, target_type=target_type)


def _unalias(expression, name, dialect):
    """Remove only the metadata projection's top-level, matching alias."""
    if not isinstance(expression, str) or not expression.strip():
        raise UnsupportedSourceTransformation(f'Missing source expression for column {name!r}')
    depth, quoted, index = 0, None, 0
    while index < len(expression):
        char = expression[index]
        if quoted:
            if char == quoted:
                if expression[index:index + 2] == quoted * 2:
                    index += 2
                    continue
                quoted = None
            elif char == '\\' and quoted == "'":
                index += 1
        elif char in {'"', "'", '`'}:
            quoted = char
        elif char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
        elif depth == 0 and expression[index:index + 4].upper() == ' AS ':
            alias = expression[index + 4:].strip()
            if alias not in {name, _identifier(name, dialect)}:
                raise UnsupportedSourceTransformation(f'Unexpected source alias for column {name!r}')
            return expression[:index].strip()
        index += 1
    if quoted or depth:
        raise UnsupportedSourceTransformation(f'Malformed source expression for column {name!r}')
    return expression.strip()


def _base_expression(column, dialect):
    name = column['column_name']
    expression = _unalias(column.get('safe_sql_value'), name, dialect)
    kind = _kind(column)
    if kind == 'text':
        if dialect != 'postgres':
            return f'CONVERT(({expression}) USING utf8mb4)'
        return f"CASE WHEN ({expression}) IS NOT DISTINCT FROM NULL THEN NULL ELSE format('%s', {expression}) END"
    if kind == 'date':
        cast = 'DATE' if column['target_type'] == 'DATE' else (
            'timestamp' if dialect == 'postgres' else 'DATETIME(6)'
        )
        return f'CAST(({expression}) AS {cast})'
    if kind == 'boolean' and dialect == 'postgres' and column['data_type'].lower() == 'bit':
        if column.get('character_maximum_length') != 1:
            raise UnsupportedSourceTransformation(f'Only one-bit Boolean columns are supported: {name!r}')
        return f'(CAST(({expression}) AS integer) <> 0)'
    if kind == 'number' and dialect == 'postgres' and column['data_type'].lower() in {'bit varying', 'varbit'}:
        # Snowflake reads the exported bit-string digits as decimal, not binary.
        return f'({expression})::text::numeric(38, 0)'
    if column.get('target_type', '').upper().split('(', 1)[0] in {'FLOAT', 'DOUBLE', 'REAL'}:
        return f'({expression})::text::double precision' if dialect == 'postgres' else (
            f'(CAST(({expression}) AS CHAR CHARACTER SET utf8mb4) + 0e0)'
        )
    return expression


def _validate_pattern(pattern):
    """Reject regex constructs whose source-engine meaning cannot be retained."""
    if not isinstance(pattern, str) or '\0' in pattern or '(?' in pattern or '[[' in pattern:
        raise UnsupportedSourceTransformation('Unsupported regex syntax in source transformation')
    if '\\' in pattern:
        raise UnsupportedSourceTransformation(
            'Backslash regex conditions cannot preserve legacy Snowflake literal semantics; '
            'use character classes such as [.] or literal control characters'
        )
    if re.search(r'\[\^?\]', pattern):
        raise UnsupportedSourceTransformation('Literal closing brackets in regex character classes are unsupported')
    if re.search(r'\[[^]]*(?:&&|--)', pattern):
        raise UnsupportedSourceTransformation('Regex character-class set operators are unsupported')
    if pattern.startswith('^'):
        pattern = pattern[1:]
    if pattern.endswith('$'):
        pattern = pattern[:-1]
    if re.search(r'[*+?}][?+]', pattern):
        raise UnsupportedSourceTransformation('Non-greedy and possessive regex repeats are unsupported')
    for repeat in re.findall(r'\{([^{}]*)\}', pattern):
        if not re.fullmatch(r'\d+(?:,\d*)?', repeat) or any(int(n) > 255 for n in repeat.split(',') if n):
            raise UnsupportedSourceTransformation('Regex repeat bounds must be integers no greater than 255')
    try:
        re.compile(pattern)
    except re.error as exc:
        raise UnsupportedSourceTransformation('Invalid regex in source transformation') from exc
    return pattern


def _portable_pattern(pattern, dialect):
    """Accept a common regex subset with explicit Snowflake match boundaries."""
    pattern = _validate_pattern(pattern)
    output, in_class, index = [], False, 0
    while index < len(pattern):
        char = pattern[index]
        if char == '[':
            if in_class:
                raise UnsupportedSourceTransformation('Unsupported regex character class')
            in_class = True
            output.append(char)
        elif char == ']':
            in_class = False
            output.append(char)
        elif char == '^' and in_class and pattern[index - 1] == '[':
            output.append(char)
        elif char in '^$':
            raise UnsupportedSourceTransformation('Regex anchors are supported only at pattern boundaries')
        elif char == '.' and not in_class:
            output.append('[^\n]')
        else:
            output.append(char)
        index += 1
    end = r'\Z' if dialect == 'postgres' else r'\z'
    flags = '' if dialect == 'postgres' else '(?-x)'
    return flags + r'\A(?:' + ''.join(output) + ')' + end


def _resolve(name, by_name):
    if not isinstance(name, str) or name.upper() not in by_name:
        raise UnsupportedSourceTransformation(f'Unknown transformation column {name!r}')
    return by_name[name.upper()]


def _condition(condition, columns, dialect):
    column = _resolve(condition.get('column'), columns)
    expression = _identifier(column['column_name'], dialect)
    if 'equals' in condition:
        return _equals(expression, column, condition['equals'], dialect)
    if _kind(column) == 'number' and column['data_type'].lower() in _INTEGER_SOURCE_TYPES:
        expression = f'({expression})::text' if dialect == 'postgres' else (
            f'CAST(({expression} + 0) AS CHAR CHARACTER SET utf8mb4)'
        )
    elif _kind(column) != 'text':
        raise UnsupportedSourceTransformation('Regex conditions require a text column')
    pattern = _literal(_portable_pattern(condition['regex_match'], dialect), dialect)
    if dialect == 'postgres':
        return f'({expression} COLLATE "C" ~ {pattern})'
    return f'({expression} COLLATE utf8mb4_bin REGEXP {pattern})'


def _equals(expression, column, value, dialect):
    if value is None:
        return f'({expression} IS NULL)'
    kind = _kind(column)
    if kind == 'text' and isinstance(value, str):
        literal = _literal(value, dialect)
        if dialect == 'postgres':
            return f'({expression} COLLATE "C" = {literal} COLLATE "C")'
        return f'(CAST({expression} AS BINARY) = CAST({literal} AS BINARY))'
    if kind == 'boolean' and isinstance(value, bool):
        return f'({expression} = {str(value).upper()})'
    if kind == 'number' and isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'({expression} = {value!r})'
    raise UnsupportedSourceTransformation(f'Unsupported equality type for column {column["column_name"]!r}')


def _hash(expression, dialect):
    if dialect == 'postgres':
        return f"encode(sha256(convert_to({expression}, 'UTF8')), 'hex')"
    return f'SHA2({expression}, 256)'


def _validate_transform(rule, column):
    """Require a supported operation whose output fits the existing mapped type."""
    try:
        transform = TransformationType(rule['type']).value
    except (ValueError, KeyError) as exc:
        raise UnsupportedSourceTransformation('Unsupported source transformation type') from exc
    if rule.get('field_paths') is not None:
        raise UnsupportedSourceTransformation('Only top-level transformations are supported')
    target_type = column['target_type']
    base = target_type.split('(', 1)[0]
    if base not in _SUPPORTED_TYPES[transform]:
        raise UnsupportedSourceTransformation(
            f'{transform} does not support mapped target type {target_type} '
            f'for column {column["column_name"]!r}'
        )
    if base == 'VARCHAR':
        required_width = 64 if transform == 'HASH' else 6 if transform == 'MASK-HIDDEN' else 0
        if transform.startswith('HASH-SKIP-FIRST-'):
            required_width = 64 + int(transform[-1])
        if int(target_type.partition('(')[2][:-1]) < required_width:
            raise UnsupportedSourceTransformation(
                f'{transform} output requires VARCHAR({required_width}), but column '
                f'{column["column_name"]!r} maps to {target_type}'
            )
    return transform


def _transform(rule, column, dialect):
    transform = _validate_transform(rule, column)
    expression = _identifier(column['column_name'], dialect)
    base = column['target_type'].split('(', 1)[0]
    if transform == 'SET-NULL':
        return 'NULL'
    kind = _kind(column)
    if transform == 'MASK-NUMBER' and kind == 'number':
        return '0'
    if transform == 'MASK-DATE' and kind == 'date':
        if base == 'DATE' and dialect == 'postgres':
            return f"CAST(date_trunc('year', {expression}) AS DATE)"
        if dialect == 'postgres':
            result = f"(date_trunc('year', {expression}) + ({expression}::time - TIME '00:00:00'))"
        else:
            result = f'DATE_SUB({expression}, INTERVAL (DAYOFYEAR({expression}) - 1) DAY)'
        return f'CAST({result} AS DATE)' if base == 'DATE' else result
    if kind != 'text':
        raise UnsupportedSourceTransformation(f'Unsupported transformation type for column {column["column_name"]!r}')
    if transform == 'MASK-HIDDEN':
        return _literal('hidden', dialect)
    if transform == 'HASH':
        return _hash(expression, dialect)
    if transform.startswith('HASH-SKIP-FIRST-'):
        count = int(transform[-1])
        first = f'SUBSTRING({expression}, 1, {count})'
        hashed = _hash(f'SUBSTRING({expression}, {count + 1})', dialect)
        return f'({first} || {hashed})' if dialect == 'postgres' else f'CONCAT({first}, {hashed})'
    if transform.startswith('MASK-STRING-SKIP-ENDS-'):
        count = int(transform[-1])
        length = f'CHAR_LENGTH({expression})'
        first = f'SUBSTRING({expression}, 1, {count})'
        middle = f"REPEAT('*', {length} - {2 * count})"
        last = f'SUBSTRING({expression}, {length} - {count} + 1, {count})'
        masked = f'({first} || {middle} || {last})' if dialect == 'postgres' else f'CONCAT({first}, {middle}, {last})'
        return f"CASE WHEN {length} > {2 * count} THEN {masked} ELSE REPEAT('*', {length}) END"
    raise UnsupportedSourceTransformation(f'Unsupported transformation type for column {column["column_name"]!r}')


def _projection(columns, replacements, dialect):
    return ', '.join(
        f'{replacements.get(column["column_name"], _identifier(column["column_name"], dialect))} '
        f'AS {_identifier(column["column_name"], dialect)}'
        for column in columns
    )


def _referenced_columns(rules, by_name):
    names = set()
    for rule in rules:
        names.add(_resolve(rule.get('field_id'), by_name)['column_name'])
        conditions = rule.get('when')
        if conditions is not None and not isinstance(conditions, list):
            raise UnsupportedSourceTransformation('Transformation when must be a list')
        for condition in conditions or []:
            if not isinstance(condition, dict):
                raise UnsupportedSourceTransformation('Transformation conditions must be objects')
            names.add(_resolve(condition.get('column'), by_name)['column_name'])
    return names


def _matching_rules(table_name, transformation_config):
    if not isinstance(transformation_config, dict):
        raise UnsupportedSourceTransformation('Transformation configuration must be an object')
    transformations = transformation_config.get('transformations', [])
    if not isinstance(transformations, list):
        raise UnsupportedSourceTransformation('Transformations must be a list')
    stream = table_name.replace('.', '-', 1).lower()
    rules = []
    unconditional = set()
    for rule in transformations:
        if not isinstance(rule, dict) or not isinstance(rule.get('tap_stream_name'), str):
            raise UnsupportedSourceTransformation('Transformation requires a source stream name')
        if rule['tap_stream_name'].lower() == stream:
            _validate_rule_config(rule)
            if not rule.get('when'):
                field = rule['field_id'].upper()
                if field in unconditional:
                    raise UnsupportedSourceTransformation(
                        f'Duplicate unconditional transformation for column {rule["field_id"]!r}'
                    )
                unconditional.add(field)
            rules.append(rule)
    return rules


def _validate_rule_config(rule):
    if rule.keys() - {'tap_stream_name', 'field_id', 'safe_field_id', 'field_paths', 'type', 'when'}:
        raise UnsupportedSourceTransformation('Unsupported source transformation configuration')
    if not isinstance(rule.get('field_id'), str) or not rule['field_id']:
        raise UnsupportedSourceTransformation('Transformation requires a non-empty field identifier')
    _identifier(rule['field_id'], 'postgres')
    try:
        TransformationType(rule.get('type'))
    except (TypeError, ValueError) as exc:
        raise UnsupportedSourceTransformation('Unsupported source transformation type') from exc
    if rule.get('when') is not None and not isinstance(rule['when'], list):
        raise UnsupportedSourceTransformation('Transformation when must be a list')
    if rule.get('field_paths') is not None:
        raise UnsupportedSourceTransformation('Only top-level transformations are supported')
    for condition in rule.get('when') or []:
        _validate_condition_config(condition)


def _validate_condition_config(condition):
    """Check portable syntax without guessing the condition column's type."""
    if not isinstance(condition, dict) or condition.get('field_path') is not None:
        raise UnsupportedSourceTransformation('Only top-level transformation conditions are supported')
    _identifier(condition.get('column'), 'postgres')
    operators = {'equals', 'regex_match'} & condition.keys()
    if len(operators) != 1 or condition.keys() - {'column', 'equals', 'regex_match', 'safe_column'}:
        raise UnsupportedSourceTransformation('Each condition requires exactly one equals or regex_match operator')
    if 'regex_match' in condition:
        _portable_pattern(condition['regex_match'], 'postgres')
        return
    value = condition['equals']
    if isinstance(value, str):
        if '\\' in value:
            raise UnsupportedSourceTransformation(
                'Backslash equality conditions cannot preserve legacy Snowflake literal semantics'
            )
        _literal(value, 'postgres')
    elif isinstance(value, float) and not math.isfinite(value):
        raise UnsupportedSourceTransformation('Non-finite equality values are unsupported')
    elif value is not None and not isinstance(value, (bool, int, float)):
        raise UnsupportedSourceTransformation('Unsupported equality value type in source transformation')


def validate_source_transformation_config(table_name, transformation_config):
    """Reject configuration-only incompatibilities before discovery or export."""
    _matching_rules(table_name, transformation_config)


def requires_regex_support(table_name, transformation_config):
    """Return whether this stream needs the source's portable regex capability."""
    if transformation_config is None:
        return False
    return any(
        'regex_match' in condition
        for rule in _matching_rules(table_name, transformation_config)
        for condition in rule.get('when') or []
    )


def validate_bookmark_column(table_name, column_name, transformation_config):
    """Reject masking a replication key whose raw MAX would be persisted."""
    if transformation_config is None or column_name is None:
        return
    if not isinstance(column_name, str) or not column_name:
        raise UnsupportedSourceTransformation('An INCREMENTAL replication key must be a non-empty column name')
    for rule in _matching_rules(table_name, transformation_config):
        if rule['field_id'].upper() == column_name.upper():
            raise UnsupportedSourceTransformation(
                'Cannot transform an INCREMENTAL replication key: raw bookmarks are required '
                'for replication state and would bypass source transformation protection'
            )


def compile_source_select(
    table_name, table_reference, where_clause, columns, transformation_config, dialect, iceberg_version=None,
):
    """Return an export SELECT preserving the existing Snowflake update order."""
    if dialect not in {'postgres', 'mysql', 'mariadb'}:
        raise UnsupportedSourceTransformation(f'Unsupported source dialect {dialect!r}')
    rules = _matching_rules(table_name, transformation_config)
    if not rules:
        return None
    by_name = {}
    for column in columns:
        name = column.get('column_name')
        _identifier(name, dialect)
        if name.upper() in by_name:
            raise UnsupportedSourceTransformation(f'Ambiguous case-insensitive column name {name!r}')
        by_name[name.upper()] = column
    referenced = _referenced_columns(rules, by_name)
    columns = [
        _mapped_column(column, iceberg_version) if column['column_name'] in referenced else column
        for column in columns
    ]
    by_name = {column['column_name'].upper(): column for column in columns}
    base = {
        column['column_name']: (
            _base_expression(column, dialect) if column['column_name'] in referenced
            else _unalias(column.get('safe_sql_value'), column['column_name'], dialect)
        )
        for column in columns
    }
    query = f'SELECT {_projection(columns, base, dialect)} FROM {table_reference} {where_clause}'.rstrip()
    unconditional = {}
    for rule in rules:
        column = _resolve(rule.get('field_id'), by_name)
        name = column['column_name']
        transformed = _transform(rule, column, dialect)
        conditions = rule.get('when')
        if conditions:
            predicate = ' AND '.join(_condition(condition, by_name, dialect) for condition in conditions)
            replacement = f'CASE WHEN {predicate} THEN {transformed} ELSE {_identifier(name, dialect)} END'
            query = f'SELECT {_projection(columns, {name: replacement}, dialect)} FROM ({query}) AS ppw_transform'
        else:
            unconditional[name] = transformed
    if unconditional:
        query = f'SELECT {_projection(columns, unconditional, dialect)} FROM ({query}) AS ppw_transform'
    return query
