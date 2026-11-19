"""Real target-engine checks for FastSync's legacy post-load transformations."""

from uuid import uuid4

from pipelinewise.fastsync.commons.target_postgres import FastSyncTargetPostgres
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake


def assert_apostrophe_conditions(connection_config: dict, *, snowflake: bool) -> None:
    """Execute equality and regex rules without changing any replicated fixtures."""
    schema = f'ppw_quoted_conditions_{uuid4().hex[:12]}'
    schema = schema.upper() if snowflake else schema
    fields = ('equals_one', 'regex_one', 'equals_many', 'regex_many')
    conditions = (
        {'equals': "O'Brien"}, {'regex_match': "^O'B.*$"},
        {'equals': "D'Angelo's"}, {'regex_match': "^D'Angelo'.*$"},
    )
    if snowflake:
        # One slash escapes the apostrophe; a pair represents a literal slash before a bare apostrophe.
        fields += ('equals_escaped', 'regex_escaped', 'equals_backslash')
        conditions += (
            {'equals': r"O\'Brien"}, {'regex_match': r"^O\'B.*$"},
            {'equals': r"O\\'Brien"},
        )
    transformations = {'transformations': [
        {'tap_stream_name': 'fixture-records', 'field_id': field, 'type': 'MASK-HIDDEN',
         'when': [{'column': 'gate', **condition}]}
        for field, condition in zip(fields, conditions)
    ]}
    target_type = FastSyncTargetSnowflake if snowflake else FastSyncTargetPostgres
    target = target_type(connection_config, transformations)
    names = tuple(name.upper() if snowflake else name for name in ('id', 'gate', *fields))
    table_name = 'RECORDS' if snowflake else 'records'
    table = f'"{schema}"."{table_name}"'
    target.query(f'CREATE SCHEMA "{schema}"')
    try:
        columns = ', '.join(f'"{name}" {"INTEGER" if index == 0 else "TEXT"}'
                            for index, name in enumerate(names))
        target.query(f'CREATE TABLE {table} ({columns})')
        gates = ("O'Brien", "D'Angelo's", 'OBrien', None, "xO'Brienx", "D'Angelo'tail")
        if snowflake:
            gates += (r"O\'Brien",)
        placeholders = ', '.join('(' + ', '.join(['%s'] * len(names)) + ')' for _ in gates)
        values = tuple(value for row_id, gate in enumerate(gates, 1)
                       for value in (row_id, gate, *(['private'] * len(fields))))
        target.query(f'INSERT INTO {table} VALUES {placeholders}', values)
        if snowflake:
            target.obfuscate_columns(schema, 'fixture.records', staging_table_name=table_name)
        else:
            target.obfuscate_columns(schema, 'fixture.records')
        projection = ', '.join(f'"{name}"' for name in (names[0], *names[2:]))
        rows = target.query(f'SELECT {projection} FROM {table} ORDER BY "{names[0]}"')
        expected = [
            (1, 'hidden', 'hidden', 'private', 'private'),
            (2, 'private', 'private', 'hidden', 'hidden'),
            (3, 'private', 'private', 'private', 'private'),
            (4, 'private', 'private', 'private', 'private'),
            (5, 'private', 'private', 'private', 'private'),
            (6, 'private', 'private', 'private', 'hidden'),
        ]
        if snowflake:
            expected.append((7, 'private', 'private', 'private', 'private'))
            escaped_results = {1: ('hidden', 'hidden', 'private'), 7: ('private', 'private', 'hidden')}
            expected = [(*row, *escaped_results.get(row[0], ('private', 'private', 'private'))) for row in expected]
        assert [tuple(row[name] for name in (names[0], *names[2:])) for row in rows] == expected
    finally:
        target.query(f'DROP SCHEMA "{schema}" CASCADE')
