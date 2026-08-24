"""Shared byte-preservation fixtures for RDBMS-to-Snowflake FastSync."""

from tests.end_to_end.helpers import assertions

FULLSYNC_TEXT_VALUES = (
    (10, 'full LF:\nnext'),
    (11, 'full CR:\rnext'),
    (12, 'full CRLF:\r\nnext'),
    (13, 'full tab:\tnext'),
    (14, r'full literals:\n and \t'),
    (15, 'full quotes: "double", apostrophe: \' and comma, text: \u521d, trailing slash:\\'),
    (16, ''),
    (17, None),
    (18, r'\N'),
)

PARTIALSYNC_TEXT_VALUES = (
    (10, 'partial LF:\nnext'),
    (11, 'partial CR:\rnext'),
    (12, 'partial CRLF:\r\nnext'),
    (13, 'partial tab:\tnext'),
    (14, r'partial literals:\n and \t'),
    (15, 'partial quotes: "double", apostrophe: \' and comma, text: \u96ea, trailing slash:\\'),
    (16, None),
    (17, ''),
    (18, r'\N'),
)

INITIAL_SENTINEL = (1, 'target sentinel')
SOURCE_ONLY_SENTINEL = (1, 'source-only LF:\nnot in bounded PartialSync')


def utf8_hex_rows(values):
    """Render exact expected Snowflake HEX_ENCODE results, retaining NULL."""
    return [
        (
            row_id,
            value is None,
            None if value is None else value.encode('utf-8').hex().upper(),
        )
        for row_id, value in values
    ]


def snowflake_utf8_hex_rows(e2e_env, target_schema):
    """Read target values without normalizing control characters in a driver."""
    return e2e_env.run_query_target_snowflake(
        'SELECT "ID", "VALUE_TEXT" IS NULL, HEX_ENCODE("VALUE_TEXT") '
        f'FROM "{target_schema}"."MULTILINE_VALUES" ORDER BY "ID"'
    )


def prepare_mysql_multiline_table(query_runner):
    """Create the shared multiline fixture in MariaDB or MySQL."""
    query_runner('DROP TABLE IF EXISTS multiline_values')
    query_runner(
        'CREATE TABLE multiline_values ('
        'id INTEGER NOT NULL PRIMARY KEY, value_text LONGTEXT NULL) '
        'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    )
    for row in (INITIAL_SENTINEL, *FULLSYNC_TEXT_VALUES):
        query_runner(
            'INSERT INTO multiline_values (id, value_text) VALUES (%s, %s)',
            row,
        )


def prepare_postgres_multiline_table(query_runner):
    """Create the shared multiline fixture in PostgreSQL."""
    query_runner('DROP TABLE IF EXISTS public.multiline_values CASCADE')
    query_runner(
        'CREATE TABLE public.multiline_values ('
        'id integer NOT NULL PRIMARY KEY, value_text text)'
    )
    for row in (INITIAL_SENTINEL, *FULLSYNC_TEXT_VALUES):
        query_runner(
            'INSERT INTO public.multiline_values '
            '(id, value_text) VALUES (%s, %s)',
            row,
        )


def assert_native_multiline_table(test_case, target_schema):
    """Require the native route to create an ordinary Snowflake table."""
    rows = test_case.e2e_env.run_query_target_snowflake(
        'SELECT IS_ICEBERG FROM INFORMATION_SCHEMA.TABLES '
        f"WHERE TABLE_SCHEMA = '{target_schema}' "
        "AND TABLE_NAME = 'MULTILINE_VALUES'"
    )
    test_case.assertEqual(rows, [('NO',)])


def exercise_multiline_fastsync(
        test_case, query_runner, source_db, tap_type, target_schema,
        assert_target_table):
    """Exercise exact text bytes through FullSync and bounded PartialSync."""
    source_table = f'{source_db}.multiline_values'
    stream_id = f'{source_db}-multiline_values'
    assertions.assert_resync_tables_success(
        test_case.tap_id,
        test_case.target_id,
        tables=source_table,
        expected_state_streams={'fastsync': {stream_id: True}},
    )
    assert_target_table()

    fullsync_values = (INITIAL_SENTINEL, *FULLSYNC_TEXT_VALUES)
    test_case.assertEqual(
        list(query_runner(
            f'SELECT id, value_text FROM {source_table} ORDER BY id'
        )),
        list(fullsync_values),
    )
    test_case.assertEqual(
        snowflake_utf8_hex_rows(test_case.e2e_env, target_schema),
        utf8_hex_rows(fullsync_values),
    )

    query_runner(
        f'UPDATE {source_table} SET value_text = %s WHERE id = %s',
        (SOURCE_ONLY_SENTINEL[1], SOURCE_ONLY_SENTINEL[0]),
    )
    for row_id, value in PARTIALSYNC_TEXT_VALUES:
        query_runner(
            f'UPDATE {source_table} SET value_text = %s WHERE id = %s',
            (value, row_id),
        )
    source_values = (SOURCE_ONLY_SENTINEL, *PARTIALSYNC_TEXT_VALUES)
    test_case.assertEqual(
        list(query_runner(
            f'SELECT id, value_text FROM {source_table} ORDER BY id'
        )),
        list(source_values),
    )

    assertions.assert_partial_sync_table_success(
        {
            'env': test_case.e2e_env,
            'tap': test_case.tap_id,
            'tap_type': tap_type,
            'target': test_case.target_id,
            'source_db': source_db,
            'table': 'multiline_values',
            'column': 'id',
        },
        start_value=10,
        end_value=18,
    )
    target_values = (INITIAL_SENTINEL, *PARTIALSYNC_TEXT_VALUES)
    test_case.assertEqual(
        snowflake_utf8_hex_rows(test_case.e2e_env, target_schema),
        utf8_hex_rows(target_values),
    )
