"""PostgreSQL-to-Snowflake FastSync CSV preservation contract."""
import csv
import gzip
import io
from unittest.mock import Mock, patch

from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.commons.target_snowflake import (
    FastSyncTargetSnowflake,
)


POSTGRES_MULTILINE_CSV = (
    ',"","line1\nline2","carriage\rreturn","windows\r\nline",'
    'tab\ttext,literal\\n and \\t,"quote "" and, comma",'
    'Grüße 🐍,trail\\\n'
).encode('utf-8')

EXPECTED_VALUES = [
    '',
    '',
    'line1\nline2',
    'carriage\rreturn',
    'windows\r\nline',
    'tab\ttext',
    r'literal\n and \t',
    'quote " and, comma',
    'Grüße 🐍',
    'trail\\',
]


def test_postgres_export_preserves_multiline(tmp_path):
    """PipelineWise writes the PostgreSQL COPY stream without rewriting it."""
    tap = FastSyncTapPostgres(
        connection_config={},
        tap_type_to_target_type={},
    )
    tap.curr = Mock()
    export_path = tmp_path / 'scripts.csv.gz'
    column_specs = [
        {'safe_sql_value': f'"column_{index}"'}
        for index in range(len(EXPECTED_VALUES))
    ]

    def write_postgres_copy(_sql, output, size):
        assert size == 131072
        output.write(POSTGRES_MULTILINE_CSV)

    tap.curr.copy_expert.side_effect = write_postgres_copy

    with patch.object(tap, 'get_table_columns', return_value=column_specs):
        tap.copy_table('public.scripts', str(export_path))

    with gzip.open(export_path, 'rb') as exported_file:
        exported_bytes = exported_file.read()

    assert exported_bytes == POSTGRES_MULTILINE_CSV
    assert exported_bytes.startswith(b',""'), 'SQL NULL and empty text must remain distinct'
    assert next(
        csv.reader(io.StringIO(exported_bytes.decode('utf-8'), newline=''), strict=True)
    ) == EXPECTED_VALUES

    copy_sql = tap.curr.copy_expert.call_args.args[0]
    assert copy_sql.startswith('COPY (SELECT "column_0"')
    assert "TO STDOUT with CSV DELIMITER ','" in copy_sql


def test_snowflake_loads_postgres_multiline():
    """The shared loader uses CSV quoting without backslash interpretation."""
    target = object.__new__(FastSyncTargetSnowflake)
    target.connection_config = {'stage': 'test_stage'}
    target.query = Mock(return_value=[])

    target.copy_to_table(
        'scripts.csv.gz',
        'target_schema',
        'scripts',
        len(POSTGRES_MULTILINE_CSV),
        is_temporary=True,
    )

    copy_sql = target.query.call_args.args[0]
    assert 'FILE_FORMAT = (type=CSV' in copy_sql
    assert 'escape=NONE' in copy_sql
    assert "escape_unenclosed_field='\\x1e'" in copy_sql
    assert "field_optionally_enclosed_by='\"'" in copy_sql
    assert 'null_if=()' in copy_sql
    assert 'empty_field_as_null=TRUE' in copy_sql
    assert 'compression=GZIP' in copy_sql
