"""FastSync exports PostgreSQL and YugabyteDB arrays as array_to_json text.

Singer decimal routes select array_to_json(col)::text for numeric arrays and COPY writes the same json text, so both
transports agree only while this projection stays the plain, first ARRAY branch.
"""

import re
from unittest.mock import MagicMock, patch

import pytest

from pipelinewise.fastsync.commons import tap_postgres, tap_yugabyte
from pipelinewise.fastsync.postgres_to_snowflake import tap_type_to_target_type as postgres_mapper
from pipelinewise.fastsync.yugabyte_to_snowflake import tap_type_to_target_type as yugabyte_mapper

ARRAY_BRANCH = "WHEN data_type = 'ARRAY' THEN 'array_to_json(\"' || column_name || '\") AS ' || column_name"
SOURCES = {
    'postgres': (tap_postgres, tap_postgres.FastSyncTapPostgres, postgres_mapper),
    'yugabyte': (tap_yugabyte, tap_yugabyte.FastSyncTapYugabyte, yugabyte_mapper),
}


def _source(dialect, hstore_as_json=False):
    _, source_class, mapper = SOURCES[dialect]
    source = source_class({}, mapper)
    source.hstore_as_json = hstore_as_json
    return source


def _table_columns_sql(source, max_num=None):
    with patch.object(source, 'query', return_value=[]) as query:
        source.get_table_columns('public.events', max_num=max_num)
    return query.call_args.args[0]


def _first_projection_branch(sql):
    """Return the first WHEN of the CASE that builds safe_sql_value."""
    return re.search(r'udt_name,\s*CASE\s*(WHEN [^\n]*)', sql).group(1).strip()


def _evaluate(branch, column_name):
    """Evaluate a branch's string concatenation for one column, as the source does."""
    pieces = [piece.strip() for piece in branch.split(' THEN ', 1)[1].split('||')]
    return ''.join(column_name if piece == 'column_name' else piece[1:-1].replace("''", "'") for piece in pieces)


def _column(name, data_type, safe_sql_value, precision=None, scale=None):
    values = (name, data_type, safe_sql_value, None, precision, scale)
    names = (
        'column_name', 'data_type', 'safe_sql_value', 'character_maximum_length', 'numeric_precision', 'numeric_scale',
    )
    return {**dict(enumerate(values)), **dict(zip(names, values))}


@pytest.mark.parametrize('dialect', SOURCES)
@pytest.mark.parametrize('max_num', [None, '99999999999999999.999999'])
@pytest.mark.parametrize('hstore_as_json', [False, True])
def test_arrays_take_the_first_projection_branch_on_every_route(dialect, max_num, hstore_as_json):
    sql = _table_columns_sql(_source(dialect, hstore_as_json), max_num)
    assert _first_projection_branch(sql) == ARRAY_BRANCH
    assert sql.count("data_type = 'ARRAY'") == 1


@pytest.mark.parametrize('dialect', SOURCES)
@pytest.mark.parametrize('max_num', [None, '1'])
def test_array_projection_is_plain_array_to_json_without_casts_or_clamping(dialect, max_num):
    branch = _first_projection_branch(_table_columns_sql(_source(dialect), max_num))
    assert _evaluate(branch, 'amounts') == 'array_to_json("amounts") AS amounts'


@pytest.mark.parametrize('dialect', SOURCES)
def test_copy_exports_the_array_projection_unchanged_beside_numeric_normalization(dialect, tmp_path):
    module = SOURCES[dialect][0]
    source = _source(dialect)
    source.target_type = 'snowflake'
    projection = _evaluate(_first_projection_branch(_table_columns_sql(source)), 'amounts')
    source.curr = MagicMock()
    source.get_table_columns = MagicMock(return_value=[
        _column('id', 'integer', '"id"'), _column('amounts', 'ARRAY', projection),
        _column('price', 'numeric', '"price"', 10, 2),
    ])
    source.get_primary_keys = MagicMock(return_value=['"ID"'])
    with patch.object(module.split_gzip, 'open'):
        source.copy_table('public.events', str(tmp_path / 'events.csv.gz'))

    sql = source.curr.copy_expert.call_args.args[0]
    assert sql.startswith('COPY (') and 'array_to_json("amounts") AS amounts,' in sql
    assert 'NULLIF("amounts"' not in sql
    # Only PostgreSQL wraps the export to turn NaN into NULL for Snowflake NUMERIC columns.
    assert ('NULLIF("price", \'NaN\'::numeric) AS "price"' in sql) is (dialect == 'postgres')
