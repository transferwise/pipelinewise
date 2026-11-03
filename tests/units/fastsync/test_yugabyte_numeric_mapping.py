from argparse import Namespace
from decimal import Decimal
from unittest.mock import Mock

import pytest
from singer.decimal_support import decimal_schema, decimal_sql_type

from pipelinewise.fastsync import yugabyte_to_postgres, yugabyte_to_snowflake
from pipelinewise.fastsync.commons.partial_sync_boundary import PartialSyncBoundaryError
from pipelinewise.fastsync.commons.tap_yugabyte import FastSyncTapYugabyte
from pipelinewise.fastsync.partialsync.rdbms_to_snowflake import _resolved_boundary


def _source(route=yugabyte_to_snowflake, columns=(), primary_keys=None):
    source = FastSyncTapYugabyte({}, route.tap_type_to_target_type)
    source.get_table_columns = Mock(return_value=list(columns))
    source.get_primary_keys = Mock(return_value=primary_keys)
    return source


@pytest.mark.parametrize('route', (yugabyte_to_postgres, yugabyte_to_snowflake))
@pytest.mark.parametrize('source_type', ('numeric', 'decimal'))
def test_declared_dimensions_reach_both_routes(route, source_type):
    source = _source(route, [('amount', source_type, '"amount"', None, 38, 18)])
    assert source.map_column_types_to_target('public.items')['columns'] == ['"AMOUNT" NUMERIC(38,18)']


@pytest.mark.parametrize('precision,scale,expected', (
    (65, 30, 'FLOAT'), (38, 38, 'FLOAT'), (3, -2, 'NUMERIC(5,0)'), (3, 5, 'NUMERIC(5,5)'),
    (None, None, 'FLOAT'), (10, 2046, 'NUMERIC(12,0)'),
))
def test_snowflake_normalizes_compatible_definitions_then_falls_back(precision, scale, expected):
    assert yugabyte_to_snowflake.tap_type_to_target_type('numeric', None, precision, scale) == expected


@pytest.mark.parametrize('precision,scale', ((65, 30), (3, -2), (3, 5), (None, None)))
def test_postgres_does_not_inherit_snowflake_limits(precision, scale):
    expected = 'NUMERIC' if precision is None else f'NUMERIC({precision},{scale})'
    assert yugabyte_to_postgres.tap_type_to_target_type('numeric', None, precision, scale) == expected


@pytest.mark.parametrize('precision,scale', ((18, 2), (38, 0), (65, 30), (None, None)))
def test_snowflake_decimal_keys_match_target_snowflake_for_yugabyte_streams(precision, scale):
    # target-snowflake only applies PostgreSQL NaN-key handling when source_tap_type is tap-postgres.
    singer_type = decimal_sql_type(decimal_schema(precision, scale), 'snowflake', is_key=True)
    assert yugabyte_to_snowflake.tap_type_to_target_type('numeric', None, precision, scale, is_key=True) == singer_type


def test_bounded_decimal_key_stays_numeric_and_marks_decimal_columns():
    source = _source(columns=[
        ('id', 'numeric', '"id"', None, 20, 0),
        ('amount', 'numeric', '"amount"', None, None, None),
        ('score', 'double precision', '"score"', None, None, None),
    ], primary_keys=['"ID"'])
    mapped = source.map_column_types_to_target('public.items')
    assert mapped['columns'] == ['"ID" NUMERIC(20,0)', '"AMOUNT" FLOAT', '"SCORE" FLOAT']
    assert mapped['decimal_columns'] == ['ID', 'AMOUNT']
    assert mapped['primary_key'] == ['"ID"']


def test_only_fallback_decimal_keys_receive_canonical_export_projection():
    source = _source(primary_keys=['"ID"', '"CODE"'])
    columns = [
        {'column_name': 'id', 'data_type': 'numeric', 'numeric_precision': None, 'numeric_scale': None,
         'safe_sql_value': 'id'},
        {'column_name': 'code', 'data_type': 'numeric', 'numeric_precision': 18, 'numeric_scale': 2,
         'safe_sql_value': 'code'},
        {'column_name': 'amount', 'data_type': 'numeric', 'numeric_precision': None, 'numeric_scale': None,
         'safe_sql_value': 'amount'},
    ]
    projected, keys = source._decimal_key_projection('public.items', columns)
    assert keys == ['"ID"', '"CODE"']
    assert "TRIM(TRAILING '.' FROM TRIM(TRAILING '0'" in projected[0]['safe_sql_value']
    assert [column['safe_sql_value'] for column in projected[1:]] == ['code', 'amount']
    assert columns[0]['safe_sql_value'] == 'id'


def test_fullsync_handover_bookmark_is_exact():
    source = _source()
    source.query = Mock(return_value=[{'key_value': Decimal('12345678901234567890.123456789')}])
    assert source.fetch_current_incremental_key_pos('source.table', 'amount')['replication_key_value'] == (
        '12345678901234567890.123456789'
    )


@pytest.mark.parametrize('target_type', ['FLOAT', 'VARCHAR(134217728)'])
def test_decimal_fallback_boundaries_require_fullsync(target_type):
    source = _source(columns=[('amount', 'numeric')])
    run = Namespace(column_name='amount', table_name='public.items', source=source,
                    args=Namespace(drop_target_table=False))
    mapped = {'source_column_names': ['amount'], 'columns': [f'"AMOUNT" {target_type}']}
    with pytest.raises(PartialSyncBoundaryError, match='decimal boundary.*FullSync'):
        _resolved_boundary(run, mapped, 1, 2)


def test_exact_decimal_boundaries_skip_the_source_check():
    source = _source(columns=[('amount', 'numeric')])
    run = Namespace(column_name='amount', table_name='public.items', source=source,
                    args=Namespace(drop_target_table=False))
    mapped = {'source_column_names': ['amount'], 'columns': ['"AMOUNT" NUMERIC(10,2)']}
    assert _resolved_boundary(run, mapped, 1, 2).column_name == 'amount'
    source.get_table_columns.assert_not_called()


def test_retained_float_decimal_uses_float_transformation_type():
    source = _source(primary_keys=None)
    source.source_transformations = []
    captured = {}

    def compile_select(_table, _reference, _where, columns, *_args):
        captured['types'] = {column['column_name']: column['target_type'] for column in columns}
        return 'SELECT 1'

    columns = [{'column_name': 'amount', 'data_type': 'numeric', 'numeric_precision': 18, 'numeric_scale': 2,
                'character_maximum_length': None}]
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr('pipelinewise.fastsync.commons.tap_yugabyte.compile_source_select', compile_select)
        source._compile_source_projection('public.items', columns, decimal_float_columns=('AMOUNT',))
        assert captured['types'] == {'amount': 'FLOAT'}
        source._compile_source_projection('public.items', columns)
        assert captured['types'] == {'amount': 'NUMERIC(18,2)'}
