"""Decimal types and generated settings retain source dimensions and scope."""

from decimal import Decimal
from argparse import Namespace
from unittest.mock import Mock

import pytest

from pipelinewise.cli.config import Config
from pipelinewise.fastsync import mysql_to_postgres, mysql_to_snowflake, postgres_to_postgres, postgres_to_snowflake
from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.commons.partial_sync_boundary import PartialSyncBoundaryError
from pipelinewise.fastsync.commons.snowflake_iceberg_model import IcebergTableSpec
from pipelinewise.fastsync.commons.snowflake_decimal_evolution import partial_preparation, plan_column_versions
from pipelinewise.fastsync.partialsync.rdbms_to_snowflake import _resolved_boundary


@pytest.mark.parametrize('route', (mysql_to_postgres, mysql_to_snowflake))
@pytest.mark.parametrize('source_type', ('decimal', 'numeric'))
@pytest.mark.parametrize('precision,scale', ((1, 0), (18, 2), (38, 18), (38, 37)))
def test_mysql_decimal_mapping_preserves_declared_dimensions(route, source_type, precision, scale):
    declaration = f'{source_type}({precision},{scale}) unsigned zerofill'
    assert route.tap_type_to_target_type(source_type, declaration) == f'NUMERIC({precision},{scale})'
    assert 'FLOAT' in route.tap_type_to_target_type('float', 'float') or route == mysql_to_postgres


@pytest.mark.parametrize('route', (postgres_to_postgres, postgres_to_snowflake))
@pytest.mark.parametrize('source_type', ('numeric', 'decimal'))
def test_postgres_fastsync_metadata_reaches_mapper(route, source_type):
    source = FastSyncTapPostgres({}, route.tap_type_to_target_type)
    assert source.map_table_columns([('amount', source_type, '"amount"', None, 38, 18)]) == [
        '"AMOUNT" NUMERIC(38,18)',
    ]


@pytest.mark.parametrize('precision,scale,expected', (
    (65, 30, 'FLOAT'), (38, 38, 'FLOAT'), (3, -2, 'NUMERIC(5,0)'), (3, 5, 'NUMERIC(5,5)'),
    (None, None, 'FLOAT'), (10, 2046, 'NUMERIC(12,0)'),
))
def test_snowflake_normalizes_compatible_definitions_then_falls_back(precision, scale, expected):
    assert postgres_to_snowflake.tap_type_to_target_type('numeric', None, precision, scale) == expected


@pytest.mark.parametrize('dialect', ('mysql', 'postgres'))
def test_iceberg_decimal_fallback_uses_double_for_creation_and_column_evolution(dialect):
    if dialect == 'mysql':
        mapped = mysql_to_snowflake.tap_type_to_target_type('decimal', 'decimal(65,30)')
    else:
        mapped = postgres_to_snowflake.tap_type_to_target_type('numeric', None, None, None)
    expected = IcebergTableSpec.from_fastsync('DB', 'SCHEMA', 'TABLE', [f'AMOUNT {mapped}'], [])
    actual = IcebergTableSpec.from_fastsync('DB', 'SCHEMA', 'TABLE', ['AMOUNT NUMERIC(18,2)'], [])
    assert expected.columns[0].data_type == 'DOUBLE'
    statements = partial_preparation(
        expected, actual, plan_column_versions(expected, actual, decimal_columns=('AMOUNT',)),
    ).statements
    assert statements[-1].endswith('ADD COLUMN "AMOUNT" DOUBLE')


@pytest.mark.parametrize('precision,scale', ((65, 30), (3, -2), (3, 5), (None, None)))
def test_postgres_does_not_inherit_snowflake_limits(precision, scale):
    expected = 'NUMERIC' if precision is None else f'NUMERIC({precision},{scale})'
    assert postgres_to_postgres.tap_type_to_target_type('numeric', None, precision, scale) == expected


@pytest.mark.parametrize('tap_type', ('tap-mysql', 'tap-postgres', 'tap-salesforce', 'tap-mixpanel', 'tap-snowflake'))
@pytest.mark.parametrize('target_type', ('target-snowflake', 'target-postgres', 'target-s3-csv'))
def test_generated_decimal_mode_is_limited_to_supported_pairs(tap_type, target_type):
    tap = {'type': tap_type, 'db_conn': {'host': 'source', 'decimal_target': 'snowflake'}}
    config = Config.generate_tap_connection_config(tap, {}, target_type)
    if tap_type in ('tap-mysql', 'tap-postgres') and target_type != 'target-s3-csv':
        assert config['decimal_target'] == target_type.removeprefix('target-')
    else:
        assert 'decimal_target' not in config


def test_mysql_fullsync_handover_bookmark_is_exact():
    source = FastSyncTapMySql({}, mysql_to_snowflake.tap_type_to_target_type)
    source.query = Mock(return_value=[{'key_value': Decimal('12345678901234567890.123456789')}])
    assert source.fetch_current_incremental_key_pos('source.table', 'amount')['replication_key_value'] == (
        '12345678901234567890.123456789'
    )


@pytest.mark.parametrize('primary_keys', [('amount',), ('"AMOUNT"',)])
def test_decimal_text_keys_match_singer_without_float_collisions(primary_keys):
    mysql = FastSyncTapMySql({}, mysql_to_snowflake.tap_type_to_target_type)
    mysql_columns = [{'column_name': 'amount', 'data_type': 'decimal', 'column_type': 'decimal(65,30)'}]
    assert mysql.map_table_columns(mysql_columns, primary_keys) == ['"AMOUNT" VARCHAR(134217728)']
    assert mysql.map_table_columns(mysql_columns) == ['"AMOUNT" FLOAT']
    mysql_columns[0]['column_type'] = 'decimal(18,2)'
    assert mysql.map_table_columns(mysql_columns, primary_keys) == ['"AMOUNT" NUMERIC(18,2)']
    postgres = FastSyncTapPostgres({}, postgres_to_snowflake.tap_type_to_target_type)
    postgres_columns = [('amount', 'numeric', '"amount"', None, None, None)]
    assert postgres.map_table_columns(postgres_columns, primary_keys) == ['"AMOUNT" VARCHAR(134217728)']
    assert postgres.map_table_columns(postgres_columns) == ['"AMOUNT" FLOAT']
    postgres_columns = [('amount', 'numeric', '"amount"', None, 18, 2)]
    assert postgres.map_table_columns(postgres_columns, primary_keys) == ['"AMOUNT" VARCHAR(134217728)']
    assert postgres.map_table_columns(postgres_columns) == ['"AMOUNT" NUMERIC(18,2)']


@pytest.mark.parametrize('dialect', ('mysql', 'postgres'))
def test_source_mapping_marks_decimal_origin_independent_of_target_type(dialect):
    if dialect == 'mysql':
        source = FastSyncTapMySql({}, mysql_to_snowflake.tap_type_to_target_type)
        columns = [
            {'column_name': 'amount', 'data_type': 'decimal', 'column_type': 'decimal(65,30)'},
            {'column_name': 'score', 'data_type': 'float', 'column_type': 'float'},
            {'column_name': 'id', 'data_type': 'integer', 'column_type': 'int(11)'},
        ]
    else:
        source = FastSyncTapPostgres({}, postgres_to_snowflake.tap_type_to_target_type)
        columns = [
            ('amount', 'numeric', '"amount"', None, None, None),
            ('score', 'double precision', '"score"', None, None, None),
            ('id', 'integer', '"id"', None, None, None),
        ]
    source.get_table_columns = Mock(return_value=columns)
    source.get_primary_keys = Mock(return_value=['"ID"'])
    mapped = source.map_column_types_to_target('public.items')
    assert mapped['decimal_columns'] == ['AMOUNT']
    assert mapped['primary_key'] == ['"ID"']


@pytest.mark.parametrize('route', (postgres_to_postgres, postgres_to_snowflake))
def test_postgres_signed_information_schema_scale_reaches_all_mappers(route):
    source = FastSyncTapPostgres({}, route.tap_type_to_target_type)
    expected = 'NUMERIC(10,-2)' if route == postgres_to_postgres else 'NUMERIC(12,0)'
    assert source.map_table_columns([('amount', 'numeric', '"amount"', None, 10, 2046)]) == [f'"AMOUNT" {expected}']


@pytest.mark.parametrize('dialect', ['mysql', 'postgres'])
def test_only_fallback_decimal_keys_receive_canonical_export_projection(dialect):
    if dialect == 'mysql':
        source = FastSyncTapMySql({}, mysql_to_snowflake.tap_type_to_target_type)
        column = {'data_type': 'decimal', 'column_type': 'decimal(65,30)'}
    else:
        source = FastSyncTapPostgres({}, postgres_to_snowflake.tap_type_to_target_type)
        column = {'data_type': 'numeric', 'numeric_precision': None, 'numeric_scale': None}
    source.get_primary_keys = Mock(return_value=['"ID"'])
    columns = [dict(column, column_name=name, safe_sql_value=name) for name in ('id', 'amount')]
    projected, keys = source._decimal_key_projection('public.items', columns)
    assert keys == ['"ID"']
    assert "CASE WHEN POSITION('.' IN CAST(" in projected[0]['safe_sql_value']
    assert "TRIM(TRAILING '.' FROM TRIM(TRAILING '0'" in projected[0]['safe_sql_value']
    assert projected[1]['safe_sql_value'] == 'amount'
    assert columns[0]['safe_sql_value'] == 'id'


@pytest.mark.parametrize('dialect', ['mysql', 'postgres'])
@pytest.mark.parametrize('target_type', ['FLOAT', 'VARCHAR(134217728)'])
def test_decimal_fallback_boundaries_require_fullsync(dialect, target_type):
    if dialect == 'mysql':
        source = FastSyncTapMySql({}, mysql_to_snowflake.tap_type_to_target_type)
        columns = [{'column_name': 'amount', 'data_type': 'decimal'}]
    else:
        source = FastSyncTapPostgres({}, postgres_to_snowflake.tap_type_to_target_type)
        columns = [('amount', 'numeric')]
    source.get_table_columns = Mock(return_value=columns)
    run = Namespace(column_name='amount', table_name='public.items', source=source,
                    args=Namespace(drop_target_table=False))
    mapped = {'source_column_names': ['amount'], 'columns': [f'"AMOUNT" {target_type}']}
    with pytest.raises(PartialSyncBoundaryError, match='decimal boundary.*FullSync'):
        _resolved_boundary(run, mapped, 1, 2)


@pytest.mark.parametrize('dialect', ['mysql', 'postgres'])
@pytest.mark.parametrize('target_type', ['FLOAT', 'VARCHAR(134217728)'])
def test_decimal_fallback_boundaries_allow_explicit_target_replacement(dialect, target_type):
    if dialect == 'mysql':
        source = FastSyncTapMySql({}, mysql_to_snowflake.tap_type_to_target_type)
    else:
        source = FastSyncTapPostgres({}, postgres_to_snowflake.tap_type_to_target_type)
    source.validate_partial_boundary = Mock()
    run = Namespace(column_name='amount', table_name='public.items', source=source,
                    args=Namespace(drop_target_table=True))
    mapped = {'source_column_names': ['amount'], 'columns': [f'"AMOUNT" {target_type}']}

    boundary = _resolved_boundary(run, mapped, 1, 2)

    assert boundary.column_name == 'amount'
    assert boundary.drop_target is True
    source.validate_partial_boundary.assert_not_called()


def test_ordinary_float_boundaries_and_exact_decimal_boundaries_are_unchanged():
    source = FastSyncTapPostgres({}, postgres_to_snowflake.tap_type_to_target_type)
    source.get_table_columns = Mock(return_value=[('amount', 'real')])
    run = Namespace(column_name='amount', table_name='public.items', source=source,
                    args=Namespace(drop_target_table=False))
    mapped = {'source_column_names': ['amount'], 'columns': ['"AMOUNT" FLOAT']}
    assert _resolved_boundary(run, mapped, 1, 2).column_name == 'amount'
    source.get_table_columns.reset_mock()
    mapped['columns'] = ['"AMOUNT" NUMERIC(10,2)']
    assert _resolved_boundary(run, mapped, 1, 2).column_name == 'amount'
    source.get_table_columns.assert_not_called()
