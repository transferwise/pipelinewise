"""Invalid source projections must not pin a new Iceberg recovery attempt."""

import copy
from argparse import Namespace
from unittest.mock import MagicMock, Mock

import pymysql
import pytest

from pipelinewise.fastsync.commons import rdbms_to_snowflake as full_runner
from pipelinewise.fastsync.commons import snowflake_iceberg_routes as routes
from pipelinewise.fastsync.commons.snowflake_iceberg import SnowflakeIcebergPublisher
from pipelinewise.fastsync.commons.source_transformations import UnsupportedSourceTransformation
from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.mysql_to_snowflake import tap_type_to_target_type as mysql_mapper
from pipelinewise.fastsync.partialsync import rdbms_to_snowflake as partial_runner
from pipelinewise.fastsync.postgres_to_snowflake import tap_type_to_target_type as postgres_mapper
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import FakeSnowflake, missing_snapshot


TABLE = 'source.orders'
BOUNDARY = {'column': 'id', 'start_value': '<S>1', 'end_value': '<S>2', 'drop_target_table': False}


def _source(engine):
    """Keep the actual projection compiler; mock only metadata and data I/O."""
    if engine == 'postgres':
        source = FastSyncTapPostgres({}, postgres_mapper)
        columns = [
            {'column_name': 'id', 'data_type': 'integer', 'safe_sql_value': '"id"'},
            {'column_name': 'email', 'data_type': 'text', 'safe_sql_value': '"email"'},
        ]
    else:
        source = FastSyncTapMySql({'engine': engine}, mysql_mapper)
        columns = [
            {'column_name': 'id', 'data_type': 'int', 'column_type': 'int', 'safe_sql_value': '`id`'},
            {'column_name': 'email', 'data_type': 'text', 'column_type': 'text', 'safe_sql_value': '`email`'},
        ]
    source.get_table_columns = Mock(return_value=columns)
    source.map_column_types_to_target = Mock(return_value={
        'columns': ['"ID" NUMBER', '"EMAIL" VARCHAR(134217728)'],
        'primary_key': ['"ID"'], 'source_column_names': ['id', 'email'],
    })
    source.copy_table = Mock()
    source.export_source_table_data = Mock(return_value=[])
    return source


def _run(tmp_path, engine, kind, rule):
    source = _source(engine)
    args = Namespace(
        tap={'engine': engine},
        target={
            'tap_id': 'transformation_preflight', 'dbname': 'TEST_DB', 'default_target_schema': 'TEST_SCHEMA',
            'target_table_format': 'iceberg', 'iceberg_version': 3,
        },
        transform={'transformations': [dict(rule, tap_stream_name='source-orders')]},
        properties={}, table=TABLE, temp_dir=str(tmp_path), drop_target_table=False,
    )
    source.source_transformations = args.transform
    adapter = Mock()
    adapter.create.return_value = source
    adapter.bookmark_kwargs.return_value = {}
    adapter.resolved_source_engine.return_value = None if engine == 'postgres' else engine
    adapter.complete_full_export.return_value = (source.map_column_types_to_target.return_value, [], 0)
    snowflake = FakeSnowflake()
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
    publisher.inspect_table = Mock(return_value=missing_snapshot())
    identity = routes.fastsync_recovery_identity(
        args, TABLE, source_route='postgres_to_snowflake' if engine == 'postgres' else 'mysql_to_snowflake',
        source_engine=engine, staging_config={}, iceberg_version=3,
    )
    common = {
        'args': args, 'source_adapter': adapter, 'snowflake': snowflake, 'logger': Mock(),
        'iceberg_version': 3, 'iceberg_requested': True, 'source': source,
        'publisher': publisher, 'recovery_identity': identity, 'staging_config': {}, 'target_schema': 'TEST_SCHEMA',
    }
    if kind == 'full':
        route_utils = Mock()
        route_utils.get_bookmark_for_table.return_value = {}
        return full_runner._FullSyncRun(
            table=TABLE, route_utils=route_utils, filepath=str(tmp_path / 'export.csv.gz'), **common,
        )
    return partial_runner._PartialSyncRun(
        table=(TABLE, BOUNDARY), table_name=TABLE, column_name='id', **common,
    )


@pytest.mark.parametrize('engine', ['postgres', 'mysql', 'mariadb'])
@pytest.mark.parametrize('kind', ['full', 'partial'])
@pytest.mark.parametrize('invalid_rule', [
    {'field_id': 'email', 'type': 'HASH', 'when': [{'column': 'email', 'regex_match': '(?=secret)secret'}]},
    {'field_id': 'missing', 'type': 'HASH'},
    {'field_id': 'id', 'type': 'MASK-HIDDEN'},
])
def test_invalid_projection_does_not_pin_recovery_and_corrected_retry_succeeds(tmp_path, engine, kind, invalid_rule):
    """Reject before export/manifest creation, then accept the corrected identity."""
    run = _run(tmp_path, engine, kind, invalid_rule)
    export = full_runner._export_full_source if kind == 'full' else partial_runner._export_partial_source

    with pytest.raises(UnsupportedSourceTransformation):
        export(run)

    assert run.attempt is None
    assert not list(tmp_path.glob('*.json'))
    assert not list(tmp_path.glob('*.csv*'))
    run.publisher.inspect_table.assert_not_called()
    run.source.copy_table.assert_not_called()
    run.source.export_source_table_data.assert_not_called()

    corrected = _run(tmp_path, engine, kind, {'field_id': 'email', 'type': 'HASH'})
    assert corrected.recovery_identity != run.recovery_identity
    export(corrected)

    assert corrected.attempt.phase == 'prepared'
    recovered = corrected.publisher.load_attempt(
        corrected.spec, expected_kind=kind, recovery_identity=corrected.recovery_identity, staging_config={},
    )
    assert recovered.load_id == corrected.attempt.load_id
    assert recovered.recovery_identity == corrected.recovery_identity
    data_export = corrected.source.copy_table if kind == 'full' else corrected.source.export_source_table_data
    data_export.assert_called_once()


@pytest.mark.parametrize('engine', ['postgres', 'mysql', 'mariadb'])
@pytest.mark.parametrize('kind', ['full', 'partial'])
def test_retained_attempt_keeps_existing_preflight_and_identity_contract(tmp_path, engine, kind):
    """Do not impose new-attempt projection validation on retained recovery."""
    run = _run(tmp_path, engine, kind, {'field_id': 'email', 'type': 'HASH'})
    prepare = (
        full_runner._plan_full_iceberg_export if kind == 'full' else partial_runner._prepare_iceberg_partial_export
    )
    prepare(run)
    original = copy.deepcopy(run.attempt.as_dict())
    run.source.validate_source_transformations = Mock(side_effect=AssertionError('Unexpected new-attempt preflight'))

    prepare(run)

    assert run.attempt.as_dict() == original
    run.source.validate_source_transformations.assert_not_called()


@pytest.mark.parametrize('engine', ['postgres', 'mysql', 'mariadb'])
@pytest.mark.parametrize('configuration', [[], False, 0, ''])
def test_preflight_rejects_falsy_malformed_configuration(engine, configuration):
    """Preflight must not skip configurations that the exporter will reject."""
    source = _source(engine)
    source.source_transformations = configuration

    with pytest.raises(UnsupportedSourceTransformation, match='configuration must be an object'):
        source.validate_source_transformations(TABLE)


@pytest.mark.parametrize('engine', ['mysql', 'mariadb'])
@pytest.mark.parametrize('kind', ['full', 'partial'])
def test_unsupported_regex_engine_does_not_create_recovery_or_export(tmp_path, engine, kind):
    run = _run(tmp_path, engine, kind, {
        'field_id': 'email', 'type': 'HASH', 'when': [{'column': 'email', 'regex_match': '.+'}],
    })
    run.source.conn_unbuffered = MagicMock()
    cursor = run.source.conn_unbuffered.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = pymysql.err.OperationalError(1139, 'Unsupported regex engine')
    export = full_runner._export_full_source if kind == 'full' else partial_runner._export_partial_source

    with pytest.raises(UnsupportedSourceTransformation, match='MySQL ICU or MariaDB PCRE'):
        export(run)

    assert run.attempt is None
    assert not list(tmp_path.iterdir())
    run.source.copy_table.assert_not_called()
    run.source.export_source_table_data.assert_not_called()
    run.publisher.inspect_table.assert_not_called()
