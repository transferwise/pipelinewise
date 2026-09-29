"""Reject incompatible native targets before export and preserve Iceberg recovery."""

import json
from argparse import Namespace
from unittest import mock

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    IcebergColumn,
    IcebergTableSpec,
    PUBLICATION_INSERT_OVERWRITE,
    PUBLICATION_PARTIAL_MERGE,
    RecoveryManifestError,
    SnowflakeIcebergPublisher,
    TABLE_FORMAT_NATIVE,
)
from pipelinewise.fastsync.commons.snowflake_types import (
    SNOWFLAKE_MAX_VARCHAR,
    canonical_native_metadata_type,
    canonical_native_type,
)
from pipelinewise.fastsync.partialsync import rdbms_to_snowflake, utils
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    FakeSnowflake,
    make_attempt,
    persist_attempt,
    v3_snapshot,
)


def _target(metadata):
    snowflake = mock.Mock()
    snowflake.query.return_value = [{'column_name': 'VALUE', 'data_type': json.dumps(metadata)}]
    return {'sf_object': snowflake, 'schema': 'TARGET', 'table': 'ORDERS'}


@pytest.mark.parametrize(('declared', 'expected'), [
    ('integer', 'NUMBER(38,0)'),
    ('numeric(18, 4)', 'NUMBER(18,4)'),
    ('decimal(18)', 'NUMBER(18,0)'),
    ('TEXT', 'VARCHAR(16777216)'),
    ('CHAR', 'VARCHAR(1)'),
    ('character varying(256)', 'VARCHAR(256)'),
    ('STRING(134217728)', SNOWFLAKE_MAX_VARCHAR),
    ('varbinary', 'BINARY(8388608)'),
    ('BINARY(256)', 'BINARY(256)'),
    ('DOUBLE PRECISION', 'FLOAT'),
    ('BOOL', 'BOOLEAN'),
    ('DATETIME', 'TIMESTAMP_NTZ(9)'),
    ('timestamp_ntz(6)', 'TIMESTAMP_NTZ(6)'),
    ('TIMESTAMP_TZ', 'TIMESTAMP_TZ(9)'),
    ('TIMESTAMP_LTZ(3)', 'TIMESTAMP_LTZ(3)'),
    ('TIME', 'TIME(9)'),
    ('DATE', 'DATE'),
    ('VARIANT', 'VARIANT'),
])
def test_canonical_native_types_preserve_load_dimensions(declared, expected):
    assert canonical_native_type(declared) == expected


@pytest.mark.parametrize('declared', [
    None, '', 'UNKNOWN', 'NUMBER(39,0)', 'NUMBER(5,6)', 'NUMBER(1,2,3)',
    'VARCHAR(0)', 'TIMESTAMP_NTZ(10)', 'BOOLEAN(1)', 'NUMBER NULL',
])
def test_invalid_native_types_fail_closed(declared):
    with pytest.raises(ValueError):
        canonical_native_type(declared)


@pytest.mark.parametrize('metadata', [
    {'type': 'FIXED', 'precision': 38},
    {'type': 'FIXED', 'precision': True, 'scale': 0},
    {'type': 'BINARY'},
    {'type': 'TIMESTAMP_NTZ'},
    {'type': 'TIME', 'scale': '9'},
])
def test_missing_native_catalog_dimensions_fail_closed(metadata):
    with pytest.raises(ValueError):
        canonical_native_metadata_type(metadata)


@pytest.mark.parametrize(('source_type', 'metadata'), [
    ('NUMBER', {'type': 'FIXED', 'precision': 38, 'scale': 0}),
    ('NUMERIC(18,2)', {'type': 'DECIMAL', 'precision': 38, 'scale': 4}),
    ('FLOAT', {'type': 'REAL'}),
    ('BINARY(16)', {'type': 'BINARY', 'length': 64}),
    ('TIMESTAMP_NTZ(6)', {'type': 'TIMESTAMP_NTZ', 'scale': 9}),
    ('TIME(6)', {'type': 'TIME', 'scale': 9}),
    ('BOOLEAN', {'type': 'BOOLEAN'}),
    ('VARIANT', {'type': 'VARIANT'}),
    (SNOWFLAKE_MAX_VARCHAR, {'type': 'TEXT', 'length': 16777216}),
])
def test_native_partial_accepts_same_family_with_sufficient_capacity(source_type, metadata):
    result = utils.diff_source_target_columns(_target(metadata), [f'"VALUE" {source_type}'])
    assert result['added_columns'] == {}
    assert result['varchar_columns_to_widen'] == (['VALUE'] if source_type == SNOWFLAKE_MAX_VARCHAR else [])


@pytest.mark.parametrize(('source_type', 'metadata'), [
    ('NUMBER', {'type': 'FIXED', 'precision': 38, 'scale': 2}),
    ('NUMBER(18,4)', {'type': 'FIXED', 'precision': 18, 'scale': 2}),
    ('NUMBER(18,2)', {'type': 'FIXED', 'precision': 18, 'scale': 4}),
    ('NUMBER', {'type': 'REAL'}),
    ('FLOAT', {'type': 'FIXED', 'precision': 38, 'scale': 0}),
    ('BINARY', {'type': 'BINARY', 'length': 1024}),
    ('BINARY', {'type': 'TEXT', 'length': 134217728}),
    ('TIMESTAMP_NTZ', {'type': 'TIMESTAMP_NTZ', 'scale': 6}),
    ('TIMESTAMP_NTZ', {'type': 'TIMESTAMP_TZ', 'scale': 9}),
    ('TIME', {'type': 'TIME', 'scale': 3}),
    ('BOOLEAN', {'type': 'FIXED', 'precision': 38, 'scale': 0}),
    ('VARIANT', {'type': 'TEXT', 'length': 134217728}),
    ('VARIANT', {'type': 'OBJECT'}),
])
def test_native_partial_rejects_type_or_capacity_loss(source_type, metadata):
    target = _target(metadata)
    with pytest.raises(utils.NativePartialSyncCompatibilityError, match='cannot safely publish.*FullSync'):
        utils.diff_source_target_columns(target, [f'"VALUE" {source_type}'])
    assert target['sf_object'].method_calls == [mock.call.query('SHOW COLUMNS IN TABLE TARGET."ORDERS"')]


def test_native_partial_normalizes_catalog_keys_and_unquoted_column_identifiers():
    target = _target({})
    target['sf_object'].query.return_value = [{
        'COLUMN_NAME': 'VALUE',
        'DATA_TYPE': json.dumps({'type': 'FIXED', 'precision': 38, 'scale': 0}),
    }]
    result = utils.diff_source_target_columns(target, ['value INTEGER'])
    assert result['added_columns'] == {}
    assert result['source_columns'] == {'"VALUE"': 'INTEGER'}


def test_native_partial_checks_types_before_bookmark_export_or_staging(tmp_path):
    state = tmp_path / 'state.json'
    state.write_text('{"bookmarks": {}}', encoding='utf8')
    args = Namespace(
        target={'dbname': 'TEST_DB', 'default_target_schema': 'TARGET'}, tap={},
        transform={}, properties={}, state=str(state), temp_dir=str(tmp_path),
    )
    source = mock.Mock()
    source.map_column_types_to_target.return_value = {
        'columns': ['"ID" NUMBER', '"VALUE" NUMBER'],
        'source_column_names': ['id', 'value'], 'primary_key': ['"ID"'],
    }
    source_adapter = mock.Mock()
    source_adapter.create.return_value = source
    target = _target({'type': 'REAL'})
    table = ('source.orders', {'column': 'id', 'start_value': '<S>1', 'end_value': None, 'drop_target_table': False})

    with mock.patch.object(
        rdbms_to_snowflake.iceberg_routes, 'require_native_target_format', return_value=TABLE_FORMAT_NATIVE,
    ), mock.patch.object(rdbms_to_snowflake.common_utils, 'get_bookmark_for_table') as bookmark:
        result = rdbms_to_snowflake.partial_sync_table(
            table, args, source_adapter, mock.Mock(return_value=target['sf_object']), mock.Mock(),
        )

    assert 'cannot safely publish "VALUE" as NUMBER(38,0)' in result
    bookmark.assert_not_called()
    source.export_source_table_data.assert_not_called()
    source_adapter.close_partial.assert_called_once_with(source)
    assert state.read_text(encoding='utf8') == '{"bookmarks": {}}'
    assert list(tmp_path.iterdir()) == [state]
    assert target['sf_object'].method_calls == [mock.call.query('SHOW COLUMNS IN TABLE TARGET."ORDERS"')]


@pytest.mark.parametrize(('exists', 'drop_target'), [(False, False), (True, True)])
def test_native_missing_or_replaced_target_does_not_restrict_export(exists, drop_target):
    run = mock.Mock(native_target_exists=exists, has_dynamic_boundary=False)
    run.table_name = 'source.orders'
    run.column_name = 'id'
    run.args.drop_target_table = drop_target
    run.source.map_column_types_to_target.return_value = {
        'columns': ['"ID" NUMBER'], 'source_column_names': ['id'], 'primary_key': ['"ID"'],
    }
    run.source_adapter.bookmark_kwargs.return_value = {}
    with mock.patch.object(
        rdbms_to_snowflake, '_resolve_partial_boundary', return_value=('1', None),
    ), mock.patch.object(
        rdbms_to_snowflake.common_utils, 'get_bookmark_for_table', return_value={'lsn': 1},
    ):
        assert rdbms_to_snowflake._prepare_native_partial_export(run) is True
    run.snowflake.query.assert_not_called()


@pytest.mark.parametrize(
    ('kind', 'method'), [('full', PUBLICATION_INSERT_OVERWRITE), ('partial', PUBLICATION_PARTIAL_MERGE)],
)
@pytest.mark.parametrize('actual_type', ['FLOAT', 'VARIANT', 'BINARY', 'TIMESTAMP_NTZ'])
def test_iceberg_recovery_type_drift_keeps_manifest_and_staging(tmp_path, kind, method, actual_type):
    spec = IcebergTableSpec.from_fastsync(
        'TEST_DB', 'TARGET', 'ORDERS', ['"ID" NUMBER', '"VALUE" VARCHAR'], ['"ID"'],
    )
    changed = IcebergTableSpec(
        spec.name,
        tuple(IcebergColumn(column.name, actual_type) if column.name == 'VALUE' else column for column in spec.columns),
        spec.primary_key,
    )
    snowflake = FakeSnowflake()
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
    publisher.inspect_table = mock.Mock(return_value=v3_snapshot(changed))
    attempt = make_attempt(spec, kind=kind, method=method, snapshot=v3_snapshot(spec))
    persist_attempt(publisher, attempt)
    saved = {path: path.read_bytes() for path in tmp_path.glob('*.json')}
    assert saved

    with pytest.raises(RecoveryManifestError, match='target changed'):
        if kind == 'full':
            publisher.plan_full_sync(attempt, spec)
        else:
            publisher.plan_partial_sync(attempt, spec)

    assert {path: path.read_bytes() for path in tmp_path.glob('*.json')} == saved
    assert snowflake.queries == []
    assert snowflake.transactions == []
