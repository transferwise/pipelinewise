"""MariaDB JSON uses VARIANT for new columns and retains established text during PartialSync."""

import csv
import gzip
import json
from argparse import Namespace
from unittest.mock import MagicMock, Mock, patch

import pytest

from pipelinewise.fastsync.commons.rdbms_source import RdbmsSnowflakeSource
from pipelinewise.fastsync.commons.snowflake_decimal_evolution import with_retained_mariadb_json_types
from pipelinewise.fastsync.commons.snowflake_iceberg import SnowflakeIcebergPublisher
from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    IcebergTableSpec, IcebergPublicationAttempt, SnowflakeTableMetadata, PUBLICATION_REPLACEMENT_CTAS,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import RecoveryManifestError, TableCompatibilityError
from pipelinewise.fastsync.commons.source_transformations import UnsupportedSourceTransformation
from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from pipelinewise.fastsync.mysql_to_snowflake import tap_type_to_target_type
from pipelinewise.fastsync.partialsync import rdbms_to_snowflake as runner
from pipelinewise.fastsync.partialsync import utils
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    FakeSnowflake, RECOVERY_IDENTITY, v3_snapshot,
)


TABLE = 'source.orders'
COLUMNS = [
    {'column_name': 'id', 'data_type': 'int', 'column_type': 'int', 'safe_sql_value': '`id`'},
    {'column_name': 'payload', 'data_type': 'json', 'column_type': 'longtext', 'safe_sql_value': '`payload`'},
    {'column_name': 'fresh', 'data_type': 'json', 'column_type': 'longtext', 'safe_sql_value': '`fresh`'},
]


def _source(iceberg_version=None):
    adapter = RdbmsSnowflakeSource.mysql(FastSyncTapMySql, tap_type_to_target_type)
    args = Namespace(tap={'engine': 'mariadb', 'json_target': 'snowflake', 'export_batch_rows': 100}, transform=None)
    source = adapter.create(args, iceberg_version)
    source.get_table_columns = Mock(return_value=COLUMNS)
    source.get_primary_keys = Mock(return_value=['"ID"'])
    return source


def _spec(payload_type='VARIANT', fresh=True):
    columns = ['"ID" NUMBER', f'"PAYLOAD" {payload_type}']
    if fresh:
        columns.append('"FRESH" VARIANT')
    return IcebergTableSpec.from_fastsync('DB', 'SCHEMA', 'ORDERS', columns, ['ID'])


def _target_rows(width=16777216):
    return [
        {'column_name': 'ID', 'data_type': json.dumps({'type': 'FIXED', 'precision': 38, 'scale': 0})},
        {'column_name': 'PAYLOAD', 'data_type': json.dumps({'type': 'TEXT', 'length': width})},
    ]


@pytest.mark.parametrize('version', [None, 3])
def test_snowflake_routes_recognize_json_aliases_with_provenance(version):
    source = _source(version)
    mapped = source.map_column_types_to_target(TABLE)

    assert source.uses_mariadb_json_aliases
    assert mapped['columns'] == ['"ID" NUMBER', '"PAYLOAD" VARIANT', '"FRESH" VARIANT']
    assert mapped['mariadb_json_columns'] == ['PAYLOAD', 'FRESH']


@pytest.mark.parametrize(('tap_config', 'expected'), [
    ({'engine': 'mariadb'}, False),
    ({'engine': 'mariadb', 'target_table_format': 'native'}, False),
    ({'engine': 'mariadb', 'json_target': 'snowflake'}, True),
    ({'engine': 'mariadb', 'target_table_format': 'iceberg', 'iceberg_version': 3}, True),
    ({'engine': 'mariadb', 'target_table_format': 'iceberg', 'iceberg_version': 3,
      'json_target': 'postgres'}, False),
    ({'engine': 'mariadb', 'target_table_format': 'iceberg', 'iceberg_version': 2,
      'json_target': 'snowflake'}, False),
    ({'engine': 'mysql', 'json_target': 'snowflake'}, False),
])
def test_json_alias_detection_waits_for_generated_singer_config(tap_config, expected):
    """FastSync must not create VARIANT before Singer enables the same MariaDB alias discovery."""
    adapter = RdbmsSnowflakeSource.mysql(FastSyncTapMySql, tap_type_to_target_type)
    source = adapter.create(Namespace(tap=tap_config, transform=None), tap_config.get('iceberg_version'))

    assert source.uses_mariadb_json_aliases is expected
    with patch.object(source, 'query', return_value=[]) as query:
        source.get_table_columns(TABLE)
    assert ("CASE WHEN is_json_alias THEN 'json' ELSE data_type END" in query.call_args.args[0]) is expected


def test_other_destinations_do_not_enable_native_mariadb_alias_detection():
    source = FastSyncTapMySql({'engine': 'mariadb'}, lambda *_: 'CHARACTER VARYING')
    assert not source.uses_mariadb_json_aliases


@pytest.mark.parametrize('width', [64, 16777216, 134217728])
@pytest.mark.parametrize('version_legacy_float_columns', [False, True])
def test_native_partial_retains_actual_json_text_and_adds_variant(width, version_legacy_float_columns):
    client = Mock()
    client.query.return_value = _target_rows(width)
    target = {'sf_object': client, 'schema': 'SCHEMA', 'table': 'ORDERS'}
    mapped = _source().map_column_types_to_target(TABLE)

    changes = utils.diff_source_target_columns(
        target, mapped['columns'], mariadb_json_columns=mapped['mariadb_json_columns'],
        version_legacy_float_columns=version_legacy_float_columns,
    )
    assert changes['staging_columns'] == ['"ID" NUMBER', f'"PAYLOAD" VARCHAR({width})', '"FRESH" VARIANT']
    assert changes['added_columns'] == {'"FRESH"': 'VARIANT'}
    assert changes['column_versions'] == {}
    assert changes['varchar_columns_to_widen'] == []
    report = utils.report_source_target_columns(
        target, mapped['columns'], client.query.return_value, mariadb_json_columns=mapped['mariadb_json_columns'],
    )
    assert [row['status'] for row in report] == ['compatible', 'compatible', 'would_add']


def test_native_generic_variant_text_mismatch_is_still_rejected():
    target = {'sf_object': Mock(), 'schema': 'SCHEMA', 'table': 'ORDERS'}
    target['sf_object'].query.return_value = _target_rows()
    with pytest.raises(utils.NativePartialSyncCompatibilityError, match='cannot safely publish'):
        utils.diff_source_target_columns(target, ['"PAYLOAD" VARIANT'])


def test_iceberg_retains_only_marked_json_text_and_keeps_new_variant():
    mapped, actual = _spec(), _spec('VARCHAR(134217728)', fresh=False)
    retained = with_retained_mariadb_json_types(mapped, actual, ('PAYLOAD', 'FRESH'))

    assert runner._source_column_definitions(retained) == [
        '"ID" NUMBER(38,0) NOT NULL', '"PAYLOAD" VARCHAR(134217728)', '"FRESH" VARIANT',
    ]
    assert with_retained_mariadb_json_types(mapped, actual) == mapped
    assert with_retained_mariadb_json_types(mapped, None, ('PAYLOAD',)) == mapped


@pytest.mark.parametrize('version', [None, 3])
@pytest.mark.parametrize('transform', ['HASH', 'MASK-HIDDEN'])
def test_retained_json_text_keeps_source_text_transformations(version, transform):
    source = _source(version)
    source.source_transformations = {'transformations': [{
        'tap_stream_name': 'source-orders', 'field_id': 'payload', 'type': transform,
    }]}
    source.retained_column_types = {'PAYLOAD': 'VARCHAR(16777216)'}

    projection = source._compile_source_projection(TABLE, COLUMNS)
    assert 'PARSE_JSON' not in projection.upper()
    assert ('SHA2(' if transform == 'HASH' else "X'68696464656e'") in projection

    source.retained_column_types = {}
    with pytest.raises(UnsupportedSourceTransformation, match='mapped target type VARIANT'):
        source._compile_source_projection(TABLE, COLUMNS)


@pytest.mark.parametrize('version', [None, 3])
def test_mysql_csv_keeps_json_roots_and_sql_null_verbatim(tmp_path, version):
    source = _source(version)
    source.get_table_columns.return_value = COLUMNS[:2]
    values = [' { "k" : [null,{}] } ', '[1,null,{}]', '"text"', '""', '42', '1.50', 'true', 'false', 'null', None]
    source.conn_unbuffered = MagicMock()
    cursor = source.conn_unbuffered.cursor.return_value.__enter__.return_value
    cursor.fetchmany.side_effect = [[(index, value, None, None, None) for index, value in enumerate(values)], []]
    path = tmp_path / 'rows.csv.gz'

    source.copy_table(TABLE, str(path))

    with gzip.open(path, 'rt', encoding='utf8', newline='') as handle:
        rows = list(csv.reader(handle))
    assert [row[1] for row in rows] == [value if value is not None else '' for value in values]
    assert rows[-2][1] == 'null'
    assert rows[-1][1] == ''


def _run(tmp_path, drop_target=False):
    source = _source(3)
    args = Namespace(
        target={'dbname': 'DB', 'default_target_schema': 'SCHEMA', 'target_table_format': 'iceberg',
                'iceberg_version': 3},
        properties={}, state='unused', end_value=None, drop_target_table=drop_target, transform=None,
    )
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    publisher.inspect_table = Mock(return_value=v3_snapshot(_spec('VARCHAR(134217728)', fresh=False)))
    adapter = Mock()
    adapter.resolved_source_engine.return_value = 'mariadb'
    adapter.bookmark_kwargs.return_value = {}
    return runner._PartialSyncRun(
        table=(TABLE, {'column': 'id', 'start_value': '<S>1', 'end_value': '<S>2'}),
        args=args, source_adapter=adapter, snowflake=Mock(), logger=Mock(), iceberg_version=3,
        iceberg_requested=True, source=source, publisher=publisher, table_name=TABLE, column_name='id',
        target_schema='SCHEMA', recovery_identity=RECOVERY_IDENTITY, staging_config={},
    )


def test_iceberg_partial_persists_retained_schema_and_alias_provenance(tmp_path):
    run = _run(tmp_path)
    with patch.object(runner.common_utils, 'get_bookmark_for_table', return_value={}):
        assert runner._prepare_iceberg_partial_export(run)

    recovered = IcebergPublicationAttempt.from_dict(run.attempt.as_dict())
    assert recovered.manifest_payload.mariadb_json_columns == ['FRESH', 'PAYLOAD']
    assert recovered.table_spec == run.spec
    assert run.source.retained_column_types == {'PAYLOAD': 'VARCHAR(134217728)'}
    plan = run.publisher.plan_partial_sync(recovered, recovered.table_spec)
    assert plan.column_renames == ()
    assert plan.preparation_statements == ('ALTER ICEBERG TABLE "DB"."SCHEMA"."ORDERS" ADD COLUMN "FRESH" VARIANT',)

    run.attempt = recovered
    with patch.object(runner.common_utils, 'get_bookmark_for_table', side_effect=AssertionError('new bookmark')):
        assert runner._prepare_iceberg_partial_export(run)
        runner._validate_partial_export(run)


@pytest.mark.parametrize('drop_target', [False, True])
def test_native_partial_preflight_passes_effective_types_to_source_compiler(tmp_path, drop_target):
    run = _run(tmp_path, drop_target=drop_target)
    run.source = _source()
    run.iceberg_requested = False
    run.native_target_exists = True
    run.target_table = 'ORDERS'
    run.snowflake.query.return_value = _target_rows()

    with patch.object(runner.common_utils, 'get_bookmark_for_table', return_value={}):
        assert runner._prepare_native_partial_export(run)

    expected_type = 'VARIANT' if drop_target else 'VARCHAR(16777216)'
    assert run.source_columns == ['"ID" NUMBER', f'"PAYLOAD" {expected_type}', '"FRESH" VARIANT']
    assert run.source.retained_column_types == ({} if drop_target else {'PAYLOAD': expected_type})


def test_iceberg_fullsync_rebuild_adopts_variant_only_for_marked_mariadb_json(tmp_path):
    run = _run(tmp_path)
    run.publisher._preflight_replacement = Mock(return_value=SnowflakeTableMetadata())
    run.publisher._verify_replacement_metadata = Mock()
    mapped = _spec()

    with pytest.raises(TableCompatibilityError, match='explicit TEXT/VARIANT migration'):
        run.publisher.prepare_full_sync(mapped, {}, recovery_identity=RECOVERY_IDENTITY)

    attempt = run.publisher.prepare_full_sync(
        mapped, {}, recovery_identity=RECOVERY_IDENTITY, resolved_source_engine='mariadb',
        mariadb_json_columns=('PAYLOAD', 'FRESH'),
    )
    recovered = IcebergPublicationAttempt.from_dict(attempt.as_dict())
    assert recovered.manifest_payload.mariadb_json_columns == ['FRESH', 'PAYLOAD']
    assert recovered.table_spec == mapped
    plan = run.publisher.plan_full_sync(recovered, mapped)
    assert plan.method == PUBLICATION_REPLACEMENT_CTAS
    assert '"PAYLOAD"' in plan.publication_statements[0]
    assert '"PAYLOAD" VARIANT' in plan.publication_statements[0]


def test_iceberg_explicit_partial_replacement_adopts_variant(tmp_path):
    run = _run(tmp_path, drop_target=True)
    run.publisher._preflight_replacement = Mock(return_value=SnowflakeTableMetadata())
    run.publisher._verify_replacement_metadata = Mock()
    with patch.object(runner.common_utils, 'get_bookmark_for_table', return_value={}):
        assert runner._prepare_iceberg_partial_export(run)

    assert run.source.retained_column_types == {}
    assert next(column for column in run.spec.columns if column.name == 'PAYLOAD').data_type == 'VARIANT'
    assert run.attempt.manifest_payload.mariadb_json_columns == ['FRESH', 'PAYLOAD']


def test_iceberg_json_reexport_rejects_old_or_changed_alias_evidence(tmp_path):
    run = _run(tmp_path)
    with patch.object(runner.common_utils, 'get_bookmark_for_table', return_value={}):
        runner._prepare_iceberg_partial_export(run)
    context = run.attempt.manifest_payload.as_context()
    del context['mariadb_json_columns']
    serialized = run.attempt.as_dict()
    serialized['context'] = context
    serialized['payload']['values'] = context
    run.attempt = IcebergPublicationAttempt.from_dict(serialized)

    with pytest.raises(RecoveryManifestError, match='JSON alias mapping changed'):
        runner._prepare_iceberg_partial_export(run)


def test_iceberg_retained_text_target_drift_is_rejected_on_reexport(tmp_path):
    run = _run(tmp_path)
    with patch.object(runner.common_utils, 'get_bookmark_for_table', return_value={}):
        runner._prepare_iceberg_partial_export(run)
    run.publisher.inspect_table.return_value = v3_snapshot(_spec('VARIANT', fresh=False))

    with pytest.raises(TableCompatibilityError, match='changed columns: PAYLOAD'):
        runner._prepare_iceberg_partial_export(run)


def test_durable_partial_plan_uses_saved_text_and_refuses_target_variant_drift(tmp_path):
    run = _run(tmp_path)
    with patch.object(runner.common_utils, 'get_bookmark_for_table', return_value={}):
        runner._prepare_iceberg_partial_export(run)
    recovered = IcebergPublicationAttempt.from_dict(run.attempt.as_dict())
    run.source.map_column_types_to_target = Mock(side_effect=AssertionError('durable staging needs no source'))

    assert run.publisher.plan_partial_sync(recovered, recovered.table_spec).column_renames == ()
    run.publisher.inspect_table.return_value = v3_snapshot(_spec('VARIANT', fresh=False))
    with pytest.raises(RecoveryManifestError, match='target changed'):
        run.publisher.plan_partial_sync(recovered, recovered.table_spec)


def test_old_attempt_without_aliases_keeps_legacy_reexport_contract(tmp_path):
    run = _run(tmp_path)
    with patch.object(runner.common_utils, 'get_bookmark_for_table', return_value={}):
        runner._prepare_iceberg_partial_export(run)
    serialized = run.attempt.as_dict()
    del serialized['context']['mariadb_json_columns']
    serialized['payload']['values'] = serialized['context']
    old = IcebergPublicationAttempt.from_dict(serialized)

    runner.iceberg_routes.validate_recovery_mariadb_json_columns(old, ())
    with pytest.raises(RecoveryManifestError, match='alias mapping changed'):
        runner.iceberg_routes.validate_recovery_mariadb_json_columns(old, ('PAYLOAD',))


@pytest.mark.parametrize('bad_value', ['PAYLOAD', ['PAYLOAD', 'PAYLOAD'], [None]])
def test_alias_provenance_rejects_malformed_durable_state(tmp_path, bad_value):
    run = _run(tmp_path)
    with patch.object(runner.common_utils, 'get_bookmark_for_table', return_value={}):
        runner._prepare_iceberg_partial_export(run)
    serialized = run.attempt.as_dict()
    serialized['context']['mariadb_json_columns'] = bad_value
    serialized['payload']['values']['mariadb_json_columns'] = bad_value
    with pytest.raises((RecoveryManifestError, ValueError)):
        IcebergPublicationAttempt.from_dict(serialized)
