"""Decimal evolution preserves history and resumes independently committed DDL."""

from dataclasses import replace
from argparse import Namespace
import json
import logging
from unittest.mock import Mock, patch

import pytest

from pipelinewise.fastsync.commons.snowflake_decimal_evolution import (
    partial_compatibility, partial_preparation, plan_column_versions, with_retained_decimal_types,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_model import IcebergColumn, IcebergTableSpec
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import RecoveryManifestError, TableCompatibilityError
from pipelinewise.fastsync.commons.snowflake_iceberg_publication import SnowflakeIcebergPublicationService
from pipelinewise.fastsync.commons.snowflake_iceberg_model import PUBLICATION_PARTIAL_MERGE
from pipelinewise.fastsync.commons.snowflake_iceberg import SnowflakeIcebergPublisher
from pipelinewise.fastsync.commons.partial_sync_boundary import PartialSyncBoundary
from pipelinewise.fastsync.partialsync.utils import diff_source_target_columns, NativePartialSyncCompatibilityError
from pipelinewise.fastsync.partialsync import utils
from pipelinewise.fastsync.partialsync import rdbms_to_snowflake
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    FakeSnowflake, RECOVERY_IDENTITY, make_attempt, persist_attempt, v3_snapshot,
)


def _spec(amount_type):
    return IcebergTableSpec.from_fastsync(
        'DB', 'SCHEMA', 'TABLE', ['ID NUMBER', f'AMOUNT {amount_type}'], ['ID'],
    )


@pytest.mark.parametrize('old_type', ('FLOAT', 'NUMERIC(18,2)', 'NUMERIC(38,12)'))
def test_iceberg_decimal_change_plans_rename_and_new_empty_column(old_type):
    expected, actual = _spec('NUMERIC(38,18)'), _spec(old_type)
    versions = plan_column_versions(
        expected, actual, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
    )
    preparation = partial_preparation(
        expected, actual, versions, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
    )
    statements = preparation.statements
    assert len(statements) == 2
    assert 'RENAME COLUMN "AMOUNT" TO "AMOUNT_' in statements[0]
    assert statements[1].endswith('ADD COLUMN "AMOUNT" NUMBER(38,18)')
    assert len(preparation.column_renames) == 1
    rename = preparation.column_renames[0]
    assert (rename.statement_index, rename.column_name, rename.archived_name) == (
        0, 'AMOUNT', versions['AMOUNT']['archived_name'],
    )


def test_iceberg_legacy_decimal_float_stays_in_place_without_opt_in():
    expected, actual = _spec('NUMERIC(38,18)'), _spec('FLOAT')

    assert partial_compatibility(
        expected, actual, decimal_columns=('AMOUNT',),
    ) == ('exact', ())
    assert plan_column_versions(expected, actual, decimal_columns=('AMOUNT',)) == {}


@pytest.mark.parametrize('version_legacy_float_columns', [False, True])
def test_iceberg_postgres_decimal_key_always_retains_float_staging_type(version_legacy_float_columns):
    expected = replace(_spec('VARCHAR(134217728)'), primary_key=('AMOUNT',))
    actual = replace(_spec('DOUBLE'), primary_key=('AMOUNT',))

    retained = with_retained_decimal_types(
        expected,
        actual,
        decimal_columns=('AMOUNT',),
        version_legacy_float_columns=version_legacy_float_columns,
    )

    assert next(column for column in retained.columns if column.name == 'AMOUNT').data_type == 'DOUBLE'
    assert partial_compatibility(retained, actual, decimal_columns=('AMOUNT',))[0] == 'exact'
    assert plan_column_versions(
        expected,
        actual,
        decimal_columns=('AMOUNT',),
        version_legacy_float_columns=version_legacy_float_columns,
    ) == {}
    assert partial_preparation(
        expected, actual, {}, decimal_columns=('AMOUNT',),
    ).statements == ()


def test_retained_iceberg_staging_definitions_exclude_pipelinewise_metadata():
    retained = with_retained_decimal_types(
        _spec('NUMERIC(38,18)'),
        _spec('FLOAT'),
        decimal_columns=('AMOUNT',),
    )

    assert rdbms_to_snowflake._source_column_definitions(retained) == [
        '"ID" NUMBER(38,0) NOT NULL',
        '"AMOUNT" DOUBLE',
    ]


def test_partial_export_validation_reapplies_retained_decimal_types():
    mapped = _spec('NUMERIC(38,18)')
    retained = with_retained_decimal_types(
        mapped,
        _spec('FLOAT'),
        decimal_columns=('AMOUNT',),
    )
    source_types = {
        'columns': ['"ID" NUMBER', '"AMOUNT" NUMERIC(38,18)'],
        'primary_key': ['"ID"'],
        'source_column_names': ['ID', 'AMOUNT'],
    }
    run = Namespace(
        source=Mock(map_column_types_to_target=Mock(return_value=source_types)),
        boundary=Mock(),
        args=Namespace(target={}),
        target_schema='SCHEMA',
        table_name='TABLE',
        spec=retained,
        decimal_columns=('AMOUNT',),
    )

    with patch.object(
        rdbms_to_snowflake.iceberg_routes,
        'create_spec',
        return_value=mapped,
    ), patch.object(
        rdbms_to_snowflake.iceberg_routes,
        'validate_recovery_source_spec',
    ) as validate:
        rdbms_to_snowflake._validate_partial_export(run)

    validate.assert_called_once_with(retained, retained)


@pytest.mark.parametrize(('current', 'mapped'), (
    ('NUMERIC(18,2)', 'FLOAT'),
    ('NUMERIC(18,2)', 'DOUBLE'),
    ('NUMERIC(18,0)', 'NUMERIC(38,0)'),
))
def test_iceberg_unrelated_numeric_drift_never_versions_without_decimal_origin(current, mapped):
    expected, actual = _spec(mapped), _spec(current)
    assert partial_compatibility(expected, actual)[0] == 'incompatible'
    with pytest.raises(TableCompatibilityError, match='incompatible'):
        plan_column_versions(expected, actual)


def test_iceberg_recovery_rejects_numeric_drift_absent_from_persisted_plan(tmp_path):
    expected = _spec('FLOAT')
    client = FakeSnowflake()
    publisher = SnowflakeIcebergPublisher(client, str(tmp_path))
    publisher.inspect_table = Mock(side_effect=[
        v3_snapshot(expected), v3_snapshot(_spec('NUMERIC(18,2)')),
    ])
    attempt = publisher.prepare_partial_sync(
        expected, {}, PartialSyncBoundary('ID', 1, 10), recovery_identity=RECOVERY_IDENTITY,
    )
    assert attempt.manifest_payload.column_versions == {}
    with pytest.raises(RecoveryManifestError, match='target changed'):
        publisher.plan_partial_sync(attempt, expected)
    assert client.queries == []


def test_partial_ddl_retry_reuses_archive_and_does_not_rename_again():
    expected, actual = _spec('NUMERIC(38,18)'), _spec('FLOAT')
    versions = plan_column_versions(
        expected, actual, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
    )
    archive = IcebergColumn(versions['AMOUNT']['archived_name'], 'FLOAT')
    after_rename = replace(actual, columns=tuple(
        archive if column.name == 'AMOUNT' else column for column in actual.columns
    ))
    preparation = partial_preparation(
        expected, after_rename, versions, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
    )
    statements = preparation.statements
    assert len(statements) == 1 and 'ADD COLUMN "AMOUNT" NUMBER(38,18)' in statements[0]
    assert preparation.column_renames == ()
    after_add = replace(expected, columns=expected.columns + (archive,))
    assert partial_preparation(
        expected, after_add, versions, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
    ).statements == ()
    assert partial_compatibility(expected, after_add) == ('exact', ())


def test_partial_recovery_rejects_changed_archive():
    expected, actual = _spec('NUMERIC(38,18)'), _spec('FLOAT')
    versions = plan_column_versions(
        expected, actual, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
    )
    changed_archive = IcebergColumn(versions['AMOUNT']['archived_name'], 'NUMBER(18,2)')
    changed = replace(expected, columns=expected.columns + (changed_archive,))
    with pytest.raises(RecoveryManifestError, match='Historical decimal column changed'):
        partial_preparation(
            expected, changed, versions, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
        )


@pytest.mark.parametrize('extra_name', ('UNEXPECTED', 'OTHER_20260929_123456_123456'))
def test_partial_does_not_accept_unrelated_extra_columns(extra_name):
    expected = _spec('NUMERIC(38,18)')
    actual = replace(expected, columns=expected.columns + (IcebergColumn(extra_name, 'FLOAT'),))
    assert partial_compatibility(expected, actual)[0] == 'incompatible'


def test_numeric_key_type_change_is_rejected_before_planning_mutations():
    expected, actual = _spec('NUMERIC(38,18)'), _spec('NUMERIC(18,2)')
    expected = replace(expected, primary_key=('AMOUNT',))
    actual = replace(actual, primary_key=('AMOUNT',))
    with pytest.raises(TableCompatibilityError, match='primary-key column AMOUNT'):
        plan_column_versions(
            expected, actual, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
        )


@pytest.mark.parametrize('current', (
    {'type': 'REAL'}, {'type': 'FIXED', 'precision': 18, 'scale': 2},
))
def test_native_partial_versions_decimal_definition_changes(current):
    client = Mock()
    client.query.return_value = [{'column_name': 'AMOUNT', 'data_type': json.dumps(current)}]
    target = {'sf_object': client, 'schema': 'SCHEMA', 'table': 'TABLE'}
    changes = diff_source_target_columns(
        target, ['"AMOUNT" NUMERIC(38,18)'], primary_keys=['ID'], decimal_columns=('AMOUNT',),
        version_legacy_float_columns=True,
    )
    assert changes['column_versions'] == {'"AMOUNT"': 'NUMERIC(38,18)'}
    assert all(call.args[0].startswith('SHOW COLUMNS') for call in client.query.call_args_list)


def test_native_partial_rejects_non_float_decimal_key_change():
    client = Mock()
    client.query.return_value = [{
        'column_name': 'AMOUNT',
        'data_type': json.dumps({'type': 'FIXED', 'precision': 18, 'scale': 2}),
    }]
    target = {'sf_object': client, 'schema': 'SCHEMA', 'table': 'TABLE'}
    with pytest.raises(NativePartialSyncCompatibilityError, match='primary-key column'):
        diff_source_target_columns(
            target,
            ['"AMOUNT" NUMERIC(38,18)'],
            primary_keys=['AMOUNT'],
            decimal_columns=('AMOUNT',),
            version_legacy_float_columns=True,
        )


def test_native_partial_keeps_legacy_decimal_float_without_opt_in():
    client = Mock()
    columns = [{'column_name': 'AMOUNT', 'data_type': json.dumps({'type': 'REAL'})}]
    client.query.return_value = columns
    target = {'sf_object': client, 'schema': 'SCHEMA', 'table': 'TABLE'}
    source = ['"AMOUNT" NUMERIC(38,18)']

    assert diff_source_target_columns(
        target, source, primary_keys=['AMOUNT'], boundary_column='amount', decimal_columns=('AMOUNT',),
    )['column_versions'] == {}
    assert utils.report_source_target_columns(
        target, source, columns, primary_keys=['AMOUNT'], boundary_column='amount', decimal_columns=('AMOUNT',),
    )[0]['status'] == 'compatible'


@pytest.mark.parametrize('version_legacy_float_columns', [False, True])
def test_native_partial_postgres_decimal_key_always_retains_float_staging_type(version_legacy_float_columns):
    client = Mock()
    columns = [{'column_name': 'ID', 'data_type': json.dumps({'type': 'REAL'})}]
    client.query.return_value = columns
    target = {'sf_object': client, 'schema': 'SCHEMA', 'table': 'TABLE'}
    source = ['"ID" VARCHAR(134217728)']

    changes = diff_source_target_columns(
        target,
        source,
        primary_keys=['ID'],
        decimal_columns=('ID',),
        version_legacy_float_columns=version_legacy_float_columns,
    )

    assert changes['column_versions'] == {}
    assert changes['staging_columns'] == ['"ID" FLOAT']
    assert utils.report_source_target_columns(
        target,
        source,
        columns,
        primary_keys=['ID'],
        decimal_columns=('ID',),
        version_legacy_float_columns=version_legacy_float_columns,
    )[0]['status'] == 'compatible'


@pytest.mark.parametrize(('source', 'current'), (
    ('"AMOUNT" FLOAT', {'type': 'FIXED', 'precision': 18, 'scale': 2}),
    ('"AMOUNT" NUMBER', {'type': 'FIXED', 'precision': 18, 'scale': 0}),
))
def test_native_unrelated_numeric_drift_never_versions_without_decimal_origin(source, current):
    client = Mock()
    client.query.return_value = [{'column_name': 'AMOUNT', 'data_type': json.dumps(current)}]
    target = {'sf_object': client, 'schema': 'SCHEMA', 'table': 'TABLE'}
    with pytest.raises(NativePartialSyncCompatibilityError, match='cannot safely publish'):
        diff_source_target_columns(target, [source])
    report = utils.report_source_target_columns(target, [source], client.query.return_value)
    assert report[0]['status'] == 'incompatible'
    assert all(call.args[0].startswith('SHOW COLUMNS') for call in client.query.call_args_list)


def test_iceberg_cannot_version_the_partial_range_column():
    expected, actual = _spec('NUMERIC(38,18)'), _spec('FLOAT')
    with pytest.raises(TableCompatibilityError, match='boundary column.*FullSync'):
        plan_column_versions(
            expected, actual, boundary_column='amount', decimal_columns=('AMOUNT',),
            version_legacy_float_columns=True,
        )


def test_iceberg_archive_cannot_take_a_new_source_column_name():
    expected, actual = _spec('NUMERIC(38,18)'), _spec('FLOAT')
    expected = replace(expected, columns=expected.columns + (IcebergColumn('NEW', 'FLOAT'),))
    with patch(
        'pipelinewise.fastsync.commons.snowflake_decimal_evolution.versioned_column_name', return_value='NEW',
    ):
        with pytest.raises(TableCompatibilityError, match='Historical column already exists: NEW'):
            plan_column_versions(
                expected, actual, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
            )


def test_iceberg_retry_rejects_a_persisted_boundary_column_rename():
    expected, actual = _spec('NUMERIC(38,18)'), _spec('FLOAT')
    versions = plan_column_versions(
        expected, actual, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
    )
    archive = IcebergColumn(versions['AMOUNT']['archived_name'], 'FLOAT')
    after_rename = replace(actual, columns=tuple(
        archive if column.name == 'AMOUNT' else column for column in actual.columns
    ))
    with pytest.raises(TableCompatibilityError, match='boundary column.*FullSync'):
        partial_preparation(
            expected, after_rename, versions, boundary_column='amount', decimal_columns=('AMOUNT',),
            version_legacy_float_columns=True,
        )


def test_native_boundary_type_change_is_rejected_before_mutation():
    client = Mock()
    client.query.return_value = [{'column_name': 'AMOUNT', 'data_type': json.dumps({'type': 'REAL'})}]
    with pytest.raises(NativePartialSyncCompatibilityError, match='boundary column.*FullSync'):
        diff_source_target_columns(
            {'sf_object': client, 'schema': 'SCHEMA', 'table': 'TABLE'},
            ['"AMOUNT" NUMERIC(38,18)'], primary_keys=['ID'], boundary_column='amount',
            decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
        )
    assert all(call.args[0].startswith('SHOW COLUMNS') for call in client.query.call_args_list)


@pytest.mark.parametrize('current_exists', [False, True])
def test_iceberg_historical_boundary_is_rejected_even_after_its_type_matches(current_exists):
    expected = _spec('NUMERIC(38,18)')
    archive = IcebergColumn('AMOUNT_20260929_123456_123456', 'FLOAT')
    actual = replace(expected, columns=tuple(
        column for column in expected.columns if current_exists or column.name != 'AMOUNT'
    ) + (archive,))
    with pytest.raises(TableCompatibilityError, match='boundary column.*historical versions.*FullSync'):
        plan_column_versions(expected, actual, boundary_column='amount')
    with pytest.raises(TableCompatibilityError, match='boundary column.*historical versions.*FullSync'):
        partial_preparation(expected, actual, {}, boundary_column='amount')


@pytest.mark.parametrize('current_exists', [False, True])
def test_native_historical_boundary_is_rejected_even_after_its_type_matches(current_exists):
    client = Mock()
    client.query.return_value = [
        {'column_name': 'AMOUNT_20260929_123456_123456', 'data_type': json.dumps({'type': 'REAL'})},
    ]
    if current_exists:
        client.query.return_value.append({
            'column_name': 'AMOUNT', 'data_type': json.dumps({'type': 'FIXED', 'precision': 38, 'scale': 18}),
        })
    target = {'sf_object': client, 'schema': 'SCHEMA', 'table': 'TABLE'}
    source_columns = ['"AMOUNT" NUMERIC(38,18)']
    with pytest.raises(NativePartialSyncCompatibilityError, match='boundary column.*historical versions.*FullSync'):
        diff_source_target_columns(target, source_columns, boundary_column='amount')
    report = utils.report_source_target_columns(
        target, source_columns, client.query.return_value, boundary_column='amount',
    )
    assert report[0]['status'] == 'incompatible'
    assert 'historical versions' in report[0]['reason']
    assert all(call.args[0].startswith('SHOW COLUMNS') for call in client.query.call_args_list)


def test_source_column_with_archive_suffix_is_not_a_historical_boundary():
    expected = _spec('NUMERIC(38,18)')
    expected = replace(expected, columns=expected.columns + (IcebergColumn('AMOUNT_20260929_123456_123456', 'FLOAT'),))
    assert plan_column_versions(expected, expected, boundary_column='amount') == {}
    utils._reject_historical_boundary(
        'amount', ['"AMOUNT"', '"AMOUNT_20260929_123456_123456"'],
        ['AMOUNT', 'AMOUNT_20260929_123456_123456'],
    )


@pytest.mark.parametrize('collision', ['existing', 'planned', 'source'])
def test_native_archive_collisions_fail_before_any_column_is_renamed(collision):
    archive = 'AMOUNT_20260929_123456_123456'
    changes = {
        'column_versions': {'"AMOUNT"': 'NUMERIC(38,18)', '"OTHER"': 'NUMERIC(38,18)'},
        'source_columns': {'"AMOUNT"': 'NUMERIC(38,18)', '"OTHER"': 'NUMERIC(38,18)'},
        'target_columns': ['AMOUNT', 'OTHER'],
    }
    if collision == 'existing':
        changes['target_columns'].append(archive)
    elif collision == 'source':
        changes['source_columns'][f'"{archive}"'] = 'FLOAT'
    with patch.object(utils, 'versioned_column_name', return_value=archive):
        with pytest.raises(NativePartialSyncCompatibilityError, match='Historical column already exists'):
            utils._plan_native_column_versions(changes)


@pytest.mark.parametrize('failure_point', ['rename', 'add'])
def test_native_retry_after_rename_adds_replacement_without_archiving_twice(failure_point, monkeypatch, caplog):
    monkeypatch.setattr(logging.getLogger('pipelinewise'), 'propagate', True)
    caplog.set_level(logging.INFO)
    client = Mock()
    archive = 'AMOUNT_20260930_120000_123456'
    target_columns = {'ID': {'type': 'FIXED', 'precision': 38, 'scale': 0}, 'AMOUNT': {'type': 'REAL'}}

    def query(sql):
        if sql.startswith('SHOW COLUMNS'):
            return [{'column_name': name, 'data_type': json.dumps(data_type)}
                    for name, data_type in target_columns.items()]
        assert f'RENAME COLUMN "AMOUNT" TO "{archive}"' in sql
        if failure_point == 'rename' and archive not in target_columns:
            raise RuntimeError('interrupted during rename')
        target_columns[archive] = target_columns.pop('AMOUNT')

    def add_columns(_schema, _table, columns):
        if columns:
            target_columns['AMOUNT'] = {'type': 'FIXED', 'precision': 38, 'scale': 18}

    client.query.side_effect = query
    client.add_columns.side_effect = RuntimeError('interrupted after rename')
    target = {'sf_object': client, 'schema': 'SCHEMA', 'table': 'TABLE', 'temp': 'STAGE'}
    args = Namespace(
        table='TABLE', drop_target_table=False,
        target={'version_legacy_float_columns': True},
    )
    source = ['"ID" NUMBER(38,0)', '"AMOUNT" NUMERIC(38,18)']
    with patch.object(utils.iceberg_routes, 'require_native_target_format'), \
            patch.object(utils, 'versioned_column_name', return_value=archive):
        with pytest.raises(RuntimeError, match='interrupted'):
            utils.load_into_snowflake(
                target, args, source, ['ID'], 'prefix', 1, 'WHERE ID > 0', decimal_columns=('AMOUNT',),
            )
        messages = [record.getMessage() for record in caplog.records if 'has been renamed to' in record.getMessage()]
        assert len(messages) == (0 if failure_point == 'rename' else 1)
        client.publish_partial_sync.assert_not_called()
        failure_point = None
        client.add_columns.side_effect = add_columns
        utils.load_into_snowflake(
            target, args, source, ['ID'], 'prefix', 1, 'WHERE ID > 0', decimal_columns=('AMOUNT',),
        )
        utils.load_into_snowflake(
            target, args, source, ['ID'], 'prefix', 1, 'WHERE ID > 0', decimal_columns=('AMOUNT',),
        )
    assert set(target_columns) == {'ID', 'AMOUNT', archive}
    assert target_columns[archive] == {'type': 'REAL'}
    assert sum('RENAME COLUMN' in call.args[0] for call in client.query.call_args_list) == (
        2 if len(messages) == 0 else 1
    )
    assert client.publish_partial_sync.call_count == 2
    messages = [record.getMessage() for record in caplog.records if 'has been renamed to' in record.getMessage()]
    assert messages == [f'Column "AMOUNT" in table "SCHEMA."TABLE"" has been renamed to "{archive}"']


@pytest.mark.parametrize('failure_point', ['none', 'rename', 'add'])
def test_iceberg_rename_logging_survives_sql_formatting_and_ddl_retries(tmp_path, monkeypatch, caplog, failure_point):
    monkeypatch.setattr(logging.getLogger('pipelinewise'), 'propagate', True)
    caplog.set_level(logging.INFO)
    expected, actual = _spec('NUMERIC(38,18)'), _spec('FLOAT')
    versions = plan_column_versions(
        expected, actual, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
    )
    archive = IcebergColumn(versions['AMOUNT']['archived_name'], 'FLOAT')
    evolved = replace(expected, columns=expected.columns + (archive,))
    key_validation = [{'HAS_NULL_KEY': 0, 'HAS_DUPLICATE_KEY': 0}]
    responses = [key_validation]
    if failure_point == 'add':
        responses.append([])
    if failure_point != 'none':
        responses.append(RuntimeError('interrupted DDL'))
    client = FakeSnowflake(responses)
    publisher = SnowflakeIcebergPublisher(client, str(tmp_path))
    publisher.inspect_table = Mock(side_effect=[v3_snapshot(actual), v3_snapshot(actual), v3_snapshot(evolved)])
    publisher._verify_published = Mock()
    attempt = make_attempt(expected, kind='partial', method=PUBLICATION_PARTIAL_MERGE, snapshot=v3_snapshot(actual),
                           context={'column_versions': versions, 'decimal_columns': ['AMOUNT'],
                                    'version_legacy_float_columns': True})
    persist_attempt(publisher, attempt)
    original_plan = publisher.publication_service.plan_partial_sync

    def reformatted_plan(*args):
        plan = original_plan(*args)
        return replace(plan, preparation_statements=tuple(
            statement.replace(' RENAME COLUMN ', '\nRENAME COLUMN ') for statement in plan.preparation_statements
        ))

    publisher.publication_service.plan_partial_sync = reformatted_plan
    if failure_point != 'none':
        with pytest.raises(RuntimeError, match='interrupted DDL'):
            publisher.publish_partial_sync(attempt, expected)
        messages = [record.getMessage() for record in caplog.records if 'has been renamed to' in record.getMessage()]
        assert len(messages) == (0 if failure_point == 'rename' else 1)
        retry_schema = actual if failure_point == 'rename' else replace(
            actual, columns=tuple(archive if column.name == 'AMOUNT' else column for column in actual.columns),
        )
        publisher.inspect_table = Mock(side_effect=[
            v3_snapshot(retry_schema), v3_snapshot(retry_schema), v3_snapshot(evolved),
        ])
        client.responses = [key_validation]
    publisher.publish_partial_sync(attempt, expected)
    messages = [record.getMessage() for record in caplog.records if 'has been renamed to' in record.getMessage()]
    assert messages == [
        f'Column "AMOUNT" in table "{expected.name.quoted}" has been renamed to "{archive.name}"',
    ]


@pytest.mark.parametrize('change', ['none', 'unexpected', 'changed_type', 'missing', 'not_nullable'])
def test_published_partial_schema_verifies_persisted_history(change):
    expected = _spec('NUMERIC(38,18)')
    old_archive = IcebergColumn('AMOUNT_20260929_120000_123456', 'FLOAT')
    new_archive = IcebergColumn('AMOUNT_20260930_120000_123456', 'NUMBER(18,2)')
    actual = replace(expected, columns=expected.columns + (old_archive, new_archive))
    history = {old_archive.name: old_archive.data_type}
    versions = {'AMOUNT': {'archived_name': new_archive.name, 'data_type': new_archive.data_type}}
    if change == 'unexpected':
        actual = replace(actual, columns=actual.columns + (IcebergColumn('AMOUNT_20260930_130000_123456', 'FLOAT'),))
    elif change == 'changed_type':
        archives = (replace(old_archive, data_type='NUMBER(18,2)'), new_archive)
        actual = replace(actual, columns=expected.columns + archives)
    elif change == 'missing':
        actual = replace(actual, columns=expected.columns + (new_archive,))
    elif change == 'not_nullable':
        actual = replace(actual, columns=expected.columns + (replace(old_archive, nullable=False), new_archive))
    attempt = Namespace(method=PUBLICATION_PARTIAL_MERGE,
                        manifest_payload=Namespace(
                            historical_columns=history, column_versions=versions,
                            decimal_columns=['AMOUNT'], version_legacy_float_columns=True,
                        ))
    service = SnowflakeIcebergPublicationService(Mock())
    assert service._published_compatibility(attempt, expected, actual)[0] == (
        'exact' if change == 'none' else 'incompatible'
    )
    if change == 'none':
        assert partial_preparation(
            expected, actual, versions, historical_columns=history, decimal_columns=('AMOUNT',),
            version_legacy_float_columns=True,
        ).statements == ()
    else:
        with pytest.raises(RecoveryManifestError):
            partial_preparation(
                expected, actual, versions, historical_columns=history, decimal_columns=('AMOUNT',),
                version_legacy_float_columns=True,
            )


def test_decimal_float_fallback_versions_once_and_can_return_to_numeric():
    numeric, floating = _spec('NUMERIC(38,18)'), _spec('FLOAT')
    for expected, actual in ((floating, numeric), (numeric, floating)):
        versions = plan_column_versions(
            expected, actual, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
        )
        statements = partial_preparation(
            expected, actual, versions, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
        ).statements
        assert len(statements) == 2
        archive = IcebergColumn(versions['AMOUNT']['archived_name'], next(
            column.data_type for column in actual.columns if column.name == 'AMOUNT'
        ))
        evolved = replace(expected, columns=expected.columns + (archive,))
        assert partial_preparation(
            expected, evolved, versions, decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
        ).statements == ()
    client = Mock()
    client.query.return_value = [{'column_name': 'AMOUNT', 'data_type': json.dumps({
        'type': 'FIXED', 'precision': 38, 'scale': 18,
    })}]
    target = {'sf_object': client, 'schema': 'SCHEMA', 'table': 'TABLE'}
    assert diff_source_target_columns(
        target, ['"AMOUNT" FLOAT'], decimal_columns=('AMOUNT',),
    )['column_versions'] == {'"AMOUNT"': 'FLOAT'}
    client.query.return_value = [{'column_name': 'AMOUNT', 'data_type': json.dumps({'type': 'REAL'})}]
    assert diff_source_target_columns(
        target, ['"AMOUNT" FLOAT'], decimal_columns=('AMOUNT',),
    )['column_versions'] == {}


def test_partial_preparation_persists_existing_archives_before_ddl(tmp_path):
    expected, actual = _spec('NUMERIC(38,18)'), _spec('FLOAT')
    archive = IcebergColumn('AMOUNT_20260929_120000_123456', 'NUMBER(18,2)')
    actual = replace(actual, columns=actual.columns + (archive,))
    client = FakeSnowflake()
    publisher = SnowflakeIcebergPublisher(client, str(tmp_path))
    publisher.inspect_table = Mock(return_value=v3_snapshot(actual))
    attempt = publisher.prepare_partial_sync(
        expected, {}, PartialSyncBoundary('ID', 1, 10), recovery_identity=RECOVERY_IDENTITY,
        decimal_columns=('AMOUNT',), version_legacy_float_columns=True,
    )
    recovered = publisher.load_attempt(expected, expected_kind='partial', recovery_identity=RECOVERY_IDENTITY)
    assert recovered.manifest_payload.historical_columns == {archive.name: archive.data_type}
    assert recovered.manifest_payload.column_versions == attempt.manifest_payload.column_versions
    assert recovered.manifest_payload.decimal_columns == ['AMOUNT']
    assert recovered.manifest_payload.version_legacy_float_columns is True
    assert client.queries == []
    assert len(publisher.plan_partial_sync(recovered, expected).preparation_statements) == 2


def test_partial_manifests_without_archive_evidence_keep_exact_schema_compatibility():
    expected = _spec('NUMERIC(38,18)')
    attempt = Namespace(method=PUBLICATION_PARTIAL_MERGE,
                        manifest_payload=Namespace(
                            historical_columns=None, column_versions=None, decimal_columns=None,
                            version_legacy_float_columns=None,
                        ))
    service = SnowflakeIcebergPublicationService(Mock())
    assert service._published_compatibility(attempt, expected, expected) == ('exact', ())
