"""Tests for source-engine evidence in managed-Iceberg recovery."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pipelinewise.fastsync.commons import rdbms_to_snowflake
from pipelinewise.fastsync.commons import snowflake_iceberg_routes as routes
from pipelinewise.fastsync.commons.partial_sync_boundary import PartialSyncBoundary
from pipelinewise.fastsync.commons.rdbms_source import RdbmsSnowflakeSource
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    PHASE_PREPARED,
    RecoveryManifestError,
    SnowflakeIcebergPublisher,
)
from pipelinewise.fastsync.partialsync import rdbms_to_snowflake as partial_runner
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    FakeSnowflake,
    RECOVERY_IDENTITY,
    make_attempt,
    missing_snapshot,
)


def _prepare_attempt(publisher, spec, kind, resolved_source_engine):
    if kind == 'full':
        return publisher.prepare_full_sync(
            spec,
            {'log_file': 'mysql-bin.000001', 'log_pos': 4},
            recovery_identity=RECOVERY_IDENTITY,
            resolved_source_engine=resolved_source_engine,
        )
    return publisher.prepare_partial_sync(
        spec,
        {'log_file': 'mysql-bin.000001', 'log_pos': 4},
        PartialSyncBoundary('ID', 1, 10),
        recovery_identity=RECOVERY_IDENTITY,
        resolved_source_engine=resolved_source_engine,
    )


@pytest.mark.parametrize('kind', ('full', 'partial'))
@pytest.mark.parametrize('resolved_source_engine', ('mysql', 'mariadb'))
def test_resolved_source_engine_round_trips_in_recovery_manifest(
        tmp_path, spec, kind, resolved_source_engine):
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    publisher.inspect_table = MagicMock(return_value=missing_snapshot())

    original = _prepare_attempt(
        publisher, spec, kind, resolved_source_engine
    )
    recovered = publisher.load_attempt(
        spec,
        expected_kind=kind,
        recovery_identity=RECOVERY_IDENTITY,
    )

    assert original.manifest_payload.resolved_source_engine == resolved_source_engine
    assert recovered.manifest_payload.resolved_source_engine == resolved_source_engine
    assert recovered.is_recovery is True


@pytest.mark.parametrize('kind', ('full', 'partial'))
@pytest.mark.parametrize('invalid_engine', ('postgres', 'MYSQL', True))
def test_prepare_rejects_invalid_resolved_source_engine(
        tmp_path, spec, kind, invalid_engine):
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    publisher.inspect_table = MagicMock(return_value=missing_snapshot())

    with pytest.raises(RecoveryManifestError, match='payload is invalid'):
        _prepare_attempt(publisher, spec, kind, invalid_engine)

    publisher.inspect_table.assert_not_called()


@pytest.mark.parametrize('kind', ('full', 'partial'))
def test_load_rejects_invalid_persisted_source_engine(tmp_path, spec, kind):
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    publisher.inspect_table = MagicMock(return_value=missing_snapshot())
    _prepare_attempt(publisher, spec, kind, 'mysql')
    store = publisher.recovery_store(spec.name, RECOVERY_IDENTITY)
    manifest_path = Path(store.path)
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    manifest['context']['resolved_source_engine'] = 'postgres'
    manifest['payload']['values']['resolved_source_engine'] = 'postgres'
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    publisher.inspect_table.reset_mock()

    with pytest.raises(RecoveryManifestError, match='payload is invalid'):
        publisher.load_attempt(
            spec,
            expected_kind=kind,
            recovery_identity=RECOVERY_IDENTITY,
        )

    publisher.inspect_table.assert_not_called()


@pytest.mark.parametrize('runner_kind', ('full', 'partial'))
@pytest.mark.parametrize(
    ('saved_engine', 'expected_message'),
    (
        (
            None,
            'Cannot re-export this Iceberg recovery attempt: its manifest has no saved '
            'source engine and may predate engine binding. Finish this attempt with the '
            'PipelineWise version and configuration that created it. Do not edit state or '
            'delete recovery files to bypass this check.',
        ),
        (
            'mysql',
            'Cannot re-export an Iceberg recovery attempt with a different source engine: '
            "saved='mysql', current='mariadb'",
        ),
    ),
)
def test_reexport_rejects_missing_or_changed_engine_before_source_export(
        spec, runner_kind, saved_engine, expected_message):
    context = (
        {'resolved_source_engine': saved_engine}
        if saved_engine is not None
        else {}
    )
    attempt = make_attempt(
        spec,
        phase=PHASE_PREPARED,
        kind=runner_kind,
        context=context,
    )
    source = MagicMock(source_engine='mariadb')
    source_factory = MagicMock(return_value=source)
    adapter = RdbmsSnowflakeSource.mysql(source_factory, MagicMock())

    with pytest.raises(RecoveryManifestError) as exc_info:
        if runner_kind == 'full':
            run = SimpleNamespace(
                source=source,
                source_adapter=adapter,
                iceberg_requested=True,
                attempt=attempt,
                table='source.ORDERS',
            )
            rdbms_to_snowflake._export_full_source(run)
        else:
            run = SimpleNamespace(
                args=SimpleNamespace(tap={}, transform=None),
                source_adapter=adapter,
                iceberg_requested=True,
                attempt=attempt,
                table_name='source.ORDERS',
            )
            partial_runner._export_partial_source(run)

    assert str(exc_info.value) == expected_message
    source.map_column_types_to_target.assert_not_called()
    source.copy_table.assert_not_called()
    source.export_source_table_data.assert_not_called()


def test_matching_mariadb_engine_allows_recovery_source_planning(spec):
    attempt = make_attempt(
        spec,
        phase=PHASE_PREPARED,
        context={'resolved_source_engine': 'mariadb'},
    )
    source = MagicMock(source_engine='mariadb')
    source.map_column_types_to_target.return_value = {
        'columns': [
            '"ID" NUMBER',
            '"PAYLOAD" VARIANT',
            '"UPDATED AT" TIMESTAMP_NTZ',
        ],
        'primary_key': ['"ID"'],
    }
    adapter = RdbmsSnowflakeSource.mysql(MagicMock(), MagicMock())
    publisher = MagicMock()
    run = SimpleNamespace(
        args=SimpleNamespace(
            target={'dbname': 'TEST_DB', 'iceberg_version': 3}
        ),
        source=source,
        source_adapter=adapter,
        attempt=attempt,
        spec=spec,
        table='source.ORDERS',
        target_schema='TEST_SCHEMA',
        publisher=publisher,
    )

    rdbms_to_snowflake._plan_full_iceberg_export(run)

    assert run.spec == spec
    assert run.bookmark == attempt.source_bookmark
    publisher.plan_full_sync.assert_called_once_with(attempt, spec)


def test_postgres_reexport_does_not_require_mysql_engine_evidence(spec):
    attempt = make_attempt(spec)
    adapter = RdbmsSnowflakeSource.postgres(MagicMock(), MagicMock())

    assert adapter.resolved_source_engine(MagicMock()) is None
    routes.validate_recovery_source_engine(
        attempt,
        adapter.resolved_source_engine(MagicMock()),
    )
