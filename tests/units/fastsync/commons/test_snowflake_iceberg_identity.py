"""Recovery identity tests for Snowflake Iceberg FastSync routes."""

import copy
from argparse import Namespace
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_routes
from pipelinewise.fastsync.commons.partial_sync_boundary import PartialSyncBoundary
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    IcebergTableSpec,
    RecoveryManifestError,
    SnowflakeIcebergPublisher,
)


STAGING_CONFIG = {
    's3_bucket': 'staging-bucket',
    's3_key_prefix': 'loads',
    'stage': 'PIPELINEWISE_STAGE',
    'file_format': 'PIPELINEWISE_FORMAT',
}


def _args():
    return Namespace(
        tap={
            'host': 'primary.internal',
            'port': 3306,
            'dbname': 'source_db',
            'user': 'source_user',
            'password': 'source-secret',
            'replica_host': 'replica.internal',
            'replica_port': 4406,
            'replica_user': 'replica_user',
            'replica_password': 'replica-secret',
        },
        target={
            'tap_id': 'tap_orders',
            'account': 'test-account',
            'dbname': 'TARGET_DB',
            'default_target_schema': 'TARGET_SCHEMA',
            'user': 'target_user',
            'role': 'PIPELINEWISE_ROLE',
            'private_key': 'private-key-secret',
            'target_table_format': 'iceberg',
            'iceberg_version': 3,
        },
        transform={
            'transformations': [
                {'field_id': 'email', 'type': 'HASH'},
            ],
        },
    )


def _identity(args, source_engine='mysql'):
    return snowflake_iceberg_routes.fastsync_recovery_identity(
        args,
        'source_db.orders',
        source_route='postgres_to_snowflake' if source_engine == 'postgres' else 'mysql_to_snowflake',
        source_engine=source_engine,
        staging_config=STAGING_CONFIG,
        iceberg_version=args.target['iceberg_version'],
    )


def _spec():
    return IcebergTableSpec.from_fastsync(
        'TARGET_DB',
        'TARGET_SCHEMA',
        'ORDERS',
        ['"ID" NUMBER'],
        ['"ID"'],
    )


@pytest.mark.parametrize(
    ('section', 'field', 'value'),
    (
        ('tap', 'host', 'other-primary.internal'),
        ('tap', 'replica_host', 'other-replica.internal'),
        ('target', 'account', 'other-account'),
        ('target', 'role', 'OTHER_ROLE'),
        (
            'transform',
            'transformations',
            [{'field_id': 'email', 'type': 'SET-NULL'}],
        ),
    ),
)
def test_identity_drift_fails_closed(
    tmp_path,
    section,
    field,
    value,
):
    """A retry cannot resume a manifest created by another route identity."""
    args = _args()
    snowflake = Mock()
    snowflake.connection_config = {'dbname': 'TARGET_DB'}
    snowflake.query.return_value = []
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
    spec = _spec()
    recovery_identity = _identity(args)
    attempt = publisher.prepare_full_sync(
        spec,
        {'position': 42},
        recovery_identity=recovery_identity,
        staging_config=STAGING_CONFIG,
    )
    store = publisher.recovery_store(spec.name, recovery_identity)
    persisted = Path(store.path).read_bytes()
    snowflake.reset_mock()

    changed = copy.deepcopy(args)
    getattr(changed, section)[field] = value
    with pytest.raises(RecoveryManifestError, match='different source, target'):
        publisher.load_attempt(
            spec,
            expected_kind='full',
            recovery_identity=_identity(changed),
            staging_config=STAGING_CONFIG,
        )

    snowflake.query.assert_not_called()
    assert Path(store.path).read_bytes() == persisted
    assert attempt.phase == 'prepared'
    assert attempt.source_bookmark == {'position': 42}


def test_identity_is_canonical_and_safe(tmp_path):
    """Configuration ordering is ignored and no raw credentials/config are stored."""
    args = _args()
    reordered = copy.deepcopy(args)
    reordered.transform = {
        'transformations': [
            {'type': 'HASH', 'field_id': 'email'},
        ],
    }

    identity = _identity(args)
    assert _identity(reordered) == identity
    assert identity['transformation_semantics_version'] == 1

    snowflake = Mock()
    snowflake.connection_config = {'dbname': 'TARGET_DB'}
    snowflake.query.return_value = []
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
    spec = _spec()
    publisher.prepare_full_sync(
        spec,
        {'position': 42},
        recovery_identity=identity,
        staging_config=STAGING_CONFIG,
    )
    manifest = Path(publisher.recovery_store(spec.name, identity).path).read_text(
        encoding='utf-8'
    )

    for excluded in (
        'source-secret',
        'replica-secret',
        'private-key-secret',
        'primary.internal',
        'test-account',
        'field_id',
    ):
        assert excluded not in manifest
    assert 'staging-bucket' in manifest


def _legacy_identity(args, source_engine='mysql'):
    """Build the pre-source-transformation identity without changing config."""
    build_identity = snowflake_iceberg_routes.build_recovery_identity

    def build_legacy(scope, identity, **kwargs):
        legacy = copy.deepcopy(identity)
        legacy['source'].pop('transformation_execution', None)
        return build_identity(scope, legacy, **kwargs)

    with patch.object(snowflake_iceberg_routes, 'build_recovery_identity', side_effect=build_legacy):
        return _identity(args, source_engine)


@pytest.mark.parametrize('source_engine', ('mysql', 'mariadb', 'postgres'))
@pytest.mark.parametrize('kind', ('full', 'partial'))
@pytest.mark.parametrize('phase', ('prepared', 'staged'))
def test_legacy_transformed_attempt_is_rejected_before_recovery(tmp_path, source_engine, kind, phase):
    """Retained raw files/staging cannot cross the source-projection contract."""
    args = _args()
    args.transform['transformations'][0]['tap_stream_name'] = 'SOURCE_DB-ORDERS'
    legacy_identity = _legacy_identity(args, source_engine)
    current_identity = _identity(args, source_engine)
    assert current_identity['fingerprint'] != legacy_identity['fingerprint']
    assert current_identity['stream_fingerprint'] == legacy_identity['stream_fingerprint']
    assert current_identity['transformation_fingerprint'] == legacy_identity['transformation_fingerprint']

    snowflake = Mock()
    snowflake.connection_config = {'dbname': 'TARGET_DB'}
    snowflake.query.return_value = []
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
    spec = _spec()
    prepare = publisher.prepare_full_sync if kind == 'full' else publisher.prepare_partial_sync
    boundary = () if kind == 'full' else (PartialSyncBoundary('ID', 1),)
    attempt = prepare(
        spec,
        {'position': 42},
        *boundary,
        recovery_identity=legacy_identity,
        staging_config=STAGING_CONFIG,
    )
    if phase == 'staged':
        publisher.record_planned_uploads(attempt, ['raw-stage.csv.gz'])
        publisher.record_uploaded(attempt, ['raw-stage.csv.gz'])
        publisher.record_staging_created(attempt)
        publisher.record_staged(attempt, row_count=1, row_fingerprint='fingerprint')
    store = publisher.recovery_store(spec.name, legacy_identity)
    persisted = Path(store.path).read_bytes()
    snowflake.reset_mock()

    with pytest.raises(RecoveryManifestError, match='transformation contract'):
        publisher.load_attempt(
            spec,
            expected_kind=kind,
            recovery_identity=current_identity,
            staging_config=STAGING_CONFIG,
        )

    snowflake.query.assert_not_called()
    assert Path(store.path).read_bytes() == persisted
    assert attempt.phase == phase
    assert attempt.source_bookmark == {'position': 42}


@pytest.mark.parametrize('transformations', ([], [{'tap_stream_name': 'source_db-other', 'type': 'HASH'}]))
def test_source_projection_contract_keeps_unaffected_recovery_compatible(transformations):
    """An unrelated stream's masking rules do not invalidate safe staging."""
    args = _args()
    args.transform = {'transformations': transformations}
    assert _identity(args) == _legacy_identity(args)
