"""Recovery identity tests for Snowflake Iceberg FastSync routes."""

import copy
from argparse import Namespace
from pathlib import Path
from unittest.mock import Mock

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_routes
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


def _identity(args):
    return snowflake_iceberg_routes.fastsync_recovery_identity(
        args,
        'source_db.orders',
        source_route='mysql_to_snowflake',
        source_engine='mysql',
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
