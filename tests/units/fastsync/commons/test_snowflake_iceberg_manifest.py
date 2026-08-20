"""Typed payload, version strategy, and lifecycle contract tests."""

# pylint: disable=invalid-name

from dataclasses import replace

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    ConversionManifestPayload,
    FullSyncManifestPayload,
    MANAGED_ICEBERG_V3_SPEC,
    PHASE_FINALIZED,
    PHASE_PREPARED,
    PHASE_PUBLISHED,
    PHASE_STAGED,
    PHASE_STAGING_CREATED,
    PHASE_SUBMITTED,
    PHASE_UPLOADED,
    PartialSyncManifestPayload,
    RecoveryManifestError,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    IcebergPublicationAttempt,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    FINALIZATION_GRANTS,
    FINALIZATION_METADATA,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    make_attempt,
)


def test_current_manifest_round_trips_typed_payload(spec):
    """New manifests serialize typed state and its compatibility projection."""
    attempt = make_attempt(
        spec,
        kind='partial',
        context={
            'where_clause_sql': ' WHERE "ID" >= 1',
            'end_is_unbounded': True,
            'delete_mode': 'hard',
            'extension': 'preserved',
        },
    )

    serialized = attempt.as_dict()
    recovered = IcebergPublicationAttempt.from_dict(serialized)

    assert serialized['payload'] == {
        'payload_version': 1,
        'payload_type': 'partial',
        'values': serialized['context'],
    }
    assert isinstance(recovered.manifest_payload, PartialSyncManifestPayload)
    assert recovered.manifest_payload.where_clause_sql == ' WHERE "ID" >= 1'
    assert recovered.manifest_payload.extensions == {'extension': 'preserved'}


def test_manifest_written_before_typed_payload_still_loads(spec):
    """A current manifest without the additive payload envelope stays recoverable."""
    serialized = make_attempt(spec, phase=PHASE_PREPARED).as_dict()
    serialized.pop('payload')

    recovered = IcebergPublicationAttempt.from_dict(serialized)

    assert isinstance(recovered.manifest_payload, FullSyncManifestPayload)
    assert recovered.context == serialized['context']


def test_manifest_requires_serialized_source_bookmark(spec):
    """A missing source bookmark cannot be normalized into state to advance."""
    serialized = make_attempt(spec, phase=PHASE_PREPARED).as_dict()
    serialized.pop('source_bookmark')

    with pytest.raises(
        RecoveryManifestError,
        match='source bookmark is invalid',
    ):
        IcebergPublicationAttempt.from_dict(serialized)


@pytest.mark.parametrize(
    'invalid_bookmark',
    (None, False, 0, '', [], [['lsn', '1/2']]),
)
def test_manifest_requires_source_bookmark_object(spec, invalid_bookmark):
    """Only an explicitly serialized dictionary is valid bookmark state."""
    serialized = make_attempt(spec, phase=PHASE_PREPARED).as_dict()
    serialized['source_bookmark'] = invalid_bookmark

    with pytest.raises(
        RecoveryManifestError,
        match='source bookmark is invalid',
    ):
        IcebergPublicationAttempt.from_dict(serialized)


def test_manifest_allows_empty_source_bookmark(spec):
    """An explicitly empty bookmark remains a valid state boundary."""
    serialized = make_attempt(spec, phase=PHASE_PREPARED).as_dict()
    serialized['source_bookmark'] = {}

    recovered = IcebergPublicationAttempt.from_dict(serialized)

    assert recovered.source_bookmark == {}


def test_manifest_requires_serialized_s3_keys(spec):
    """A missing upload plan cannot be normalized into an empty cleanup set."""
    serialized = make_attempt(spec, phase=PHASE_PREPARED).as_dict()
    serialized.pop('s3_keys')

    with pytest.raises(RecoveryManifestError, match='S3 keys are invalid'):
        IcebergPublicationAttempt.from_dict(serialized)


@pytest.mark.parametrize(
    'invalid_s3_keys',
    (
        None,
        '',
        'load/part.csv.gz',
        {'load/part.csv.gz': True},
        ('load/part.csv.gz',),
        [1],
        [''],
        ['load/part.csv.gz', 'load/part.csv.gz'],
    ),
)
def test_manifest_rejects_unsafe_s3_keys(spec, invalid_s3_keys):
    """Recovery accepts only the exact unique string list planned for cleanup."""
    serialized = make_attempt(spec, phase=PHASE_PREPARED).as_dict()
    serialized['s3_keys'] = invalid_s3_keys

    with pytest.raises(RecoveryManifestError, match='S3 keys are invalid'):
        IcebergPublicationAttempt.from_dict(serialized)


def test_manifest_s3_keys_are_defensively_copied(spec):
    """Validated staging keys do not alias a caller-owned serialized list."""
    serialized = make_attempt(spec, phase=PHASE_PREPARED).as_dict()
    serialized['s3_keys'] = ['load/part.csv.gz']

    recovered = IcebergPublicationAttempt.from_dict(serialized)
    serialized['s3_keys'].append('load/other.csv.gz')

    assert recovered.s3_keys == ['load/part.csv.gz']


def test_manifest_rejects_unsafe_s3_keys_before_serialization(spec):
    """In-process mutation cannot persist an unsafe cleanup collection."""
    attempt = make_attempt(spec, phase=PHASE_PREPARED)
    attempt.s3_keys = 'load/part.csv.gz'

    with pytest.raises(RecoveryManifestError, match='S3 keys are invalid'):
        attempt.as_dict()


@pytest.mark.parametrize(
    'phase',
    (
        PHASE_PREPARED,
        PHASE_UPLOADED,
        PHASE_STAGING_CREATED,
        PHASE_STAGED,
        PHASE_SUBMITTED,
    ),
)
def test_manifest_rejects_finalization_before_publication(spec, phase):
    """Pre-publication state cannot claim that later cleanup already happened."""
    serialized = make_attempt(spec, phase=phase).as_dict()
    serialized['finalization'] = {FINALIZATION_GRANTS: True}

    with pytest.raises(
        RecoveryManifestError,
        match=f'finalization actions are inconsistent with phase {phase}',
    ):
        IcebergPublicationAttempt.from_dict(serialized)


def test_manifest_rejects_finalization_before_serialization(spec):
    """In-process mutation cannot persist impossible finalization progress."""
    attempt = make_attempt(spec, phase=PHASE_STAGED)
    attempt.finalization = {FINALIZATION_GRANTS: True}

    with pytest.raises(
        RecoveryManifestError,
        match='finalization actions are inconsistent with phase staged',
    ):
        attempt.as_dict()


def test_published_manifest_accepts_applicable_partial_finalization(spec):
    """Published recovery can resume after any durable subset of required work."""
    serialized = make_attempt(spec, phase=PHASE_PUBLISHED).as_dict()
    serialized['finalization'] = {FINALIZATION_GRANTS: True}

    recovered = IcebergPublicationAttempt.from_dict(serialized)

    assert recovered.finalization == {FINALIZATION_GRANTS: True}


def test_published_manifest_rejects_inapplicable_finalization(spec):
    """Metadata progress is impossible when publication did not replace a table."""
    serialized = make_attempt(spec, phase=PHASE_PUBLISHED).as_dict()
    serialized['finalization'] = {FINALIZATION_METADATA: True}

    with pytest.raises(
        RecoveryManifestError,
        match='finalization actions are inconsistent with phase published',
    ):
        IcebergPublicationAttempt.from_dict(serialized)


def test_published_replacement_accepts_metadata_finalization(spec):
    """Replacement publication includes metadata in its applicable action set."""
    serialized = make_attempt(
        spec,
        phase=PHASE_PUBLISHED,
        context={'replacement_metadata': {}},
    ).as_dict()
    serialized['finalization'] = {FINALIZATION_METADATA: True}

    recovered = IcebergPublicationAttempt.from_dict(serialized)

    assert recovered.finalization == {FINALIZATION_METADATA: True}


@pytest.mark.parametrize('phase', (PHASE_PUBLISHED, PHASE_FINALIZED))
def test_manual_conversion_rejects_publication_finalization_actions(spec, phase):
    """Manual conversion uses its own terminal validator, not route cleanup markers."""
    serialized = make_attempt(
        spec,
        phase=phase,
        kind='manual_conversion',
        method='manual_conversion',
        context={
            'eventual': 'iceberg',
            'backup_table': 'ORDERS_NATIVE',
            'source_schema_fingerprint': 'a' * 64,
        },
    ).as_dict()
    serialized['finalization'] = {FINALIZATION_GRANTS: True}

    with pytest.raises(RecoveryManifestError, match='finalization actions'):
        IcebergPublicationAttempt.from_dict(serialized)


@pytest.mark.parametrize('invalid_context', (None, False, 0, '', []))
def test_legacy_manifest_rejects_explicit_non_object_context(
    spec,
    invalid_context,
):
    """Falsy malformed legacy contexts are not normalized into empty objects."""
    serialized = make_attempt(spec, phase=PHASE_PREPARED).as_dict()
    serialized.pop('payload')
    serialized['context'] = invalid_context

    with pytest.raises(
        RecoveryManifestError,
        match='manifest context is invalid',
    ):
        IcebergPublicationAttempt.from_dict(serialized)


def test_payload_must_match_legacy_projection(spec):
    """Contradictory typed and compatibility representations fail closed."""
    serialized = make_attempt(spec, phase=PHASE_PREPARED).as_dict()
    serialized['payload']['values']['staging_config'] = {'stage': 'other'}

    with pytest.raises(
        RecoveryManifestError,
        match='payload does not match',
    ):
        IcebergPublicationAttempt.from_dict(serialized)


@pytest.mark.parametrize(
    ('kind', 'field_name', 'invalid_value'),
    (
        ('full', 'staging_config', 'stage'),
        ('full', 'replacement_metadata', []),
        ('full', 'schema_evolution_applied', 1),
        ('full', 'publication_query_hash', 'not-a-hash'),
        ('full', 'publication_query_type', ''),
        ('full', 'publication_submitted_at', float('inf')),
        ('partial', 'where_clause_sql', ''),
        ('partial', 'delete_mode', 'soft'),
        ('partial', 'end_is_unbounded', 1),
        ('partial', 'drop_target', 'false'),
        ('manual_conversion', 'eventual', 'unknown'),
        ('manual_conversion', 'backup_table', ''),
        ('manual_conversion', 'source_schema_fingerprint', 'short'),
        ('manual_conversion', 'rollback_required', 1),
    ),
)
def test_known_payload_fields_fail_closed_on_load(
    spec,
    kind,
    field_name,
    invalid_value,
):
    """Known payload fields reject malformed durable state before recovery."""
    context = None
    method = 'missing_ctas'
    if kind == 'manual_conversion':
        method = 'manual_conversion'
        context = {
            'eventual': 'iceberg',
            'backup_table': 'ORDERS_NATIVE',
            'source_schema_fingerprint': 'a' * 64,
        }
    attempt = make_attempt(
        spec,
        phase=PHASE_PREPARED,
        kind=kind,
        method=method,
        context=context,
    )
    serialized = attempt.as_dict()
    serialized['context'][field_name] = invalid_value
    serialized['payload']['values'][field_name] = invalid_value

    with pytest.raises(RecoveryManifestError, match='payload is invalid'):
        IcebergPublicationAttempt.from_dict(serialized)


def test_payload_is_authoritative_after_construction(spec):
    """Legacy context is a projection rather than a second mutable source of truth."""
    attempt = make_attempt(spec, phase=PHASE_PREPARED)
    attempt.context['staging_config'] = {'stage': 'legacy-mutation'}

    serialized = attempt.as_dict()

    assert attempt.manifest_payload.staging_config is None
    assert 'staging_config' not in serialized['context']
    attempt.update_manifest_payload({'staging_config': {'stage': 'typed'}})
    assert attempt.manifest_payload.staging_config == {'stage': 'typed'}
    assert attempt.context['staging_config'] == {'stage': 'typed'}


def test_nested_context_projection_cannot_mutate_typed_payload(spec):
    """Nested compatibility values do not alias authoritative typed values."""
    attempt = make_attempt(
        spec,
        phase=PHASE_PREPARED,
        context={
            'staging_config': {
                'stage': 'original',
                'options': {'compression': 'gzip'},
            },
            'extension': {'nested': 'original'},
        },
    )

    attempt.context['staging_config']['options']['compression'] = 'legacy-mutation'
    exposed_payload = attempt.manifest_payload
    exposed_payload.staging_config['stage'] = 'direct-mutation'
    exposed_payload.extensions['extension']['nested'] = 'direct-mutation'
    projected = exposed_payload.as_context()
    projected['staging_config']['stage'] = 'projection-mutation'
    serialized = attempt.as_dict()
    serialized['context']['staging_config']['stage'] = 'serialized-mutation'

    assert attempt.manifest_payload.staging_config == {
        'stage': 'original',
        'options': {'compression': 'gzip'},
    }
    assert serialized['payload']['values']['staging_config']['stage'] == 'original'
    assert serialized['payload']['values']['extension'] == {'nested': 'original'}


@pytest.mark.parametrize(
    ('current_phase', 'next_phase'),
    (
        (PHASE_PREPARED, PHASE_UPLOADED),
        (PHASE_UPLOADED, PHASE_STAGING_CREATED),
        (PHASE_STAGING_CREATED, PHASE_STAGED),
        (PHASE_STAGED, PHASE_SUBMITTED),
        (PHASE_SUBMITTED, PHASE_PUBLISHED),
        (PHASE_PUBLISHED, PHASE_FINALIZED),
    ),
)
def test_full_sync_legal_transition_graph(spec, current_phase, next_phase):
    """Every forward FullSync lifecycle edge is explicit."""
    attempt = make_attempt(spec, phase=current_phase)

    attempt.transition_to(next_phase)

    assert attempt.phase == next_phase


def test_illegal_transition_does_not_mutate_attempt(spec):
    """A skipped production lifecycle edge fails before phase mutation."""
    attempt = make_attempt(spec, phase=PHASE_PREPARED)

    with pytest.raises(RecoveryManifestError, match='prepared -> published'):
        attempt.transition_to(PHASE_PUBLISHED)

    assert attempt.phase == PHASE_PREPARED


def test_conversion_payload_and_rollback_transition_are_explicit(spec):
    """Conversion owns its fields and permits the published rollback edge."""
    attempt = make_attempt(
        spec,
        phase=PHASE_PUBLISHED,
        kind='manual_conversion',
        method='manual_conversion',
        context={
            'eventual': 'iceberg',
            'backup_table': 'ORDERS_NATIVE',
            'source_schema_fingerprint': 'a' * 64,
        },
    )

    assert isinstance(attempt.manifest_payload, ConversionManifestPayload)
    attempt.transition_to(PHASE_SUBMITTED)
    assert attempt.phase == PHASE_SUBMITTED


def test_version_strategy_is_immutable_and_complete():
    """A future strategy cannot omit CoW or silently mutate v3 semantics."""
    with pytest.raises(TypeError):
        MANAGED_ICEBERG_V3_SPEC.table_options['TARGET_FILE_SIZE'] = '64MB'
    with pytest.raises(ValueError, match='incomplete'):
        replace(
            MANAGED_ICEBERG_V3_SPEC,
            table_options={'TARGET_FILE_SIZE': '16MB'},
        )
