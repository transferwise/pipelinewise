"""Recovery errors, finalization state, and safe diagnostic formatting."""

from typing import Any, Dict, Iterable, Optional, Tuple


PHASE_PREPARED = 'prepared'
PHASE_UPLOADED = 'uploaded'
PHASE_STAGING_CREATED = 'staging_created'
PHASE_STAGED = 'staged'
PHASE_SUBMITTED = 'submitted'
PHASE_PUBLISHED = 'published'
PHASE_FINALIZED = 'finalized'

FINALIZATION_GRANTS = 'grants'
FINALIZATION_METADATA = 'metadata'
FINALIZATION_S3_CLEANUP = 's3_cleanup'
FINALIZATION_STAGING_CLEANUP = 'staging_cleanup'
FINALIZATION_ACTIONS = frozenset({
    FINALIZATION_GRANTS,
    FINALIZATION_METADATA,
    FINALIZATION_S3_CLEANUP,
    FINALIZATION_STAGING_CLEANUP,
})


class IcebergPublicationError(RuntimeError):
    """Base error for guarded Iceberg publication."""


class TableFormatDiscoveryError(IcebergPublicationError):
    """Snowflake returned incomplete or contradictory table metadata."""


class TableCompatibilityError(IcebergPublicationError):
    """A physical target cannot be safely used for the requested publication."""


class StagingPrimaryKeyError(IcebergPublicationError):
    """Transformed PartialSync staging violates primary-key integrity."""


class RecoveryManifestError(IcebergPublicationError):
    """A durable publication attempt cannot be safely reconciled."""


class AmbiguousPublicationError(RecoveryManifestError):
    """Snowflake history does not prove whether a submitted publication committed."""


class RetryableQueryHistoryRecoveryError(AmbiguousPublicationError):
    """A query-history failure that requires an unchanged operator retry."""

    retryable = True
    recovery_guidance = (
        'Publication status remains ambiguous. The recovery manifest and staging '
        'table were preserved; retry the same FastSync command unchanged.'
    )

    def __init__(self, message):
        super().__init__(f'{message}. {self.recovery_guidance}')


def validate_finalization_action(action: Any) -> None:
    """Reject finalization actions outside the durable recovery contract."""
    if not isinstance(action, str) or action not in FINALIZATION_ACTIONS:
        raise RecoveryManifestError(
            f'Unsupported Iceberg finalization action: {action}'
        )


def validate_finalization_state(value: Any) -> Dict[str, bool]:
    """Require exact completed-action booleans in durable recovery state."""
    if not isinstance(value, dict) or any(
        action not in FINALIZATION_ACTIONS or completed is not True
        for action, completed in value.items()
    ):
        raise RecoveryManifestError(
            'Iceberg recovery manifest finalization state is invalid'
        )
    return dict(value)


def required_finalization_actions(attempt) -> frozenset:
    """Return the exact production actions required before state handoff."""
    if attempt.kind not in ('full', 'partial'):
        return frozenset()
    actions = {
        FINALIZATION_GRANTS,
        FINALIZATION_S3_CLEANUP,
        FINALIZATION_STAGING_CLEANUP,
    }
    if attempt.manifest_payload.replacement_metadata is not None:
        actions.add(FINALIZATION_METADATA)
    return frozenset(actions)


def validate_required_finalization_actions(
    attempt,
    finalization: Optional[Dict[str, bool]] = None,
) -> None:
    """Require the exact completed action set before terminal handoff."""
    completed = validate_finalization_state(
        attempt.finalization if finalization is None else finalization
    )
    required = required_finalization_actions(attempt)
    if set(completed) != set(required):
        missing = ', '.join(sorted(required.difference(completed))) or 'none'
        unexpected = ', '.join(sorted(set(completed).difference(required))) or 'none'
        raise RecoveryManifestError(
            'Iceberg finalization actions are incomplete or inconsistent: '
            f'missing={missing}; unexpected={unexpected}'
        )


def validate_phase_finalization_actions(
    attempt,
    finalization: Optional[Dict[str, bool]] = None,
) -> Dict[str, bool]:
    """Require finalization progress that is possible in the current phase."""
    completed = validate_finalization_state(
        attempt.finalization if finalization is None else finalization
    )
    required = required_finalization_actions(attempt)
    completed_actions = set(completed)

    if attempt.phase == PHASE_FINALIZED:
        validate_required_finalization_actions(attempt, completed)
    elif attempt.phase == PHASE_PUBLISHED:
        unexpected = completed_actions.difference(required)
        if unexpected:
            raise RecoveryManifestError(
                'Iceberg finalization actions are inconsistent with phase '
                f'{attempt.phase}: unexpected={", ".join(sorted(unexpected))}'
            )
    elif completed_actions:
        raise RecoveryManifestError(
            'Iceberg finalization actions are inconsistent with phase '
            f'{attempt.phase}: unexpected={", ".join(sorted(completed_actions))}'
        )
    return completed


def content_evidence_mismatch(
    prefix: str,
    expected: Tuple[Any, Any],
    actual: Tuple[Any, Any],
) -> str:
    """Describe a mismatch without exposing row or column values."""
    return (
        f'{prefix}: expected_row_count={expected[0]}, '
        f'actual_row_count={actual[0]}, '
        f'expected_row_fingerprint={expected[1]}, '
        f'actual_row_fingerprint={actual[1]}'
    )


class IcebergFinalizationService:
    """Stateless finalization behavior composed into an Iceberg publisher."""

    def __init__(self, publisher):
        self.publisher = publisher

    def _transition(self, attempt, phase) -> None:
        self.publisher._transition(attempt, phase)  # pylint: disable=protected-access

    def _save_active_attempt(self, attempt) -> None:
        self.publisher._save_active_attempt(  # pylint: disable=protected-access
            attempt
        )

    def _complete_attempt_cleanup(self, attempt) -> None:
        self.publisher._complete_attempt_cleanup(  # pylint: disable=protected-access
            attempt
        )

    def mark_finalized(
        self,
        attempt,
        completed_actions: Optional[Iterable[str]] = None,
    ) -> None:
        """Persist completion of all caller-owned finalization actions."""
        if attempt.phase != PHASE_PUBLISHED:
            raise RecoveryManifestError(
                'Iceberg finalization requires a published attempt'
            )
        actions = tuple(completed_actions or ())
        for action in actions:
            validate_finalization_action(action)
        finalization = dict(attempt.finalization)
        finalization.update(dict.fromkeys(actions, True))
        validate_required_finalization_actions(attempt, finalization)
        attempt.finalization = finalization
        self._transition(attempt, PHASE_FINALIZED)

    def record_finalization_action(self, attempt, action: str) -> None:
        """Persist one idempotent cleanup or metadata-restoration action."""
        if attempt.phase != PHASE_PUBLISHED:
            raise RecoveryManifestError(
                'Finalization progress requires a published attempt'
            )
        validate_finalization_action(action)
        finalization = dict(attempt.finalization)
        finalization[action] = True
        validate_phase_finalization_actions(attempt, finalization)
        attempt.finalization = finalization
        self._save_active_attempt(attempt)

    def complete_state_handoff(self, attempt) -> None:
        """Remove recovery only after the caller durably writes the saved bookmark."""
        if attempt.phase != PHASE_FINALIZED:
            raise RecoveryManifestError(
                'Iceberg state handoff requires a finalized attempt'
            )
        validate_required_finalization_actions(attempt)
        self._complete_attempt_cleanup(attempt)


# Compatibility alias for integrations that imported the pre-composition name.
IcebergFinalizationMixin = IcebergFinalizationService
