"""Target-scoped recovery stores, locks, and pointer reconciliation."""

from contextlib import ExitStack, contextmanager
import os
from typing import Any, Callable, Dict, Optional
from uuid import uuid4

from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    PHASE_FINALIZED,
    PHASE_PUBLISHED,
    PHASE_STAGED,
    PHASE_SUBMITTED,
    PUBLICATION_PARTIAL_MERGE,
    TARGET_ATTEMPT_ABORTING,
    TARGET_ATTEMPT_ACTIVE,
    TARGET_ATTEMPT_COMPLETED,
    TARGET_ATTEMPT_RESERVED,
    IcebergPublicationAttempt,
    IcebergRecoveryStore,
    IcebergTargetAttemptPointer,
    SnowflakeObjectName,
    validate_recovery_identity,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    RecoveryManifestError,
)


def target_runtime_dir(
    state_path: Optional[str],
    temp_dir: str,
    *,
    require_target_scope: bool,
) -> str:
    """Resolve the one target directory shared by FastSync and conversion."""
    if state_path:
        return os.path.dirname(os.path.dirname(os.path.abspath(state_path)))
    if require_target_scope:
        raise ValueError(
            'Snowflake Iceberg FastSync requires a state file for target-scoped recovery'
        )
    return os.path.abspath(temp_dir)


class RecoveryCoordinator:
    """Own every store and lock participating in one target lifecycle."""

    def __init__(
        self,
        runtime_dir: str,
        attempt_validator: Optional[Callable[[IcebergPublicationAttempt], None]] = None,
    ):
        self.runtime_dir = os.path.realpath(runtime_dir)
        self.attempt_validator = attempt_validator
        self._stores = {}

    def recovery_store(
        self,
        target: SnowflakeObjectName,
        recovery_identity: Optional[Dict] = None,
    ) -> IcebergRecoveryStore:
        """Return one reentrant manifest store for a target or source stream."""
        recovery_key = None
        if recovery_identity is not None:
            validate_recovery_identity(recovery_identity)
            if recovery_identity['scope'] == 'fastsync':
                recovery_key = recovery_identity['stream_fingerprint']
        return self._recovery_store(target, recovery_key)

    def _recovery_store(
        self,
        target: SnowflakeObjectName,
        recovery_key: Optional[str],
    ) -> IcebergRecoveryStore:
        store_identity = (
            ('stream', recovery_key)
            if recovery_key is not None
            else ('target', target.database, target.schema, target.table)
        )
        if store_identity not in self._stores:
            self._stores[store_identity] = IcebergRecoveryStore(
                self.runtime_dir,
                target,
                recovery_key=recovery_key,
            )
        return self._stores[store_identity]

    @contextmanager
    def table_lock(
        self,
        target: SnowflakeObjectName,
        recovery_identity: Optional[Dict] = None,
    ):
        """Serialize both source-stream and physical-target lifecycle state."""
        stores = {
            store.lock_path: store
            for store in (
                self.recovery_store(target, recovery_identity),
                self.recovery_store(target),
            )
        }
        with ExitStack() as stack:
            for lock_path in sorted(stores):
                stack.enter_context(stores[lock_path].locked())
            yield

    def reconcile_target_attempt(
        self,
        target: SnowflakeObjectName,
        recovery_identity: Dict,
    ) -> Optional[IcebergPublicationAttempt]:
        """Reconcile the target pointer with its canonical stream manifest."""
        target_store = self.recovery_store(target)
        stream_store = self.recovery_store(target, recovery_identity)
        manual_attempt = target_store.load()
        pointer = target_store.load_fastsync_target_pointer()
        current_attempt = stream_store.load()

        if current_attempt is not None:
            self.validate_attempt_identity(current_attempt, recovery_identity)
            if current_attempt.target != target:
                raise RecoveryManifestError(
                    'Iceberg recovery manifest target does not match the requested target'
                )

        self._reject_manual_attempt(manual_attempt, pointer, current_attempt)

        if pointer is None:
            if current_attempt is not None:
                raise RecoveryManifestError(
                    'Iceberg FastSync stream manifest has no target attempt pointer'
                )
            return None
        if pointer.target != target:
            raise RecoveryManifestError(
                'Iceberg FastSync target attempt pointer belongs to another target'
            )

        pointed_store = self._recovery_store(target, pointer.stream_fingerprint)
        pointed_attempt = self._load_pointed_attempt(
            pointer,
            recovery_identity,
            current_attempt,
            pointed_store,
        )
        if pointed_attempt is None:
            return self._reconcile_pointer_without_manifest(pointer, target_store)

        self._validate_target_pointer(pointer, pointed_attempt)
        self._validate_attempt(pointed_attempt)
        if self._reconcile_pointer_state(
            pointer,
            pointed_attempt,
            target_store,
            pointed_store,
        ):
            return None
        if pointer.stream_fingerprint != recovery_identity['stream_fingerprint']:
            raise RecoveryManifestError(
                'An Iceberg FastSync attempt for a different source stream is active '
                'for this target'
            )
        return pointed_attempt

    def persist_new_attempt(self, attempt: IcebergPublicationAttempt) -> None:
        """Reserve a target before publishing its canonical stream manifest."""
        target_store = self.recovery_store(attempt.target)
        stream_store = self.recovery_store(
            attempt.target,
            attempt.recovery_identity,
        )
        target_store.save_fastsync_target_pointer(
            IcebergTargetAttemptPointer.from_attempt(
                attempt,
                TARGET_ATTEMPT_RESERVED,
            )
        )
        stream_store.save(attempt)
        target_store.save_fastsync_target_pointer(
            IcebergTargetAttemptPointer.from_attempt(
                attempt,
                TARGET_ATTEMPT_ACTIVE,
            )
        )

    def transition(
        self,
        attempt: IcebergPublicationAttempt,
        phase: str,
        **updates: Any,
    ) -> None:
        """Validate, serialize, and persist one active lifecycle transition."""
        attempt.validate_transition_to(phase)
        with self.table_lock(attempt.target, attempt.recovery_identity):
            persisted = self.reconcile_target_attempt(
                attempt.target,
                attempt.recovery_identity,
            )
            if persisted is None or persisted.load_id != attempt.load_id:
                raise RecoveryManifestError(
                    'Iceberg recovery attempt changed before state transition'
                )
            for name, value in updates.items():
                setattr(attempt, name, value)
            attempt.transition_to(phase)
            self.recovery_store(
                attempt.target,
                attempt.recovery_identity,
            ).save(attempt)

    def save_active_attempt(self, attempt: IcebergPublicationAttempt) -> None:
        """Persist payload or finalization progress for the active attempt."""
        with self.table_lock(attempt.target, attempt.recovery_identity):
            persisted = self.reconcile_target_attempt(
                attempt.target,
                attempt.recovery_identity,
            )
            if persisted is None or persisted.load_id != attempt.load_id:
                raise RecoveryManifestError(
                    'Iceberg recovery attempt changed before manifest update'
                )
            self.recovery_store(
                attempt.target,
                attempt.recovery_identity,
            ).save(attempt)

    def rearm_partial_merge_replay(
        self,
        attempt: IcebergPublicationAttempt,
    ) -> None:
        """Give one ambiguous Partial MERGE replay a fresh submission identity."""
        if (
            attempt.kind != 'partial'
            or attempt.method != PUBLICATION_PARTIAL_MERGE
            or attempt.phase != PHASE_SUBMITTED
        ):
            raise RecoveryManifestError(
                'Only a submitted Iceberg PartialSync MERGE can be rearmed'
            )
        attempt.validate_transition_to(PHASE_STAGED)
        with self.table_lock(attempt.target, attempt.recovery_identity):
            persisted = self.reconcile_target_attempt(
                attempt.target,
                attempt.recovery_identity,
            )
            if (
                persisted is None
                or persisted.load_id != attempt.load_id
                or persisted.attempt_id != attempt.attempt_id
                or persisted.phase != PHASE_SUBMITTED
            ):
                raise RecoveryManifestError(
                    'Iceberg recovery attempt changed before PartialSync replay'
                )
            attempt.attempt_id = uuid4().hex
            attempt.query_id = None
            attempt.update_manifest_payload(remove=(
                'publication_query_hash',
                'publication_query_type',
                'publication_submitted_at',
            ))
            attempt.transition_to(PHASE_STAGED)
            self.recovery_store(
                attempt.target,
                attempt.recovery_identity,
            ).save(attempt)

    def complete_attempt_cleanup(
        self,
        attempt: IcebergPublicationAttempt,
    ) -> None:
        """Complete target pointer and manifest removal after state handoff."""
        with self.table_lock(attempt.target, attempt.recovery_identity):
            persisted = self.reconcile_target_attempt(
                attempt.target,
                attempt.recovery_identity,
            )
            if persisted is None:
                return
            if persisted.load_id != attempt.load_id:
                raise RecoveryManifestError(
                    'Iceberg recovery attempt changed before cleanup'
                )
            target_store = self.recovery_store(attempt.target)
            stream_store = self.recovery_store(
                attempt.target,
                attempt.recovery_identity,
            )
            target_store.save_fastsync_target_pointer(
                IcebergTargetAttemptPointer.from_attempt(
                    persisted,
                    TARGET_ATTEMPT_COMPLETED,
                )
            )
            stream_store.delete(persisted.attempt_id)
            target_store.delete_fastsync_target_pointer(
                attempt.recovery_identity['stream_fingerprint']
            )

    def abort_attempt_cleanup(
        self,
        attempt: IcebergPublicationAttempt,
    ) -> None:
        """Remove pre-publication recovery state after caller cleanup succeeds."""
        with self.table_lock(attempt.target, attempt.recovery_identity):
            persisted = self.reconcile_target_attempt(
                attempt.target,
                attempt.recovery_identity,
            )
            if persisted is None:
                return
            if persisted.load_id != attempt.load_id:
                raise RecoveryManifestError(
                    'Iceberg recovery attempt changed before cleanup'
                )
            if persisted.phase in (
                PHASE_FINALIZED,
                PHASE_PUBLISHED,
                PHASE_SUBMITTED,
            ):
                raise RecoveryManifestError(
                    'Cannot abort after publication submission'
                )
            target_store = self.recovery_store(attempt.target)
            stream_store = self.recovery_store(
                attempt.target,
                attempt.recovery_identity,
            )
            target_store.save_fastsync_target_pointer(
                IcebergTargetAttemptPointer.from_attempt(
                    persisted,
                    TARGET_ATTEMPT_ABORTING,
                )
            )
            stream_store.delete(persisted.attempt_id)
            target_store.delete_fastsync_target_pointer(
                attempt.recovery_identity['stream_fingerprint']
            )

    def save_conversion_attempt(
        self,
        attempt: IcebergPublicationAttempt,
    ) -> None:
        """Persist one target-keyed manual conversion attempt."""
        validate_recovery_identity(attempt.recovery_identity)
        if attempt.recovery_identity['scope'] != 'manual_conversion':
            raise RecoveryManifestError(
                'Only manual conversion attempts use target-keyed manifests'
            )
        self.recovery_store(attempt.target).save(attempt)

    def delete_conversion_attempt(
        self,
        attempt: IcebergPublicationAttempt,
    ) -> None:
        """Delete the exact target-keyed manual conversion attempt."""
        validate_recovery_identity(attempt.recovery_identity)
        if attempt.recovery_identity['scope'] != 'manual_conversion':
            raise RecoveryManifestError(
                'Only manual conversion attempts use target-keyed manifests'
            )
        self.recovery_store(attempt.target).delete(attempt.attempt_id)

    def _validate_attempt(self, attempt: IcebergPublicationAttempt) -> None:
        if self.attempt_validator is not None:
            self.attempt_validator(attempt)

    @staticmethod
    def validate_attempt_identity(attempt, expected_identity):
        """Require the exact durable source, target, and transformation identity."""
        validate_recovery_identity(attempt.recovery_identity)
        if attempt.recovery_identity != expected_identity:
            raise RecoveryManifestError(
                'Iceberg recovery manifest belongs to a different source, '
                'target, staging configuration, or transformation contract'
            )

    @staticmethod
    def _reject_manual_attempt(manual_attempt, pointer, current_attempt) -> None:
        if manual_attempt is None:
            return
        validate_recovery_identity(manual_attempt.recovery_identity)
        if manual_attempt.recovery_identity['scope'] != 'manual_conversion':
            raise RecoveryManifestError(
                'Target-keyed Iceberg recovery manifest has an invalid scope'
            )
        if pointer is not None or current_attempt is not None:
            raise RecoveryManifestError(
                'Multiple Iceberg recovery attempts exist for the same target'
            )
        raise RecoveryManifestError(
            'A native-to-Iceberg conversion attempt is active for this target'
        )

    @staticmethod
    def _load_pointed_attempt(
        pointer,
        recovery_identity,
        current_attempt,
        pointed_store,
    ):
        if pointer.stream_fingerprint == recovery_identity['stream_fingerprint']:
            return current_attempt
        if current_attempt is not None:
            raise RecoveryManifestError(
                'Multiple Iceberg FastSync stream manifests exist for the same target'
            )
        return pointed_store.load_locked()

    @staticmethod
    def _reconcile_pointer_without_manifest(pointer, target_store):
        if pointer.state in (
            TARGET_ATTEMPT_ABORTING,
            TARGET_ATTEMPT_COMPLETED,
            TARGET_ATTEMPT_RESERVED,
        ):
            target_store.delete_fastsync_target_pointer(pointer.stream_fingerprint)
            return None
        raise RecoveryManifestError(
            'Active Iceberg FastSync target pointer has no stream manifest'
        )

    @staticmethod
    def _reconcile_pointer_state(
        pointer,
        pointed_attempt,
        target_store,
        pointed_store,
    ) -> bool:
        if pointer.state == TARGET_ATTEMPT_ABORTING:
            if pointed_attempt.phase in (
                PHASE_FINALIZED,
                PHASE_PUBLISHED,
                PHASE_SUBMITTED,
            ):
                raise RecoveryManifestError(
                    'Aborting Iceberg FastSync target pointer has an unsafe stream manifest'
                )
            pointed_store.delete_locked(pointed_attempt.attempt_id)
            target_store.delete_fastsync_target_pointer(
                pointer.stream_fingerprint
            )
            return True
        if pointer.state == TARGET_ATTEMPT_RESERVED:
            target_store.save_fastsync_target_pointer(
                IcebergTargetAttemptPointer.from_attempt(
                    pointed_attempt,
                    TARGET_ATTEMPT_ACTIVE,
                )
            )
            return False
        if pointer.state != TARGET_ATTEMPT_COMPLETED:
            return False
        if pointed_attempt.phase != PHASE_FINALIZED:
            raise RecoveryManifestError(
                'Completed Iceberg FastSync target pointer has an unsafe stream manifest'
            )
        pointed_store.delete_locked(pointed_attempt.attempt_id)
        target_store.delete_fastsync_target_pointer(pointer.stream_fingerprint)
        return True

    @staticmethod
    def _validate_target_pointer(
        pointer: IcebergTargetAttemptPointer,
        attempt: IcebergPublicationAttempt,
    ) -> None:
        validate_recovery_identity(attempt.recovery_identity)
        if (
            attempt.recovery_identity['scope'] != 'fastsync'
            or attempt.target != pointer.target
            or attempt.recovery_identity['stream_fingerprint']
            != pointer.stream_fingerprint
            or attempt.recovery_identity['fingerprint']
            != pointer.recovery_fingerprint
            or attempt.kind != pointer.kind
        ):
            raise RecoveryManifestError(
                'Iceberg FastSync target pointer and stream manifest are inconsistent'
            )
