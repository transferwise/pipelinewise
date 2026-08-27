"""Tests for shared Snowflake Iceberg publication and recovery."""

from dataclasses import replace

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_versions as versions
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    IcebergColumn,
    IcebergPublicationAttempt,
    IcebergTargetAttemptPointer,
    IcebergTableSpec,
    PHASE_FINALIZED,
    PHASE_PUBLISHED,
    PHASE_STAGED,
    PHASE_SUBMITTED,
    PUBLICATION_MISSING_CTAS,
    SnowflakeTableSnapshot,
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
    TABLE_FORMAT_MISSING,
    TARGET_ATTEMPT_ACTIVE,
    TARGET_ATTEMPT_RESERVED,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    build_recovery_identity,
)


RECOVERY_IDENTITY = build_recovery_identity(
    "fastsync",
    {"route": "unit-test"},
    transformation_config={},
    stream_identity={"tap_id": "unit-test", "route": "unit-test", "table": "source.table"},
    target_table_format="iceberg",
    iceberg_version=3,
)


def _canonical_future_type(data_type):
    """Give a synthetic v4 a visibly different executable type strategy."""
    canonical = versions.MANAGED_ICEBERG_V3_SPEC.canonical_type(data_type)
    return 'NUMBER(20,0)' if canonical == 'NUMBER(38,0)' else canonical


def _canonical_future_existing_column(row):
    name, data_type, nullable = (
        versions.MANAGED_ICEBERG_V3_SPEC.canonical_existing_column(row)
    )
    return name, _canonical_future_type(data_type), nullable


def _validate_future_parameter_rows(rows, target, spec):
    versions.MANAGED_ICEBERG_V3_SPEC.validate_parameter_rows(
        rows,
        target,
        spec,
    )


def future_version_spec():
    """Return a complete synthetic v4 whose type hook differs from v3."""
    return replace(
        versions.MANAGED_ICEBERG_V3_SPEC,
        version=4,
        table_format='managed_iceberg_v4',
        logical_to_physical_types={
            **versions.MANAGED_ICEBERG_V3_SPEC.logical_to_physical_types,
            'number': 'number(20,0)',
        },
        table_options={
            **versions.MANAGED_ICEBERG_V3_SPEC.table_options,
            'FUTURE_VERSION_OPTIONS': True,
        },
        canonical_type=_canonical_future_type,
        canonical_existing_column=_canonical_future_existing_column,
        validate_parameter_rows=_validate_future_parameter_rows,
    )


def assert_managed_v3_copy_on_write_ddl(statement):
    """Require the exact non-deprecated managed-v3 copy-on-write DDL contract."""
    parameter = 'ICEBERG_MERGE_ON_READ_BEHAVIOR'
    assert statement.count(parameter) == 1
    assert f"{parameter} = 'DISABLED'" in statement
    assert 'ENABLE_ICEBERG_MERGE_ON_READ' not in statement


def assert_native_ddl_omits_iceberg_mode(statement):
    """Require native DDL to remain independent of Iceberg write-mode options."""
    assert 'ICEBERG_MERGE_ON_READ_BEHAVIOR' not in statement
    assert 'ENABLE_ICEBERG_MERGE_ON_READ' not in statement


class FakeSnowflake:
    """Record SQL while returning caller-provided metadata."""

    def __init__(self, responses=()):
        """Initialize the fake Snowflake adapter."""
        self.connection_config = {"dbname": "test_db"}
        self.responses = list(responses)
        self.queries = []
        self.query_timeouts = []
        self.transactions = []

    def query(self, query, params=None, query_tag_props=None):
        """Query."""
        self.queries.append((query, params, query_tag_props))
        response = self.responses.pop(0) if self.responses else []
        if isinstance(response, Exception):
            raise response  # pylint: disable=raising-bad-type
        return response

    def query_with_timeout(self, query, params, timeout_seconds):
        """Record a bounded recovery lookup."""
        self.query_timeouts.append(timeout_seconds)
        return self.query(query, params)

    def execute_transaction(self, queries, query_tag_props=None):
        """Execute transaction."""
        self.transactions.append((tuple(queries), query_tag_props))


class FakeClock:
    """Deterministic monotonic clock for polling tests."""

    def __init__(self):
        self.current = 0.0
        self.sleeps = []

    def monotonic(self):
        """Return the current monotonic time."""
        return self.current

    def sleep(self, seconds):
        """Advance the clock without blocking."""
        self.sleeps.append(seconds)
        self.current += seconds


@pytest.fixture(name="spec")
def fixture_spec():
    """Fixture spec."""
    return IcebergTableSpec.from_fastsync(
        "TEST_DB",
        "TEST_SCHEMA",
        "ORDERS",
        ['"ID" NUMBER', '"PAYLOAD" VARIANT', '"UPDATED AT" TIMESTAMP_NTZ'],
        ['"ID"'],
    )


def missing_snapshot():
    """Missing snapshot."""
    return SnowflakeTableSnapshot(TABLE_FORMAT_MISSING, None, None)


def v3_snapshot(spec, identity="target-id"):
    """V3 snapshot."""
    return SnowflakeTableSnapshot(TABLE_FORMAT_MANAGED_ICEBERG_V3, spec, identity)


def make_attempt(
    spec,
    phase=PHASE_STAGED,
    kind="full",
    context=None,
    method=PUBLICATION_MISSING_CTAS,
    snapshot=None,
):
    """Make attempt."""
    snapshot = snapshot or missing_snapshot()
    load_id = "1" * 32
    attempt_context = dict(context or {})
    if kind == "partial":
        attempt_context.setdefault("column_name", "ID")
        attempt_context.setdefault("start_value", 1)
        attempt_context.setdefault("end_value", None)
        attempt_context.setdefault(
            "end_is_unbounded", attempt_context["end_value"] is None
        )
        attempt_context.setdefault("drop_target", False)
        attempt_context.setdefault("delete_mode", "hard")
    if phase == PHASE_SUBMITTED:
        attempt_context.setdefault("publication_submitted_at", 1_700_000_000.0)
    has_staging_evidence = phase in (
        PHASE_STAGED,
        PHASE_SUBMITTED,
        PHASE_PUBLISHED,
        PHASE_FINALIZED,
    )
    return IcebergPublicationAttempt(
        load_id=load_id,
        attempt_id="2" * 32,
        kind=kind,
        table_spec=spec,
        source_bookmark={"lsn": "1/2"},
        intended_state={"bookmarks": {}},
        staging_table=spec.name.staging_name(load_id),
        method=method,
        pre_publication_target_fingerprint=snapshot.fingerprint,
        target_table_format="iceberg",
        iceberg_version=3,
        phase=phase,
        expected_row_count=0 if has_staging_evidence else None,
        expected_row_fingerprint="fixture-hash" if has_staging_evidence else None,
        recovery_identity=RECOVERY_IDENTITY,
        context=attempt_context,
    )


def persist_attempt(publisher, attempt, pointer_state=TARGET_ATTEMPT_ACTIVE):
    """Persist a test attempt through the production target/stream ordering."""
    with publisher.table_lock(attempt.target, attempt.recovery_identity):
        target_store = publisher.recovery_store(attempt.target)
        target_store.save_fastsync_target_pointer(
            IcebergTargetAttemptPointer.from_attempt(
                attempt,
                TARGET_ATTEMPT_RESERVED,
            )
        )
        publisher.recovery_store(
            attempt.target,
            attempt.recovery_identity,
        ).save(attempt)
        target_store.save_fastsync_target_pointer(IcebergTargetAttemptPointer.from_attempt(attempt, pointer_state))
    return attempt


def replace_nullable(column):
    """Return a nullable copy of an Iceberg column."""
    return IcebergColumn(column.name, column.data_type, True)
