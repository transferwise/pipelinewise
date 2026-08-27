"""Query-history polling tests for Snowflake Iceberg recovery."""

from unittest.mock import MagicMock, patch

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    PHASE_SUBMITTED,
    QueryHistoryVisibilityTimeoutError,
    RECOVERY_FINALIZE,
    SnowflakeIcebergPublisher,
    _sql_hash,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    FakeClock,
    FakeSnowflake,
    make_attempt,
    persist_attempt,
    v3_snapshot,
)


def test_default_budget_waits_beyond_minute(
    tmp_path,
    spec,
):
    """The default recovery budget waits beyond the former 60-second limit."""
    statement = 'CREATE ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS"'
    running = [{
        'QUERY_ID': 'query-id',
        'QUERY_TEXT': statement,
        'QUERY_TYPE': 'CREATE_TABLE_AS_SELECT',
        'EXECUTION_STATUS': 'RUNNING',
    }]
    success = [{
        **running[0],
        'EXECUTION_STATUS': 'SUCCESS',
    }]
    snowflake = FakeSnowflake([
        *([running] * 13),
        success,
        [{'ROW_COUNT': 0, 'ROW_FINGERPRINT': 'fixture-hash'}],
    ])
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
    publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
    attempt = make_attempt(
        spec,
        phase=PHASE_SUBMITTED,
        context={
            'publication_query_hash': _sql_hash(statement),
            'publication_query_type': 'CREATE_TABLE_AS_SELECT',
        },
    )
    persist_attempt(publisher, attempt)
    clock = FakeClock()

    with (
        patch(
            'pipelinewise.fastsync.commons.snowflake_iceberg_publication.time.monotonic',
            side_effect=clock.monotonic,
        ),
        patch(
            'pipelinewise.fastsync.commons.snowflake_iceberg_publication.time.sleep',
            side_effect=clock.sleep,
        ),
    ):
        outcome = publisher.reconcile(attempt, spec)

    assert outcome.action == RECOVERY_FINALIZE
    assert publisher.history_policy.timeout_seconds == 900
    assert clock.sleeps == [5.0] * 13
    assert sum(clock.sleeps) == 65.0
    assert snowflake.query_timeouts == [30.0] * 14
    assert 'END_TIME_RANGE_END' not in snowflake.queries[0][0]


def test_timeout_reports_active_status(tmp_path, spec):
    """A visible non-terminal query is distinguished from absent history."""
    running = [{'EXECUTION_STATUS': 'RUNNING'}]
    publisher = SnowflakeIcebergPublisher(
        FakeSnowflake([running, running]),
        str(tmp_path),
        history_poll_attempts=2,
    )
    attempt = make_attempt(spec, phase=PHASE_SUBMITTED)
    clock = FakeClock()

    with (
        patch(
            'pipelinewise.fastsync.commons.snowflake_iceberg_publication.time.monotonic',
            side_effect=clock.monotonic,
        ),
        patch(
            'pipelinewise.fastsync.commons.snowflake_iceberg_publication.time.sleep',
            side_effect=clock.sleep,
        ),
        pytest.raises(
            QueryHistoryVisibilityTimeoutError,
            match='remained non-terminal.*last statuses: running',
        ) as error,
    ):
        publisher.reconcile(attempt, spec)

    assert error.value.last_statuses == ('running',)
    assert clock.sleeps == [5.0]
