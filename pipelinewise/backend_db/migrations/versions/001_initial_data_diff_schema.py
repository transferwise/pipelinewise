"""initial data diff schema

Revision ID: 001
Revises: None
Create Date: 2026-07-24
"""

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None

SCHEMA = "public"


def upgrade():

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_check_definitions (
            check_id UUID PRIMARY KEY,
            full_check_name TEXT NOT NULL,
            revision INTEGER NOT NULL,
            config_hash CHAR(64) NOT NULL,
            canonical_config JSONB NOT NULL,
            target_id TEXT NOT NULL,
            tap_id TEXT NOT NULL,
            source_type TEXT NOT NULL,
            target_type TEXT NOT NULL,
            source_database TEXT NOT NULL,
            target_database TEXT NOT NULL,
            source_schema TEXT NOT NULL,
            source_table TEXT NOT NULL,
            target_schema TEXT NOT NULL,
            target_table TEXT NOT NULL,
            source_key_column TEXT NOT NULL,
            target_key_column TEXT NOT NULL,
            source_timestamp_column TEXT NOT NULL,
            target_timestamp_column TEXT NOT NULL,
            checks JSONB NOT NULL,
            frequency TEXT NOT NULL,
            window_start_seconds BIGINT NOT NULL CHECK (window_start_seconds > 0),
            window_end_seconds BIGINT NOT NULL CHECK (window_end_seconds >= 0),
            statement_timeout_seconds BIGINT NOT NULL CHECK (statement_timeout_seconds > 0),
            is_current BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            superseded_at TIMESTAMPTZ,
            UNIQUE (full_check_name, revision)
        )
    """)

    op.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_dd_check_definitions_is_current_name
            ON {SCHEMA}.dd_check_definitions(full_check_name)
            WHERE is_current
    """)

    op.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_dd_check_definitions_is_current_scope
            ON {SCHEMA}.dd_check_definitions(is_current, target_id, tap_id)
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_preflight_log (
            preflight_id UUID PRIMARY KEY,
            check_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_check_definitions(check_id) ON DELETE RESTRICT,
            status TEXT NOT NULL CHECK (status IN ('PASS', 'BLOCKED', 'ERROR')),
            checked_at TIMESTAMPTZ NOT NULL,
            query_fingerprint CHAR(64) NOT NULL,
            index_metadata JSONB NOT NULL,
            findings JSONB NOT NULL,
            error TEXT,
            -- The inputs the PASS/BLOCKED verdict was made from, so a decision
            -- stays auditable after the threshold or the table size changes.
            table_rows BIGINT,
            row_limit BIGINT,
            has_leading_index BOOLEAN
        )
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_run_attempts (
            run_id UUID PRIMARY KEY,
            check_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_check_definitions(check_id) ON DELETE RESTRICT,
            scheduled_for TIMESTAMPTZ NOT NULL,
            window_start TIMESTAMPTZ NOT NULL,
            window_end TIMESTAMPTZ NOT NULL,
            attempt INTEGER NOT NULL,
            trigger_type TEXT NOT NULL
                CONSTRAINT ck_dd_run_attempts_trigger_type
                CHECK (trigger_type IN ('SCHEDULED', 'MANUAL', 'RETRY', 'REMEDIATION')),
            status TEXT NOT NULL CHECK (status IN ('RUNNING', 'PASS', 'FAIL', 'ERROR')),
            rerun_of_run_id UUID REFERENCES {SCHEMA}.dd_run_attempts(run_id) ON DELETE RESTRICT,
            remediation_reference TEXT,
            preflight_id UUID REFERENCES {SCHEMA}.dd_preflight_log(preflight_id) ON DELETE RESTRICT,
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ,
            error TEXT,
            CHECK (window_start < window_end),
            UNIQUE (check_id, scheduled_for, attempt)
        )
    """)

    op.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_dd_run_attempts_schedule
            ON {SCHEMA}.dd_run_attempts(check_id, scheduled_for, status)
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_run_results (
            run_id UUID NOT NULL REFERENCES {SCHEMA}.dd_run_attempts(run_id) ON DELETE RESTRICT,
            check_type TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL', 'ERROR')),
            source_value JSONB,
            target_value JSONB,
            source_query_seconds DOUBLE PRECISION,
            target_query_seconds DOUBLE PRECISION,
            error TEXT,
            PRIMARY KEY (run_id, check_type)
        )
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_run_slot_state (
            check_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_check_definitions(check_id) ON DELETE RESTRICT,
            scheduled_for TIMESTAMPTZ NOT NULL,
            run_id UUID NOT NULL UNIQUE
                REFERENCES {SCHEMA}.dd_run_attempts(run_id) ON DELETE RESTRICT,
            attempt INTEGER NOT NULL CHECK (attempt > 0),
            window_start TIMESTAMPTZ NOT NULL,
            window_end TIMESTAMPTZ NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL', 'ERROR')),
            CHECK (window_start < window_end),
            PRIMARY KEY (check_id, scheduled_for)
        )
    """)

    op.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_dd_run_slot_state_coverage_order
            ON {SCHEMA}.dd_run_slot_state(
                check_id, window_start, window_end, scheduled_for
            )
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_watermark_state (
            check_id UUID PRIMARY KEY
                REFERENCES {SCHEMA}.dd_check_definitions(check_id) ON DELETE RESTRICT,
            coverage_start TIMESTAMPTZ NOT NULL,
            verified_through TIMESTAMPTZ NOT NULL,
            max_observed_end TIMESTAMPTZ NOT NULL,
            coverage_status TEXT NOT NULL CHECK (coverage_status IN ('CONTIGUOUS', 'BLOCKED')),
            blocking_run_id UUID REFERENCES {SCHEMA}.dd_run_attempts(run_id) ON DELETE RESTRICT,
            evaluated_run_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_run_attempts(run_id) ON DELETE RESTRICT,
            event_type TEXT NOT NULL
                CHECK (event_type IN ('INITIALIZE', 'ADVANCE', 'INVALIDATE', 'BLOCK', 'CONFIRM')),
            state_version BIGINT NOT NULL CHECK (state_version > 0),
            updated_at TIMESTAMPTZ NOT NULL,
            reason TEXT NOT NULL,
            CHECK (coverage_start <= verified_through),
            CHECK (verified_through <= max_observed_end)
        )
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_watermark_events (
            watermark_event_id UUID PRIMARY KEY,
            event_sequence BIGSERIAL NOT NULL UNIQUE,
            check_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_check_definitions(check_id) ON DELETE RESTRICT,
            evaluated_run_id UUID NOT NULL UNIQUE
                REFERENCES {SCHEMA}.dd_run_attempts(run_id) ON DELETE RESTRICT,
            event_type TEXT NOT NULL
                CHECK (event_type IN ('INITIALIZE', 'ADVANCE', 'INVALIDATE', 'BLOCK', 'CONFIRM')),
            coverage_start TIMESTAMPTZ NOT NULL,
            previous_verified_through TIMESTAMPTZ,
            verified_through TIMESTAMPTZ NOT NULL,
            max_observed_end TIMESTAMPTZ NOT NULL,
            coverage_status TEXT NOT NULL CHECK (coverage_status IN ('CONTIGUOUS', 'BLOCKED')),
            blocking_run_id UUID REFERENCES {SCHEMA}.dd_run_attempts(run_id) ON DELETE RESTRICT,
            recorded_at TIMESTAMPTZ NOT NULL,
            reason TEXT NOT NULL,
            CHECK (coverage_start <= verified_through),
            CHECK (verified_through <= max_observed_end)
        )
    """)

    op.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_dd_watermark_events_history
            ON {SCHEMA}.dd_watermark_events(check_id, event_sequence DESC)
    """)

    # Table comments
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_check_definitions IS "
        "'Versioned data-diff check definitions imported from YAML configuration'"
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_preflight_log IS "
        "'Append-only preflight evidence and early validation errors for run attempts'"
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_run_attempts IS "
        "'Execution attempts updated from RUNNING to a terminal outcome'"
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_run_results IS "
        "'Append-only per-check-type outcomes for completed run attempts'"
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_run_slot_state IS "
        "'Mutable selected terminal attempt for each check and scheduled slot'"
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_watermark_state IS "
        "'Mutable current verified-through watermark for each check definition'"
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_watermark_events IS "
        "'Append-only history of verified-through watermark transitions'"
    )
    _grant_application_privileges()


def _grant_application_privileges():
    """Grant the application user access to objects owned by ``ddl_user``.

    Migrations run as ``ddl_user``, which owns everything it creates. Without
    these grants a separate application user cannot read its own backend. No-op
    when both roles are the same, or when the application user is unknown.
    """
    application_user = op.get_context().config.get_main_option(
        "pipelinewise_application_user"
    )
    if not application_user:
        return

    role = _quote_identifier(application_user)
    for table in (
        "dd_check_definitions", "dd_preflight_log", "dd_run_attempts", "dd_run_results",
        "dd_run_slot_state", "dd_watermark_state", "dd_watermark_events",
    ):
        op.execute(
            f"GRANT SELECT, INSERT, UPDATE ON {SCHEMA}.{table} TO {role}"
        )
    # dd_watermark_events.event_sequence is a BIGSERIAL: inserting needs the
    # sequence, not just the table.
    op.execute(
        f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {SCHEMA} TO {role}"
    )

    # alembic_version is read on every connection that checks the schema version.
    op.execute(f"GRANT SELECT ON {SCHEMA}.alembic_version TO {role}")
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {role}")


def _quote_identifier(name):
    """Quote a role name so mixed case and special characters survive."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def downgrade():
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_watermark_events CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_watermark_state CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_run_slot_state CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_run_results CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_run_attempts CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_preflight_log CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_check_definitions CASCADE")
