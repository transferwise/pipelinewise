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
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_checks (
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
            current BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            superseded_at TIMESTAMPTZ,
            UNIQUE (full_check_name, revision)
        )
    """)

    op.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS dd_one_current_version
            ON {SCHEMA}.dd_checks(full_check_name)
            WHERE current
    """)

    op.execute(f"""
        CREATE INDEX IF NOT EXISTS dd_current_scope
            ON {SCHEMA}.dd_checks(current, target_id, tap_id)
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_preflights (
            preflight_id UUID PRIMARY KEY,
            check_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_checks(check_id) ON DELETE RESTRICT,
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
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_runs (
            run_id UUID PRIMARY KEY,
            dd_check_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_checks(check_id) ON DELETE RESTRICT,
            scheduled_for TIMESTAMPTZ NOT NULL,
            window_start TIMESTAMPTZ NOT NULL,
            window_end TIMESTAMPTZ NOT NULL,
            attempt INTEGER NOT NULL,
            trigger TEXT NOT NULL
                CONSTRAINT dd_runs_trigger_check
                CHECK (trigger IN ('SCHEDULED', 'MANUAL', 'RETRY', 'REMEDIATION')),
            status TEXT NOT NULL CHECK (status IN ('RUNNING', 'PASS', 'FAIL', 'ERROR')),
            rerun_of_run_id UUID REFERENCES {SCHEMA}.dd_runs(run_id) ON DELETE RESTRICT,
            remediation_reference TEXT,
            preflight_id UUID REFERENCES {SCHEMA}.dd_preflights(preflight_id) ON DELETE RESTRICT,
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ,
            error TEXT,
            CHECK (window_start < window_end),
            UNIQUE (dd_check_id, scheduled_for, attempt)
        )
    """)

    op.execute(f"""
        CREATE INDEX IF NOT EXISTS dd_runs_schedule
            ON {SCHEMA}.dd_runs(dd_check_id, scheduled_for, status)
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_results (
            run_id UUID NOT NULL REFERENCES {SCHEMA}.dd_runs(run_id) ON DELETE RESTRICT,
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
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_effective_attempts (
            check_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_checks(check_id) ON DELETE RESTRICT,
            scheduled_for TIMESTAMPTZ NOT NULL,
            run_id UUID NOT NULL UNIQUE
                REFERENCES {SCHEMA}.dd_runs(run_id) ON DELETE RESTRICT,
            attempt INTEGER NOT NULL CHECK (attempt > 0),
            window_start TIMESTAMPTZ NOT NULL,
            window_end TIMESTAMPTZ NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL', 'ERROR')),
            CHECK (window_start < window_end),
            PRIMARY KEY (check_id, scheduled_for)
        )
    """)

    op.execute(f"""
        CREATE INDEX IF NOT EXISTS dd_effective_coverage_order
            ON {SCHEMA}.dd_effective_attempts(
                check_id, window_start, window_end, scheduled_for
            )
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_coverage_state (
            check_id UUID PRIMARY KEY
                REFERENCES {SCHEMA}.dd_checks(check_id) ON DELETE RESTRICT,
            coverage_start TIMESTAMPTZ NOT NULL,
            verified_through TIMESTAMPTZ NOT NULL,
            max_observed_end TIMESTAMPTZ NOT NULL,
            coverage_status TEXT NOT NULL CHECK (coverage_status IN ('CONTIGUOUS', 'BLOCKED')),
            blocking_run_id UUID REFERENCES {SCHEMA}.dd_runs(run_id) ON DELETE RESTRICT,
            evaluated_run_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_runs(run_id) ON DELETE RESTRICT,
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
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dd_coverage_events (
            coverage_event_id UUID PRIMARY KEY,
            event_sequence BIGSERIAL NOT NULL UNIQUE,
            check_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_checks(check_id) ON DELETE RESTRICT,
            evaluated_run_id UUID NOT NULL UNIQUE
                REFERENCES {SCHEMA}.dd_runs(run_id) ON DELETE RESTRICT,
            event_type TEXT NOT NULL
                CHECK (event_type IN ('INITIALIZE', 'ADVANCE', 'INVALIDATE', 'BLOCK', 'CONFIRM')),
            coverage_start TIMESTAMPTZ NOT NULL,
            previous_verified_through TIMESTAMPTZ,
            verified_through TIMESTAMPTZ NOT NULL,
            max_observed_end TIMESTAMPTZ NOT NULL,
            coverage_status TEXT NOT NULL CHECK (coverage_status IN ('CONTIGUOUS', 'BLOCKED')),
            blocking_run_id UUID REFERENCES {SCHEMA}.dd_runs(run_id) ON DELETE RESTRICT,
            recorded_at TIMESTAMPTZ NOT NULL,
            reason TEXT NOT NULL,
            CHECK (coverage_start <= verified_through),
            CHECK (verified_through <= max_observed_end)
        )
    """)

    op.execute(f"""
        CREATE INDEX IF NOT EXISTS dd_coverage_history
            ON {SCHEMA}.dd_coverage_events(check_id, event_sequence DESC)
    """)

    op.execute(f"""
        CREATE OR REPLACE VIEW {SCHEMA}.dd_current_coverage AS
        SELECT state.check_id, checks.full_check_name,
               checks.revision, checks.source_schema, checks.source_table,
               checks.source_timestamp_column, state.coverage_start,
               state.verified_through, state.max_observed_end,
               state.coverage_status, state.blocking_run_id,
               state.evaluated_run_id, state.event_type,
               state.updated_at AS verified_at, state.reason
          FROM {SCHEMA}.dd_coverage_state state
          JOIN {SCHEMA}.dd_checks checks
            ON checks.check_id = state.check_id
    """)

    op.execute(f"""
        CREATE OR REPLACE VIEW {SCHEMA}.dd_remediation_history AS
        SELECT checks.full_check_name, checks.revision,
               original.run_id AS failed_run_id, original.status AS failed_status,
               original.window_start, original.window_end,
               original.finished_at AS failed_at,
               remediation.run_id AS remediation_run_id,
               remediation.attempt AS remediation_attempt,
               remediation.status AS remediation_status,
               remediation.remediation_reference,
               remediation.finished_at AS remediation_finished_at,
               COALESCE(remediation.status = 'PASS', FALSE) AS recovered
          FROM {SCHEMA}.dd_runs original
          JOIN {SCHEMA}.dd_checks checks
            ON checks.check_id = original.dd_check_id
          LEFT JOIN {SCHEMA}.dd_runs remediation
            ON remediation.rerun_of_run_id = original.run_id
         WHERE original.rerun_of_run_id IS NULL
           AND original.status IN ('FAIL', 'ERROR')
    """)

    # Table and view comments
    op.execute(f"COMMENT ON TABLE {SCHEMA}.dd_checks IS 'Versioned check definitions imported from YAML config'")
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_preflights IS "
        "'Source size and index validation recorded before each check execution'"
    )
    op.execute(f"COMMENT ON TABLE {SCHEMA}.dd_runs IS 'Shared execution log — one row per scheduled job attempt'")
    op.execute(f"COMMENT ON TABLE {SCHEMA}.dd_results IS 'Per-check-type outcome detail for each data-diff run'")
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_effective_attempts IS "
        "'Latest terminal attempt for each data-diff scheduled slot'"
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_coverage_state IS "
        "'Materialized current verified-through watermark for each data-diff check'"
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.dd_coverage_events IS "
        "'Append-only log tracking the data-diff verified-through watermark'"
    )
    op.execute(f"COMMENT ON VIEW {SCHEMA}.dd_current_coverage IS 'Latest data-diff coverage watermark per check'")
    op.execute(
        f"COMMENT ON VIEW {SCHEMA}.dd_remediation_history IS "
        "'Failed data-diff runs paired with their remediation attempts'"
    )

    _grant_application_privileges()


def _grant_application_privileges():
    """Grant the application user access to objects owned by ``ddl_user``.

    Migrations run as ``ddl_user``, which owns everything it creates. Without
    these grants a separate application user cannot read its own backend. No-op
    when both roles are the same, or when the application user is unknown.
    """
    application_user = op.get_context().config.get_main_option("pipelinewise_application_user")
    if not application_user:
        return

    role = _quote_identifier(application_user)
    for table in (
        "dd_checks",
        "dd_preflights",
        "dd_runs",
        "dd_results",
        "dd_effective_attempts",
        "dd_coverage_state",
        "dd_coverage_events",
    ):
        op.execute(f"GRANT SELECT, INSERT, UPDATE ON {SCHEMA}.{table} TO {role}")
    for view in ("dd_current_coverage", "dd_remediation_history"):
        op.execute(f"GRANT SELECT ON {SCHEMA}.{view} TO {role}")

    # dd_coverage_events.event_sequence is a BIGSERIAL: inserting needs the
    # sequence, not just the table.
    op.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {SCHEMA} TO {role}")

    # alembic_version is read on every connection that checks the schema version.
    op.execute(f"GRANT SELECT ON {SCHEMA}.alembic_version TO {role}")
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {role}")


def _quote_identifier(name):
    """Quote a role name so mixed case and special characters survive."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def downgrade():
    op.execute(f"DROP VIEW IF EXISTS {SCHEMA}.dd_remediation_history")
    op.execute(f"DROP VIEW IF EXISTS {SCHEMA}.dd_current_coverage")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_coverage_events CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_coverage_state CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_effective_attempts CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_results CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_runs CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_preflights CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dd_checks CASCADE")
