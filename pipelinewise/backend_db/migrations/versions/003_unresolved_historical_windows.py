"""Represent unresolved historical windows without fabricated timestamps.

Revision ID: 003
Revises: 002
Create Date: 2026-09-25
"""

from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None

SCHEMA = "public"
WINDOW_TABLES = ("dd_run_attempts", "dd_run_slot_state")
WATERMARK_TABLES = ("dd_watermark_state", "dd_watermark_events")


def _set_attempt_statuses(statuses):
    op.execute(
        f"ALTER TABLE {SCHEMA}.dd_run_attempts "
        "DROP CONSTRAINT dd_run_attempts_status_check"
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.dd_run_attempts "
        "ADD CONSTRAINT dd_run_attempts_status_check "
        f"CHECK (status IN ({statuses}))"
    )


def upgrade():
    _set_attempt_statuses("'RUNNING', 'PASS', 'FAIL', 'ERROR', 'DEFERRED'")
    for table in WINDOW_TABLES:
        unresolved_statuses = "'RUNNING', 'ERROR', 'DEFERRED'" if table == "dd_run_attempts" else "'ERROR'"
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ALTER COLUMN window_start DROP NOT NULL")
        op.execute(
            f"ALTER TABLE {SCHEMA}.{table} "
            f"ADD CONSTRAINT ck_{table}_resolved_window "
            f"CHECK (window_start IS NOT NULL OR status IN ({unresolved_statuses}))"
        )
        op.execute(
            f"COMMENT ON COLUMN {SCHEMA}.{table}.window_start IS "
            "'Inclusive comparison start; NULL while the historical start is unresolved'"
        )

    for table in WATERMARK_TABLES:
        op.execute(
            f"ALTER TABLE {SCHEMA}.{table} "
            "ALTER COLUMN verified_start DROP NOT NULL, "
            "ALTER COLUMN verified_end DROP NOT NULL"
        )
        op.execute(
            f"ALTER TABLE {SCHEMA}.{table} "
            f"ADD CONSTRAINT ck_{table}_verified_bounds "
            "CHECK ((verified_start IS NOT NULL AND verified_end IS NOT NULL) OR "
            "(verified_start IS NULL AND verified_end IS NULL AND "
            "verified_status = 'BLOCKED' AND blocking_run_id IS NOT NULL))"
        )
        for column, meaning in (
            ("verified_start", "Start of the verified interval"),
            ("verified_end", "End of the contiguous verified interval"),
        ):
            op.execute(
                f"COMMENT ON COLUMN {SCHEMA}.{table}.{column} IS "
                f"'{meaning}; NULL when an unresolved historical run blocks verification'"
            )
    op.execute(
        f"COMMENT ON COLUMN {SCHEMA}.dd_run_attempts.status IS "
        "'RUNNING or terminal PASS, FAIL, ERROR, or DEFERRED; "
        "DEFERRED means neither side has settled historical timestamps'"
    )


def downgrade():
    # Failed historical evidence remains append-only after remediation. Revision
    # 002 cannot represent it, so reject rollback before changing the schema.
    op.execute(f"""
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM {SCHEMA}.dd_run_attempts
                 WHERE window_start IS NULL OR status = 'DEFERRED'
            ) OR EXISTS (
                SELECT 1 FROM {SCHEMA}.dd_run_slot_state WHERE window_start IS NULL
            ) OR EXISTS (
                SELECT 1 FROM {SCHEMA}.dd_watermark_state
                 WHERE verified_start IS NULL OR verified_end IS NULL
            ) OR EXISTS (
                SELECT 1 FROM {SCHEMA}.dd_watermark_events
                 WHERE verified_start IS NULL OR verified_end IS NULL
            ) THEN
                RAISE EXCEPTION 'Cannot downgrade data-diff to revision 002: unresolved historical '
                    'bounds or DEFERRED attempts are not representable. Retain revision 003 or '
                    'restore the backend from a pre-upgrade backup; do not discard audit history.';
            END IF;
        END $$
    """)
    for table in WATERMARK_TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT ck_{table}_verified_bounds")
        op.execute(
            f"ALTER TABLE {SCHEMA}.{table} "
            "ALTER COLUMN verified_start SET NOT NULL, "
            "ALTER COLUMN verified_end SET NOT NULL"
        )
        for column in ("verified_start", "verified_end"):
            op.execute(f"COMMENT ON COLUMN {SCHEMA}.{table}.{column} IS NULL")

    for table in WINDOW_TABLES:
        op.execute(f"ALTER TABLE {SCHEMA}.{table} DROP CONSTRAINT ck_{table}_resolved_window")
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ALTER COLUMN window_start SET NOT NULL")
        op.execute(f"COMMENT ON COLUMN {SCHEMA}.{table}.window_start IS NULL")
    _set_attempt_statuses("'RUNNING', 'PASS', 'FAIL', 'ERROR'")
    op.execute(f"COMMENT ON COLUMN {SCHEMA}.dd_run_attempts.status IS NULL")
