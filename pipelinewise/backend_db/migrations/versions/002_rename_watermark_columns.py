"""rename watermark state and event columns

Revision ID: 002
Revises: 001
Create Date: 2026-09-07
"""

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None

SCHEMA = "public"
STATE_TABLE = "dd_watermark_state"
EVENTS_TABLE = "dd_watermark_events"

STATE_COLUMN_RENAMES = (
    ("coverage_start", "verified_start"),
    ("verified_through", "verified_end"),
    ("max_observed_end", "furthest_observed_end"),
    ("coverage_status", "verified_status"),
    ("evaluated_run_id", "last_evaluated_run_id"),
)

EVENT_COLUMN_RENAMES = (
    ("coverage_start", "verified_start"),
    ("previous_verified_through", "previous_verified_end"),
    ("verified_through", "verified_end"),
    ("max_observed_end", "furthest_observed_end"),
    ("coverage_status", "verified_status"),
)

STATE_CONSTRAINT_RENAMES = (
    (
        "dd_watermark_state_coverage_status_check",
        "dd_watermark_state_verified_status_check",
    ),
    (
        "dd_watermark_state_evaluated_run_id_fkey",
        "dd_watermark_state_last_evaluated_run_id_fkey",
    ),
)

EVENT_CONSTRAINT_RENAMES = (
    (
        "dd_watermark_events_coverage_status_check",
        "dd_watermark_events_verified_status_check",
    ),
)


def _rename_columns(table, renames):
    for old_name, new_name in renames:
        op.execute(
            f"ALTER TABLE {SCHEMA}.{table} "
            f"RENAME COLUMN {old_name} TO {new_name}"
        )


def _rename_constraints(table, renames):
    for old_name, new_name in renames:
        op.execute(
            f"ALTER TABLE {SCHEMA}.{table} "
            f"RENAME CONSTRAINT {old_name} TO {new_name}"
        )


def upgrade():
    _rename_columns(STATE_TABLE, STATE_COLUMN_RENAMES)
    _rename_columns(EVENTS_TABLE, EVENT_COLUMN_RENAMES)
    _rename_constraints(STATE_TABLE, STATE_CONSTRAINT_RENAMES)
    _rename_constraints(EVENTS_TABLE, EVENT_CONSTRAINT_RENAMES)
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.{STATE_TABLE} IS "
        "'Mutable current verified interval and furthest observed window "
        "for each check definition'"
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.{EVENTS_TABLE} IS "
        "'Append-only history of verified intervals and furthest observed "
        "window transitions'"
    )


def downgrade():
    _rename_constraints(
        EVENTS_TABLE,
        (
            (new_name, old_name)
            for old_name, new_name in reversed(EVENT_CONSTRAINT_RENAMES)
        ),
    )
    _rename_constraints(
        STATE_TABLE,
        (
            (new_name, old_name)
            for old_name, new_name in reversed(STATE_CONSTRAINT_RENAMES)
        ),
    )
    _rename_columns(
        EVENTS_TABLE,
        (
            (new_name, old_name)
            for old_name, new_name in reversed(EVENT_COLUMN_RENAMES)
        ),
    )
    _rename_columns(
        STATE_TABLE,
        (
            (new_name, old_name)
            for old_name, new_name in reversed(STATE_COLUMN_RENAMES)
        ),
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.{STATE_TABLE} IS "
        "'Mutable current verified-through watermark for each check definition'"
    )
    op.execute(
        f"COMMENT ON TABLE {SCHEMA}.{EVENTS_TABLE} IS "
        "'Append-only history of verified-through watermark transitions'"
    )
