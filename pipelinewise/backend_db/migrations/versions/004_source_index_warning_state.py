"""Remember delivered source-index warnings across checks and config revisions.

Revision ID: 004
Revises: 003
Create Date: 2026-10-09
"""

from alembic import op

revision = '004'
down_revision = '003'
branch_labels = None
depends_on = None

SCHEMA = 'public'


def upgrade():
    op.execute(f'''
        CREATE TABLE {SCHEMA}.dd_index_warning_state (
            warning_id UUID PRIMARY KEY,
            check_id UUID NOT NULL
                REFERENCES {SCHEMA}.dd_check_definitions(check_id) ON DELETE RESTRICT,
            sent_at TIMESTAMPTZ NOT NULL
        )
    ''')
    op.execute(
        f'COMMENT ON TABLE {SCHEMA}.dd_index_warning_state IS '
        "'Delivered source-index warnings, retained across config revisions and scheduled runs'"
    )
    op.execute(
        f'COMMENT ON COLUMN {SCHEMA}.dd_index_warning_state.warning_id IS '
        "'Deterministic UUID of tap, source type, database, schema, table, and timestamp column'"
    )
    op.execute(
        f'COMMENT ON COLUMN {SCHEMA}.dd_index_warning_state.check_id IS '
        "'Historical check definition that first delivered this warning'"
    )
    application_user = op.get_context().config.get_main_option('pipelinewise_application_user')
    if application_user:
        role = '"' + application_user.replace('"', '""') + '"'
        op.execute(f'GRANT SELECT, INSERT, UPDATE ON {SCHEMA}.dd_index_warning_state TO {role}')


def downgrade():
    op.execute(f'DROP TABLE {SCHEMA}.dd_index_warning_state')
