"""create scheduler tables

Revision ID: fb58dbb37262
Revises:
Create Date: 2023-01-16 13:19:23.545027

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func


# revision identifiers, used by Alembic.
revision = 'fb58dbb37262'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # TAP_SCHEDULE table
    op.create_table(
        'tap_schedule',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('tap_id', sa.String(length=255), nullable=False),
        sa.Column('target_id', sa.String(length=255), nullable=False),
        sa.Column('tap_type', sa.String(length=255), nullable=False),
        sa.Column('ppw_host', sa.String(length=255)),
        sa.Column('is_enabled', sa.Boolean, nullable=False),
        sa.Column('state', sa.String(length=32), nullable=False),
        sa.Column('sync_period', sa.String(length=100)),
        sa.Column('first_run', sa.DateTime(), nullable=False),
        sa.Column('last_run', sa.DateTime(), nullable=False),
        sa.Column('adhoc_execute', sa.Boolean, nullable=False),
        sa.Column('adhoc_parameters', sa.String(length=255)),
        sa.Column('created_at', sa.DateTime(), default=func.now()),
        sa.Column('updated_at', sa.DateTime(), default=func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tap_id', 'target_id', name='tap_target_key'),
    )


def downgrade() -> None:
    op.drop_table('tap_schedule')
