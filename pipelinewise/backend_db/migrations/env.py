"""Alembic environment for PipelineWise backend migrations.

Connection is configured via sqlalchemy.url set by BackendDatabase.migrate().
"""

from alembic import context
from sqlalchemy import engine_from_config, pool


def run_migrations_online():
    """Run migrations using the connection URL set by BackendDatabase."""
    connectable = engine_from_config(
        context.config.get_section(context.config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=None,
            compare_type=True,
            version_table_schema='public',
        )
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
