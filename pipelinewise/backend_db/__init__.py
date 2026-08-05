"""Shared backend database infrastructure with Alembic migrations."""

from .database import BackendDatabase, BackendDatabaseConfigError

__all__ = ["BackendDatabase", "BackendDatabaseConfigError"]
