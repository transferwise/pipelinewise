"""Structured, dialect-safe PartialSync boundary predicates."""

from dataclasses import dataclass, replace
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from typing import Any, Iterable, Tuple

from pipelinewise.fastsync.commons.snowflake_sql_utils import (
    quote_identifier,
    sql_string_literal,
)


class PartialSyncBoundaryError(ValueError):
    """A PartialSync boundary cannot be applied to the discovered source."""


def _json_safe_boundary(value: Any) -> Any:
    """Return stable JSON evidence for a resolved PartialSync boundary."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return {'type': 'decimal', 'value': str(value)}
    if isinstance(value, datetime):
        return {'type': 'datetime', 'value': value.isoformat()}
    if isinstance(value, date):
        return {'type': 'date', 'value': value.isoformat()}
    if isinstance(value, datetime_time):
        return {'type': 'time', 'value': value.isoformat()}
    raise PartialSyncBoundaryError(
        f'Unsupported PartialSync boundary evidence type: '
        f'{type(value).__name__}'
    )


def _restore_boundary(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    if set(value) != {'type', 'value'}:
        raise PartialSyncBoundaryError(
            'PartialSync recovery boundary value is invalid'
        )
    boundary_type = value.get('type')
    serialized = value.get('value')
    if not isinstance(serialized, str) or not serialized:
        raise PartialSyncBoundaryError(
            'PartialSync recovery boundary value is invalid'
        )
    constructors = {
        'date': date.fromisoformat,
        'datetime': datetime.fromisoformat,
        'decimal': Decimal,
        'time': datetime_time.fromisoformat,
    }
    if boundary_type not in constructors:
        raise PartialSyncBoundaryError(
            'PartialSync recovery boundary value is invalid'
        )
    try:
        return constructors[boundary_type](serialized)
    except (ValueError, ArithmeticError) as exc:
        raise PartialSyncBoundaryError(
            'PartialSync recovery boundary value is invalid'
        ) from exc


def _source_identifier(identifier: str, dialect: str) -> str:
    identifier = identifier.replace('%', '%%')
    if dialect == 'mysql':
        return '`' + identifier.replace('`', '``') + '`'
    if dialect == 'postgres':
        return quote_identifier(identifier)
    raise ValueError(f'Unsupported PartialSync source dialect: {dialect}')


@dataclass(frozen=True)
class SourceBoundarySql:
    """A source predicate template and its separately bound values."""

    statement: str
    parameters: Tuple[Any, ...]


@dataclass(frozen=True)
class PartialSyncBoundary:
    """Resolved PartialSync range, source column, and replacement intent."""

    column_name: str
    start_value: Any
    end_value: Any = None
    drop_target: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.column_name, str) or not self.column_name:
            raise PartialSyncBoundaryError(
                'PartialSync boundary column must be a non-empty string'
            )
        if self.start_value is None:
            raise PartialSyncBoundaryError(
                'PartialSync boundary start value is required'
            )
        if not isinstance(self.drop_target, bool):
            raise PartialSyncBoundaryError(
                'PartialSync drop-target boundary flag must be Boolean'
            )

    def resolved(self, source_columns: Iterable[str]) -> 'PartialSyncBoundary':
        """Resolve the configured column to its exact source-catalog spelling."""
        available = tuple(source_columns)
        if any(not isinstance(column, str) or not column for column in available):
            raise PartialSyncBoundaryError(
                'PartialSync source column discovery returned an invalid name'
            )
        if self.column_name in available:
            return self
        matches = [
            column
            for column in available
            if column.casefold() == self.column_name.casefold()
        ]
        if len(matches) == 1:
            return replace(self, column_name=matches[0])
        raise PartialSyncBoundaryError(
            f'PartialSync boundary column {self.column_name!r} was not found '
            'unambiguously in the source table'
        )

    def require_exact_column(
        self, source_columns: Iterable[str]
    ) -> 'PartialSyncBoundary':
        """Reject source-column drift after the boundary has been persisted."""
        resolved = self.resolved(source_columns)
        if resolved.column_name != self.column_name:
            raise PartialSyncBoundaryError(
                f'PartialSync boundary column changed from '
                f'{self.column_name!r} to {resolved.column_name!r}'
            )
        return self

    def source_sql(
        self,
        dialect: str,
        source_columns: Iterable[str],
    ) -> SourceBoundarySql:
        """Render only identifiers; return values separately for driver binding."""
        self.require_exact_column(source_columns)
        column = _source_identifier(self.column_name, dialect)
        parameters = []
        if self.start_value == 'NULL':
            statement = f' WHERE {column} >= NULL'
        else:
            statement = f' WHERE {column} >= %s'
            parameters.append(str(self.start_value))
        if self.end_value is not None:
            statement += f' AND {column} <= %s'
            parameters.append(str(self.end_value))
        return SourceBoundarySql(statement, tuple(parameters))

    def snowflake_where_clause(self) -> str:
        """Render the boundary for PipelineWise's uppercased Snowflake column."""
        column = quote_identifier(self.column_name.upper())
        if self.start_value == 'NULL':
            statement = f' WHERE {column} >= NULL'
        else:
            statement = (
                f' WHERE {column} >= '
                f'{sql_string_literal(str(self.start_value))}'
            )
        if self.end_value is not None:
            statement += (
                f' AND {column} <= '
                f'{sql_string_literal(str(self.end_value))}'
            )
        return statement

    def as_context(self):
        """Return stable, structured manifest evidence for this boundary."""
        return {
            'column_name': self.column_name,
            'start_value': _json_safe_boundary(self.start_value),
            'end_value': _json_safe_boundary(self.end_value),
            'end_is_unbounded': self.end_value is None,
            'drop_target': self.drop_target,
            'delete_mode': 'hard',
        }

    @classmethod
    def from_manifest_payload(cls, payload) -> 'PartialSyncBoundary':
        """Rebuild an executable boundary only from typed manifest fields."""
        return cls(
            column_name=payload.column_name,
            start_value=_restore_boundary(payload.start_value),
            end_value=_restore_boundary(payload.end_value),
            drop_target=payload.drop_target,
        )
