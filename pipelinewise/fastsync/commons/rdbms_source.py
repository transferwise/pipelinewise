"""Source-specific behavior used by shared RDBMS-to-Snowflake runners."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple


ExportInspection = Callable[[], Tuple[List[str], int]]
TypeValidator = Callable[[Dict[str, Any]], None]


class RdbmsSnowflakeSource(ABC):
    """Explicit source contract for the shared Snowflake lifecycles."""

    route_name: str

    @classmethod
    def mysql(cls, factory, type_mapper):
        """Build the MySQL/MariaDB source contract."""
        return MySqlSnowflakeSource(factory, type_mapper)

    @classmethod
    def postgres(cls, factory, type_mapper):
        """Build the PostgreSQL source contract."""
        return PostgresSnowflakeSource(factory, type_mapper)

    @abstractmethod
    def create(self, args, iceberg_requested: bool):
        """Create and configure a source connector."""

    @abstractmethod
    def source_engine(self, args) -> str:
        """Return the source-engine component of recovery identity."""

    @abstractmethod
    def bookmark_kwargs(self, args) -> Dict[str, str]:
        """Return source-specific bookmark lookup arguments."""

    @abstractmethod
    def open(self, source) -> None:
        """Open the source connection or connections."""

    @abstractmethod
    def complete_full_export(
        self,
        source,
        table: str,
        iceberg_requested: bool,
        validate_types: TypeValidator,
        inspect_export: ExportInspection,
    ) -> Tuple[Dict[str, Any], List[str], int]:
        """Map the exported schema, inspect files, and close in source order."""

    @abstractmethod
    def close_partial(self, source) -> None:
        """Close a PartialSync source using its route-specific behavior."""

    @abstractmethod
    def close_finally(self, source) -> None:
        """Silently retry source cleanup after a FullSync outcome."""


@dataclass(frozen=True)
class MySqlSnowflakeSource(RdbmsSnowflakeSource):
    """MySQL/MariaDB implementation of the shared source contract."""

    factory: Callable[..., Any]
    type_mapper: Callable[..., str]
    route_name = 'mysql_to_snowflake'

    def create(self, args, iceberg_requested: bool):
        source = self.factory(args.tap, self.type_mapper)
        if iceberg_requested:
            source.set_mariadb_json_aliases_enabled(True)
        return source

    def source_engine(self, args) -> str:
        return args.tap.get('engine', 'mysql')

    def bookmark_kwargs(self, args) -> Dict[str, str]:
        del args
        return {}

    def open(self, source) -> None:
        source.open_connections()

    def complete_full_export(
        self,
        source,
        table: str,
        iceberg_requested: bool,
        validate_types: TypeValidator,
        inspect_export: ExportInspection,
    ) -> Tuple[Dict[str, Any], List[str], int]:
        del iceberg_requested
        snowflake_types = source.map_column_types_to_target(table)
        validate_types(snowflake_types)
        source.close_connections()
        file_parts, size_bytes = inspect_export()
        return snowflake_types, file_parts, size_bytes

    def close_partial(self, source) -> None:
        source.close_connections(silent=True)

    def close_finally(self, source) -> None:
        source.close_connections(silent=True)


@dataclass(frozen=True)
class PostgresSnowflakeSource(RdbmsSnowflakeSource):
    """PostgreSQL implementation of the shared source contract."""

    factory: Callable[..., Any]
    type_mapper: Callable[..., str]
    route_name = 'postgres_to_snowflake'

    def create(self, args, iceberg_requested: bool):
        source = self.factory(args.tap, self.type_mapper)
        source.hstore_as_json = iceberg_requested
        return source

    def source_engine(self, args) -> str:
        del args
        return 'postgres'

    def bookmark_kwargs(self, args) -> Dict[str, str]:
        return {'dbname': args.tap.get('dbname')}

    def open(self, source) -> None:
        source.open_connection()

    def complete_full_export(
        self,
        source,
        table: str,
        iceberg_requested: bool,
        validate_types: TypeValidator,
        inspect_export: ExportInspection,
    ) -> Tuple[Dict[str, Any], List[str], int]:
        snowflake_types = None
        if iceberg_requested:
            snowflake_types = source.map_column_types_to_target(table)
            validate_types(snowflake_types)

        file_parts, size_bytes = inspect_export()
        if snowflake_types is None:
            snowflake_types = source.map_column_types_to_target(table)
        source.close_connection()
        return snowflake_types, file_parts, size_bytes

    def close_partial(self, source) -> None:
        source.close_connection()

    def close_finally(self, source) -> None:
        source.close_connection(silent=True)
