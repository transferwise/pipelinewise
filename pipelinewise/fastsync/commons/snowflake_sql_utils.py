"""Dependency-free Snowflake SQL quoting helpers."""


def quote_identifier(identifier: str) -> str:
    """Quote one exact Snowflake identifier."""
    return '"' + identifier.replace('"', '""') + '"'


def sql_string_literal(value: str) -> str:
    """Quote a Snowflake string literal."""
    return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"
