# queries/base.py
"""Base classes and interfaces for database queries."""

from collections.abc import Generator
from enum import Enum
from typing import Any, Protocol


class DBType(Enum):
    """Supported database types."""

    ORACLE = 'oracle'
    POSTGRESQL = 'postgresql'
    SQLITE = 'sqlite'


class DatabaseConnection(Protocol):
    """Protocol for database connections."""

    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


def build_query(
    table: str,
    filters: dict[str, Any] | None,
    db_type: DBType,
) -> tuple[str, dict[str, Any] | list[Any]]:
    """
    Build SQL query with appropriate parameter style for database type.

    Args:
        table: Table name
        filters: WHERE clause conditions
        db_type: Database type (Oracle/PostgreSQL/SQLite)

    Returns:
        Tuple of (query_string, parameters)
    """
    match db_type:
        case DBType.ORACLE:
            from queries.oracle import OracleQueries

            return OracleQueries.build_query(table, filters)
        case DBType.POSTGRESQL:
            from queries.postgresql import PostgreSQLQueries

            return PostgreSQLQueries.build_query(table, filters)
        case DBType.SQLITE:
            from queries.sqlite import SQLiteQueries

            return SQLiteQueries.build_query(table, filters)


def execute_query_stream(
    connection: DatabaseConnection,
    query: str,
    params: dict[str, Any] | list[Any],
    fetch_size: int,
    db_type: DBType,
) -> Generator[list[tuple], None, None]:
    """
    Execute query and stream results in chunks.

    Args:
        connection: Database connection
        query: SQL query string
        params: Query parameters
        fetch_size: Number of rows to fetch per iteration
        db_type: Database type

    Yields:
        Chunks of rows
    """
    match db_type:
        case DBType.ORACLE:
            from queries.oracle import OracleQueries

            yield from OracleQueries.execute_stream(connection, query, params, fetch_size)
        case DBType.POSTGRESQL:
            from queries.postgresql import PostgreSQLQueries

            yield from PostgreSQLQueries.execute_stream(connection, query, params, fetch_size)
        case DBType.SQLITE:
            from queries.sqlite import SQLiteQueries

            yield from SQLiteQueries.execute_stream(connection, query, params, fetch_size)
