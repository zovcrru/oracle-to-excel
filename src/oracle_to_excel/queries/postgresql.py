# queries/postgresql.py
"""PostgreSQL-specific query handling."""

from collections.abc import Generator
from typing import Any

class PostgreSQLQueries:
    """PostgreSQL database query operations (psycopg3)."""

    @staticmethod
    def build_query(
        table: str,
        filters: dict[str, Any] | None,
    ) -> tuple[str, dict[str, Any]]:
        """Build PostgreSQL query with named parameters (%(param)s)."""
        query = f"SELECT * FROM {table}"
        params = {}

        if filters:
            conditions = []
            for key, value in filters.items():
                conditions.append(f"{key} = %({key})s")
                params[key] = value
            query += " WHERE " + " AND ".join(conditions)

        return query, params

    @staticmethod
    def execute_stream(
        connection: Any,
        query: str,
        params: dict[str, Any],
        fetch_size: int,
    ) -> Generator[list[tuple], None, None]:
        """Execute PostgreSQL query with server-side cursor."""
        # psycopg3 supports server-side cursors with name parameter
        with connection.cursor(name='server_cursor') as cursor:
            cursor.itersize = fetch_size
            cursor.execute(query, params)

            while chunk := cursor.fetchmany(fetch_size):
                yield chunk
