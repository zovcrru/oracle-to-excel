# queries/oracle.py
"""Oracle-specific query handling."""

from collections.abc import Generator
from typing import Any

class OracleQueries:
    """Oracle database query operations."""

    @staticmethod
    def build_query(
        table: str,
        filters: dict[str, Any] | None,
    ) -> tuple[str, dict[str, Any]]:
        """Build Oracle query with named parameters (:param)."""
        query = f"SELECT * FROM {table}"
        params = {}

        if filters:
            conditions = []
            for key, value in filters.items():
                conditions.append(f"{key} = :{key}")
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
        """Execute Oracle query with streaming results."""
        cursor = connection.cursor()
        cursor.arraysize = fetch_size

        try:
            cursor.execute(query, params)
            while chunk := cursor.fetchmany(fetch_size):
                yield chunk
        finally:
            cursor.close()
