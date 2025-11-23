# queries/__init__.py
"""Database query modules for Oracle, PostgreSQL, and SQLite."""

from queries.base import DatabaseConnection, build_query, execute_query_stream
from queries.oracle import OracleQueries
from queries.postgresql import PostgreSQLQueries
from queries.sqlite import SQLiteQueries

__all__ = [
    'DatabaseConnection',
    'build_query',
    'execute_query_stream',
    'OracleQueries',
    'PostgreSQLQueries',
    'SQLiteQueries',
]
