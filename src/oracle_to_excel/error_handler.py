# error_handler.py
"""Universal error handling for multiple databases."""

from typing import Any
from queries.base import DBType

class DatabaseError(Exception):
    """Base database error."""
    def __init__(self, message: str, original_error: Exception | None = None):
        super().__init__(message)
        self.original_error = original_error

class ConnectionError(DatabaseError):
    """Connection-related errors."""
    pass

class QueryError(DatabaseError):
    """Query execution errors."""
    pass

class DataError(DatabaseError):
    """Data-related errors."""
    pass

def handle_db_error(error: Exception, db_type: DBType) -> DatabaseError:
    """
    Convert database-specific errors to unified error types.

    Args:
        error: Original database exception
        db_type: Database type

    Returns:
        Unified DatabaseError subclass
    """
    match db_type:
        case DBType.ORACLE:
            return _handle_oracle_error(error)
        case DBType.POSTGRESQL:
            return _handle_postgresql_error(error)
        case DBType.SQLITE:
            return _handle_sqlite_error(error)

def _handle_oracle_error(error: Exception) -> DatabaseError:
    """Handle Oracle-specific errors."""
    import oracledb

    if isinstance(error, oracledb.DatabaseError):
        error_obj, = error.args
        code = error_obj.code
        message = error_obj.message

        # Connection errors
        if code in (1017, 12154, 12505, 12514):  # Authentication/TNS errors
            return ConnectionError(f"Oracle connection failed: {message}", error)

        # Query errors
        if code in (942, 904, 936):  # Table/column not found, missing expression
            return QueryError(f"Oracle query error: {message}", error)

        # Data errors
        if code in (1722, 1438):  # Invalid number, value too large
            return DataError(f"Oracle data error: {message}", error)

    return DatabaseError(f"Oracle error: {error}", error)

def _handle_postgresql_error(error: Exception) -> DatabaseError:
    """Handle PostgreSQL-specific errors (psycopg3)."""
    try:
        import psycopg

        if isinstance(error, psycopg.OperationalError):
            return ConnectionError(f"PostgreSQL connection failed: {error}", error)

        if isinstance(error, psycopg.ProgrammingError):
            return QueryError(f"PostgreSQL query error: {error}", error)

        if isinstance(error, psycopg.DataError):
            return DataError(f"PostgreSQL data error: {error}", error)

        if isinstance(error, psycopg.Error):
            # Check sqlstate code
            if hasattr(error, 'sqlstate'):
                code = error.sqlstate
                # 28xxx - Authentication errors
                if code and code.startswith('28'):
                    return ConnectionError(f"PostgreSQL auth error [{code}]: {error}", error)
                # 42xxx - Syntax/schema errors
                if code and code.startswith('42'):
                    return QueryError(f"PostgreSQL query error [{code}]: {error}", error)

    except ImportError:
        pass

    return DatabaseError(f"PostgreSQL error: {error}", error)

def _handle_sqlite_error(error: Exception) -> DatabaseError:
    """Handle SQLite-specific errors."""
    import sqlite3

    if isinstance(error, sqlite3.OperationalError):
        msg = str(error).lower()
        if 'unable to open database' in msg or 'locked' in msg:
            return ConnectionError(f"SQLite connection failed: {error}", error)
        if 'no such table' in msg or 'no such column' in msg:
            return QueryError(f"SQLite query error: {error}", error)

    if isinstance(error, sqlite3.IntegrityError):
        return DataError(f"SQLite data integrity error: {error}", error)

    if isinstance(error, sqlite3.DataError):
        return DataError(f"SQLite data error: {error}", error)

    return DatabaseError(f"SQLite error: {error}", error)
