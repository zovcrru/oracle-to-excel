# transformers.py
"""Universal data type transformers for multiple databases."""

from datetime import datetime, date, time
from decimal import Decimal
from typing import Any
from queries.base import DBType

def convert_db_types(value: Any, db_type: DBType) -> str | int | float | None:
    """
    Convert database-specific types to Excel-compatible types.

    Args:
        value: Database value
        db_type: Database type (Oracle/PostgreSQL/SQLite)

    Returns:
        Converted value suitable for Excel
    """
    if value is None:
        return None

    # Common conversions for all databases
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, time):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')

    # Database-specific conversions
    match db_type:
        case DBType.ORACLE:
            return _convert_oracle_specific(value)
        case DBType.POSTGRESQL:
            return _convert_postgresql_specific(value)
        case DBType.SQLITE:
            return _convert_sqlite_specific(value)

    return value

def _convert_oracle_specific(value: Any) -> Any:
    """Oracle-specific type conversions."""
    # Oracle CLOB/BLOB handling
    if hasattr(value, 'read'):  # LOB objects
        return value.read()
    return value

def _convert_postgresql_specific(value: Any) -> Any:
    """PostgreSQL-specific type conversions (базовые типы)."""
    # Пока только базовые типы, специфичные (ARRAY, JSON) не обрабатываем
    return value

def _convert_sqlite_specific(value: Any) -> Any:
    """SQLite-specific type conversions."""
    # SQLite имеет ограниченный набор типов, дополнительная обработка не нужна
    return value

def transform_row(
    row: tuple,
    column_names: list[str],
    db_type: DBType,
) -> dict[str, Any]:
    """
    Transform database row to dictionary with type conversion.

    Args:
        row: Database row tuple
        column_names: List of column names
        db_type: Database type

    Returns:
        Dictionary with column names as keys and converted values
    """
    return {
        col: convert_db_types(val, db_type)
        for col, val in zip(column_names, row, strict=True)
    }
