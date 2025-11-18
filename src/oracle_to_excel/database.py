from __future__ import annotations

import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from typing import Literal, Protocol
from urllib.parse import urlparse

try:
    import psycopg
except ImportError as err:
    raise RuntimeError('Модуль psycopg3 не установлен.') from err
try:
    import oracledb
except ImportError as err:
    raise RuntimeError('Модуль oracledb не установлен.') from err

from oracle_to_excel.logger import get_logger, log_execution_time

# Типы с запятой
type DBType = Literal['oracle', 'postgresql', 'sqlite']
type ConnectionString = str


class DBCursor(Protocol):
    """
    Protocol defining the minimal interface for a database cursor object.

    Classes implementing this protocol should provide methods for executing
    SQL queries, fetching results, and closing the cursor. This protocol
    enables static type-checking and interface validation for code working
    with different database backends.

    Example of use:
        cursor.execute("SELECT * FROM table")
        row = cursor.fetchone()
        cursor.close()
    """

    def execute(self, query: str, /) -> None:
        """
        Execute a database query.

        This method executes the provided SQL query on the database.
        The forward slash in the parameter list indicates that the query
        parameter must be provided as a positional argument.

        Args:
            query: The SQL query string to execute.

        Returns:
            None: This method doesn't return a value.
        """
        ...

    def fetchone(self) -> object:
        """
        Fetch the next row of a query result set.

        This method retrieves the next row from the result set of a previously executed query.
        If all rows have been fetched, it returns None.

        Returns:
            object: The next row as a single sequence, or None when no more data is available.
        """
        ...

    def close(self) -> None:
        """
        Close the cursor and release associated resources.

        This method closes the cursor, releasing any resources that were allocated during
        its creation and use. After calling this method, the cursor should not be used
        for any further operations.

        Returns:
            None: This method doesn't return a value.
        """
        ...


class DatabaseConnection(Protocol):
    """Protocol defining the interface for database connections."""

    def cursor(self) -> DBCursor:
        """Return a new cursor object using the connection."""
        ...

    def commit(self) -> None:
        """Commit the current transaction."""
        ...

    def rollback(self) -> None:
        """Roll back the current transaction."""
        ...

    def close(self) -> None:
        """Close the database connection."""
        ...


class DatabaseTypeDetectionError(ValueError):
    """
    Exception raised when the database type cannot be determined from the connection string.

    This error indicates that the provided connection string does not match a supported
    database type (for example, Oracle or PostgreSQL). It is intended to signal
    configuration issues or invalid input for database detection logic.

    Args:
        message (str): Detailed error message explaining the failure.

    Example:
        >>> raise DatabaseTypeDetectionError(
        ...     'Unable to determine database type from the given connection string.',
        ... )

    Typically raised by:
        - detect_db_type(connection_string)
    """


def detect_db_type(connection_string: ConnectionString) -> DBType:
    """
    Determine the database type from the connection string.

    Args:
        connection_string: Database connection string.

    Returns:
        Database type as a string literal ("oracle" or "postgresql").

    Raises:
        DatabaseTypeDetectionError: If the database type cannot be determined.
    """
    parsed = urlparse(connection_string)
    scheme = parsed.scheme.lower()
    match scheme:
        case 'oracle' | 'oracle+cx_oracle' | 'oracle+oracledb':
            return 'oracle'
        case 'postgresql' | 'postgres' | 'postgresql+psycopg' | 'postgresql+psycopg3':
            return 'postgresql'
        case 'sqlite' | 'sqlite3':
            return 'sqlite'
        case _:
            if ':1521/' in connection_string or ':1521@' in connection_string:
                return 'oracle'
            if (
                ':5432/' in connection_string
                or 'postgresql://' in connection_string.lower()
                or ':5433/' in connection_string
            ):
                return 'postgresql'
            raise DatabaseTypeDetectionError(
                f'Не удалось определить тип БД: {connection_string}',
            )


@log_execution_time
def create_connection(
    connection_string: ConnectionString,
    db_type: DBType | None = None,
    *,
    read_only: bool = False,
    timeout: int = 30,
) -> DatabaseConnection:
    """
    Creates a database connection based on the provided connection string and database type.

    This function establishes a connection to either Oracle or PostgreSQL databases.
    If the database type is not explicitly provided, it will be automatically detected
    from the connection string.

    Args:
        connection_string: A string containing the database connection information.
        db_type: The type of database to connect to ('oracle', 'postgresql', or 'sqlite').
                 If None, the type will be detected from the connection string.
        read_only: When True, creates a read-only connection to the database.
                   Default is False.
        timeout: The number of seconds to wait for a connection before timing out.
                 Default is 30 seconds.

    Returns:
        A database connection object that implements the DatabaseConnection protocol.

    Raises:
        ValueError: If the database type is not supported.
        RuntimeError: If required database modules are not installed.
        DatabaseTypeDetectionError: If the database type cannot be determined from the
        connection string.
    """
    logger = get_logger('database')
    logger.debug('Creating connection to %s database', db_type or 'unknown')

    db_type = db_type or detect_db_type(connection_string)
    match db_type:
        case 'oracle':
            return _create_oracle_connection(
                connection_string,
                read_only=read_only,
                timeout=timeout,
            )
        case 'postgresql':
            return _create_postgresql_connection(
                connection_string,
                read_only=read_only,
                timeout=timeout,
            )
        case 'sqlite':
            return _create_sqlite_connection(
                connection_string,
                read_only=read_only,
                timeout=timeout,
            )

        case _:
            logger.error('Unsupported database type: %s', db_type)
            raise ValueError('Неподдерживаемый тип БД: %s', db_type)


def _create_oracle_connection(
    connection_string: ConnectionString,
    *,
    read_only: bool,
    timeout: int,
) -> DatabaseConnection:
    parsed = urlparse(connection_string)
    print(connection_string, parsed)
    print(parsed.hostname, parsed.port)
    exit(1)
    dsn = oracledb.makedsn(
        parsed.hostname, parsed.port or 1521, service_name=parsed.path.lstrip('/')
    )
    conn = oracledb.connect(
        user=parsed.username,
        password=parsed.password,
        dsn=dsn,
        config_dir=None,
        disable_oob=True,
        timeout=timeout,
    )
    if read_only:
        cursor = conn.cursor()
        cursor.execute('SET TRANSACTION READ ONLY')
        cursor.close()
    conn.autocommit = False
    return conn


def _create_postgresql_connection(
    connection_string: ConnectionString,
    *,
    read_only: bool,
    timeout: int,
) -> DatabaseConnection:
    options = f'-c default_transaction_read_only={"on" if read_only else "off"}'
    conn = psycopg.connect(
        connection_string,
        autocommit=False,
        connect_timeout=timeout,
        options=options,
    )
    return conn


def _create_sqlite_connection(
    connection_string: ConnectionString,
    *,
    read_only: bool,
    timeout: int,
) -> DatabaseConnection:
    conn = sqlite3.connect(
        connection_string,
        timeout=timeout,
        autocommit=False,
    )
    return conn


def close_connection(
    connection: DatabaseConnection | None,
) -> None:
    """
    Safely closes a database connection if it exists.

    This function checks if the provided connection object is not None
    before attempting to close it, preventing errors when trying to close
    a non-existent connection.

    Args:
        connection: The database connection to close. Can be None,
                   in which case no action is taken.

    Returns:
        None: This function doesn't return a value.
    """
    if connection is not None:
        connection.close()


@contextmanager
def get_connection(
    connection_string: ConnectionString,
    db_type: DBType | None = None,
    *,
    read_only: bool = False,
    timeout: int = 30,
) -> Generator[
    DatabaseConnection,
    None,
    None,
]:
    """
    Context manager для работы с подключением к БД.

    Args:
        connection_string: Строка подключения к БД.
        db_type: Тип БД (опционально).
        read_only: Создать read-only подключение.
        timeout: Таймаут подключения.

    Yields:
        Объект подключения к БД.
    """
    logger = get_logger('database')

    connection = None
    try:
        connection = create_connection(
            connection_string, db_type, read_only=read_only, timeout=timeout
        )
        logger.debug('Context manager: подключение создано')
        yield connection
    except Exception as e:
        logger.warning('Ошибка в context manager: %s', e)
        if connection:
            try:
                connection.rollback()
                logger.debug('Выполнен rollback транзакции')
            except Exception:  # noqa: S110
                pass
        raise
    finally:
        close_connection(connection)
        logger.debug('Context manager: подключение закрыто')


def oracle_info(
    cursor,
) -> dict[
    str,
    str | int,
]:
    """
    Retrieves information about an Oracle database connection.

    This function executes SQL queries against an Oracle database to gather
    version and database name information.

    Args:
        cursor: An Oracle database cursor object that implements the DBCursor protocol.
               Used to execute queries against the database.

    Returns:
        A dictionary containing information about the Oracle database:
        - 'version': The Oracle database version string
        - 'database': The name of the connected Oracle database
    """
    info = {}
    cursor.execute("SELECT * FROM v$version WHERE banner LIKE 'Oracle%'")
    result = cursor.fetchone()
    if result:
        info['version'] = result[0]
    cursor.execute("SELECT SYS_CONTEXT('USERENV', 'DB_NAME') FROM DUAL")
    result = cursor.fetchone()
    if result:
        info['database'] = result[0]
    return info


def postgres_info(
    cursor,
) -> dict[
    str,
    str | int,
]:
    """
    Retrieves information about a PostgreSQL database connection.

    This function executes SQL queries against a PostgreSQL database to gather
    version and database name information.

    Args:
        cursor: A PostgreSQL database cursor object that implements the DBCursor protocol.
               Used to execute queries against the database.

    Returns:
        A dictionary containing information about the PostgreSQL database:
        - 'version': The PostgreSQL database version string
        - 'database': The name of the connected PostgreSQL database
    """
    info = {}
    cursor.execute('SELECT version()')
    result = cursor.fetchone()
    if result:
        info['version'] = result[0]
    cursor.execute('SELECT current_database()')
    result = cursor.fetchone()
    if result:
        info['database'] = result[0]
    return info


def get_db_info(
    connection: DatabaseConnection,
    db_type: DBType,
) -> dict[
    str,
    str | int,
]:
    """
    Получает информацию о БД.

    Args:
        connection: Объект подключения к БД.
        db_type: Тип БД.

    Returns:
        Словарь с информацией о БД.
    """
    logger = get_logger('database')
    info: dict[str, str | int] = {'db_type': db_type}

    info_funcs = {
        'oracle': oracle_info,
        'postgresql': postgres_info,
    }

    cursor = connection.cursor()
    try:
        if db_type in info_funcs:
            info.update(info_funcs[db_type](cursor))
        else:
            logger.warning('Unsupported database type: %s', db_type)
        logger.debug('Получена информация о БД: %s', info)
    except Exception as e:
        logger.warning('Не удалось получить информацию о БД: %s', e)
    finally:
        cursor.close()
    return info


def check_non_empty_string(
    connection_string: str,
) -> tuple[
    bool,
    str,
]:
    """
    Проверяет, что connection string не пустой и является строкой.

    Args:
        connection_string: Строка подключения для проверки.

    Returns:
        Кортеж (валидность, сообщение об ошибке).
    """
    if not connection_string or not isinstance(connection_string, str):
        return False, 'Connection string должен быть непустой строкой'
    return True, ''


def check_url_parts(
    parsed,
) -> tuple[
    bool,
    str,
]:
    """
    Проверяет корректность схемы и hostname в разобранном URL.

    Args:
        parsed: Результат разбора URL (urlparse).

    Returns:
        Кортеж (валидность, сообщение об ошибке).
    """
    if not parsed.scheme:
        return False, 'Отсутствует схема подключения (oracle:// или postgresql://)'
    if not parsed.hostname:
        return False, 'Отсутствует hostname'
    return True, ''


def try_parse_url(connection_string: str) -> tuple[bool, object | str]:
    """
    Попытка разбора URL из connection string.

    Returns:
        (True, parsed_url) если успешно,
        (False, сообщение об ошибке) иначе.
    """
    try:
        return True, urlparse(connection_string)
    except Exception as e:
        return False, f'Ошибка при валидации: {e}'


def try_detect_db_type(connection_string: str) -> tuple[bool, str]:
    """
    Попытка определения типа БД из connection string.

    Returns:
        (True, db_type) если успешно,
        (False, сообщение об ошибке) иначе.
    """
    try:
        db_type = detect_db_type(connection_string)
        return True, db_type
    except ValueError as e:
        return False, str(e)


def validate_connection_string(
    connection_string: ConnectionString,
) -> tuple[bool, str]:
    """
    Валидирует connection string.

    Args:
        connection_string: Строка подключения для проверки.

    Returns:
        Кортеж (валидность, сообщение об ошибке).
    """
    logger = get_logger('database')

    is_valid, error = check_non_empty_string(connection_string)
    if not is_valid:
        return is_valid, error

    is_valid, parsed_or_err = try_parse_url(connection_string)
    if not is_valid:
        return False, parsed_or_err

    is_valid, error = check_url_parts(parsed_or_err)
    if not is_valid:
        return is_valid, error

    is_valid, db_type_or_err = try_detect_db_type(connection_string)
    if not is_valid:
        return False, db_type_or_err

    logger.debug('Connection string валиден для %s', db_type_or_err)
    return True, ''
