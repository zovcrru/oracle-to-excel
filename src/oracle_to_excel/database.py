"""
Модуль для работы с подключениями к базам данных.

Поддерживает Oracle, PostgreSQL и SQLite через унифицированный интерфейс.
Использует возможности Python 3.14 и централизованное логирование.
"""
from __future__ import annotations

import os
import platform
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager, suppress
from pathlib import Path as _Path
from typing import Literal, Protocol, cast
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

# Типы с запятой (Python 3.14)
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
    s = connection_string.lower()
    # Проверка по префиксу схемы
    if s.startswith(('oracle', 'oracle+cx_oracle', 'oracle+oracledb')):
        return 'oracle'
    if s.startswith(('postgresql', 'postgres', 'postgresql+psycopg', 'postgresql+psycopg3')):
        return 'postgresql'
    if s.startswith(('sqlite', 'sqlite3')) or s == ':memory:' or s.endswith(('.sqlite3', '.db')):
        return 'sqlite'

    # Проверка по порту (fallback для строк без явной схемы)
    if ':1521/' in s or ':1521@' in s:
        return 'oracle'
    if ':5432/' in s or ':5433/' in s or 'postgresql://' in s:
        return 'postgresql'

    raise DatabaseTypeDetectionError(f'Не удалось определить тип БД: {connection_string}')


@log_execution_time
def create_connection(
    connection_string: ConnectionString,
    db_type: DBType | None = None,
    *,
    read_only: bool = False,
    timeout: int = 30,
    lib_dir: str | None = None,
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
            oracle_lib_dir = lib_dir or r'd:\instantclient_12_1'
            return _create_oracle_connection(
                connection_string,
                read_only=read_only,
                thick_mode=True,
                lib_dir=oracle_lib_dir,  # ← ИСПОЛЬЗОВАТЬ
            )
        case 'postgresql' | 'postgres':
            return _create_postgresql_connection(
                connection_string,
                read_only=read_only,
                timeout=timeout,
            )
        case 'sqlite' | 'sqlite3':
            return _create_sqlite_connection(
                connection_string,
                timeout=timeout,
            )
        case _:
            raise ValueError(f'Неподдерживаемый тип БД: {db_type}')


# Multi-platform helpers for Oracle thick-mode initialization
def _normalize_lib_dir(lib_dir: _Path | str | None) -> str | None:
    """Нормализует путь к библиотеке Oracle instant client."""
    if isinstance(lib_dir, _Path):
        return str(lib_dir)
    if isinstance(lib_dir, str) or lib_dir is None:
        return lib_dir
    raise TypeError('lib_dir must be a Path, str or None')


def _autodetect_windows_instantclient() -> str | None:
    """Автоопределение пути к Oracle instant client на Windows."""
    cand = _Path(r'd:\instantclient_12_1')
    return str(cand) if cand.exists() else None


def _ensure_path_contains(lib_dir_str: str | None) -> None:
    """Добавляет путь к библиотеке в PATH если нужно."""
    if lib_dir_str and platform.system() == 'Windows':
        current = os.environ.get('PATH', '')
        if lib_dir_str not in current:
            os.environ['PATH'] = f'{lib_dir_str};{current}'


def _verify_oci_presence(lib_dir_str: str | None) -> None:
    """Проверяет наличие oci.dll в указанной директории."""
    if lib_dir_str:
        oci = _Path(lib_dir_str) / 'oci.dll'
        if not oci.exists():
            raise FileNotFoundError(f'oci.dll not found in {lib_dir_str}')


# Multi-platform setup
def init_oracle_thick_mode(lib_dir: _Path | str | None = None) -> bool:
    """
    Инициализирует Oracle thick client support (best-effort).

    Args:
        lib_dir: Путь к директории с Oracle instant client.

    Returns:
        True при успешной инициализации, raises on fatal errors.

    Raises:
        RuntimeError: При критических ошибках инициализации.
    """
    lib_dir_str = _normalize_lib_dir(lib_dir)

    if lib_dir_str is None and platform.system() == 'Windows':
        lib_dir_str = _autodetect_windows_instantclient()

    _ensure_path_contains(lib_dir_str)
    _verify_oci_presence(lib_dir_str)

    try:
        oracledb.init_oracle_client(
            lib_dir=r'D:\instantclient_12_1',
            config_dir=r'D:\instantclient_12_1',
        )
    except Exception as e:
        raise RuntimeError(f'Failed to init Oracle thick mode: {e}') from e
    return True


def _create_oracle_connection(
    connection_string: ConnectionString,
    *,
    read_only: bool,
    thick_mode: bool = True,
    lib_dir: _Path | str | None = None,
) -> DatabaseConnection:
    parsed = urlparse(connection_string)
    host = parsed.hostname
    if not host:
        raise ValueError('Отсутствует hostname в строке подключения Oracle')
    port = parsed.port or 1521
    dsn = oracledb.makedsn(host, port, service_name=parsed.path.lstrip('/'))
    # Включаем thick mode если требуется
    if thick_mode:
        # best-effort initialization; errors will propagate if critical
        _ = init_oracle_thick_mode(lib_dir=lib_dir)
    conn = oracledb.connect(
        user=parsed.username,
        password=parsed.password,
        dsn=dsn,
        config_dir=False,
        disable_oob=True,
    )
    if read_only:
        cursor = conn.cursor()
        cursor.execute('SET TRANSACTION READ ONLY')
        cursor.close()
    conn.autocommit = False
    return cast(DatabaseConnection, conn)


def _create_postgresql_connection(
    connection_string: ConnectionString,
    *,
    read_only: bool,
    timeout: int,
) -> DatabaseConnection:
    """Создает подключение к PostgreSQL БД."""
    options = f'-c default_transaction_read_only={"on" if read_only else "off"}'
    conn = psycopg.connect(
        connection_string,
        autocommit=False,
        connect_timeout=timeout,
        options=options,
    )
    return cast(DatabaseConnection, conn)


def _resolve_sqlite_path(conn_str: str) -> tuple[str, bool]:
    """Определяет путь к SQLite БД из connection string."""
    parsed = urlparse(conn_str)
    db_path_local = conn_str
    if parsed.scheme and parsed.scheme.startswith('sqlite'):
        db_path_local = parsed.path.lstrip('/') or parsed.netloc or db_path_local

    use_uri_local = db_path_local.startswith('file:') or '://' in conn_str

    if use_uri_local:
        return db_path_local, use_uri_local

    p = _Path(db_path_local)
    if p.is_absolute():
        return db_path_local, use_uri_local

    cand = _Path.cwd() / p
    if cand.exists():
        return str(cand), use_uri_local

    module_dir = _Path(__file__).resolve().parent
    cand2 = module_dir / p
    if cand2.exists():
        return str(cand2), use_uri_local

    parent = cand2.parent
    with suppress(Exception):
        parent.mkdir(parents=True, exist_ok=True)
    return str(cand2), use_uri_local


def _create_sqlite_connection(
    connection_string: ConnectionString,
    *,
    timeout: int,
) -> DatabaseConnection:
    """Создает подключение к SQLite БД."""
    db_path, use_uri = _resolve_sqlite_path(connection_string)

    if use_uri:
        if not db_path.startswith('file:'):
            db_path = 'file:' + db_path
        conn = sqlite3.connect(db_path, timeout=timeout, uri=True)
    else:
        conn = sqlite3.connect(db_path, timeout=timeout)

    return cast(DatabaseConnection, conn)


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
    lib_dir: str | None = None,
) -> Generator[DatabaseConnection]:
    """
    Context manager для работы с подключением к БД.

    Args:
        connection_string: Строка подключения к БД.
        db_type: Тип БД (опционально).
        read_only: Создать read-only подключение.
        timeout: Таймаут подключения.

    Yields:
        Объект подключения к БД.

    Examples:
        >>> with get_connection('sqlite:///test.db') as conn:
        ...     cursor = conn.cursor()
        ...     cursor.execute('SELECT 1')
    """
    logger = get_logger('database')

    connection = None
    try:
        connection = create_connection(
            connection_string,
            db_type,
            read_only=read_only,
            timeout=timeout,
            lib_dir=lib_dir,
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
    cursor.execute("SELECT USER FROM dual")
    result = cursor.fetchone()
    if result:
        info['user'] = result[0]
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
    cursor.execute('SELECT CURRENT_USER')
    if result:
        info['user'] = result[0]

    return info


def sqlite_info(
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
    cursor.execute('SELECT sqlite_version()')
    result = cursor.fetchone()
    if result:
        info['version'] = result[0]
    cursor.execute('SELECT name FROM pragma_database_list WHERE name="main"')
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
        Словарь с информацией о БД (version, database, db_type).
    """
    logger = get_logger('database')
    info: dict[str, str | int] = {'db_type': db_type}
    info_funcs = {
        'oracle': oracle_info,
        'postgresql': postgres_info,
        'postgres': postgres_info,
        'sqlite': sqlite_info,
        'sqlite3': sqlite_info,
    }

    cursor = connection.cursor()
    try:
        if db_type in info_funcs:
            print("db_type=", db_type)
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
    # Для sqlite допустимы схемы 'sqlite' и отсутствие hostname (локальный файл)
    if not parsed.scheme:
        # Отсутствие схемы допускается для локальных файлов (например 'lice.sqlite3')
        if not parsed.path:
            return False, 'Отсутствует путь к файлу для sqlite или схема подключения'
        return True, ''

    scheme = parsed.scheme.lower()
    if scheme.startswith('sqlite'):
        # sqlite://... может не иметь hostname, проверяем наличие пути/нетлока
        if parsed.path or parsed.netloc:
            return True, ''
        return False, 'Отсутствует путь к sqlite базе'

    # для остальных схем требуется hostname
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
    except ValueError as e:
        return False, str(e)
    else:
        return True, db_type


# def validate_connection_string(
#     connection_string: ConnectionString,
# ) -> tuple[bool, str]:
#     """
#     Валидирует connection string.

#     Args:
#         connection_string: Строка подключения для проверки.

#     Returns:
#         Кортеж (валидность, сообщение об ошибке).
#     """
#     logger = get_logger('database')

#     is_valid, error = check_non_empty_string(connection_string)
#     if not is_valid:
#         return is_valid, error

#     is_valid, parsed_or_err = try_parse_url(connection_string)
#     if not is_valid:
#         return False, str(parsed_or_err)

#     is_valid, error = check_url_parts(parsed_or_err)
#     if not is_valid:
#         return is_valid, error

#     is_valid, db_type_or_err = try_detect_db_type(connection_string)
#     if not is_valid:
#         return False, str(db_type_or_err)

#     logger.debug('Connection string валиден для %s', db_type_or_err)
#     return True, ''
