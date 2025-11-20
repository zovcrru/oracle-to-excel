"""
Модуль для работы с подключениями к базам данных.

Поддерживает Oracle, PostgreSQL и SQLite через унифицированный интерфейс.
Использует возможности Python 3.14 и централизованное логирование.
"""

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
type DBType = Literal[
    'oracle',
    'postgresql',
    'sqlite',
]
type ConnectionString = str


class DBCursor(Protocol):
    """
    Protocol defining the minimal interface for a database cursor object.

    Classes implementing this protocol should provide methods for executing
    SQL queries, fetching results, and closing the cursor. This protocol
    enables static type-checking and interface validation for code working
    with different database backends.
    """

    def execute(self, query: str, /) -> None:
        """Execute a database query."""
        ...

    def fetchone(self) -> object:
        """Fetch the next row of a query result set."""
        ...

    def close(self) -> None:
        """Close the cursor and release associated resources."""
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
    database type (Oracle, PostgreSQL, or SQLite).
    """


# Global flag to track initialization
_thick_mode_initialized = False


def detect_and_validate_db_type(
    connection_string: ConnectionString,
) -> tuple[DBType | Literal[''], bool, str]:
    """
    Определяет тип БД и валидирует connection string (объединенная функция).

    Эта функция заменяет собой detect_db_type() + validate_connection_string(),
    устраняя дублирование логики и упрощая код.

    Args:
        connection_string: Строка подключения к БД.

    Returns:
        Кортеж (db_type, is_valid, error_message):
        - db_type: 'oracle' | 'postgresql' | 'sqlite' или '' если невалидно
        - is_valid: True если строка валидна
        - error_message: Описание ошибки или '' если валидно

    Examples:
        >>> detect_and_validate_db_type('oracle://user:pwd@host:1521/service')
        ('oracle', True, '')

        >>> detect_and_validate_db_type('')
        ('', False, 'Connection string должен быть непустой строкой')

        >>> detect_and_validate_db_type('invalid://xyz')
        ('', False, 'Не удалось определить тип БД')
    """
    logger = get_logger('database')

    # 1. Проверка на пустоту
    if not connection_string or not isinstance(connection_string, str):
        return ('', False, 'Connection string должен быть непустой строкой')

    # 2. Парсинг URL
    try:
        parsed = urlparse(connection_string)
    except Exception as e:
        return ('', False, f'Ошибка парсинга URL: {e}')

    # 3. Определение типа БД (встроенная логика)
    s = connection_string.lower()
    db_type: DBType | Literal[''] = ''

    if s.startswith(('oracle', 'oracle+cx_oracle', 'oracle+oracledb')):
        db_type = 'oracle'
    elif s.startswith(('postgresql', 'postgres', 'postgresql+psycopg', 'postgresql+psycopg3')):
        db_type = 'postgresql'
    elif s.startswith(('sqlite', 'sqlite3')) or s == ':memory:' or s.endswith(('.sqlite3', '.db')):
        db_type = 'sqlite'
    elif ':1521/' in s or ':1521@' in s:
        db_type = 'oracle'
    elif ':5432/' in s or ':5433/' in s or 'postgresql://' in s:
        db_type = 'postgresql'

    # Если тип не определен
    if not db_type:
        return ('', False, f'Не удалось определить тип БД: {connection_string}')

    # 4. Валидация структуры URL в зависимости от типа
    if db_type == 'sqlite':
        # Для sqlite достаточно наличия пути или :memory:
        if not (parsed.path or parsed.netloc or s == ':memory:'):
            return ('', False, 'Отсутствует путь к sqlite базе')
    # Для Oracle и PostgreSQL нужен hostname
    elif parsed.hostname:
        return ('', False, f'Отсутствует hostname для {db_type}')

    logger.debug('Connection string валиден для %s', db_type)
    return (db_type, True, '')


def detect_db_type(connection_string: ConnectionString) -> DBType:
    """
    Определяет тип БД из connection string (legacy wrapper).

    Args:
        connection_string: Строка подключения к БД.

    Returns:
        Тип БД: 'oracle' | 'postgresql' | 'sqlite'

    Raises:
        DatabaseTypeDetectionError: Если тип не определен или строка невалидна.

    Note:
        Эта функция является legacy wrapper для detect_and_validate_db_type().
        Для нового кода рекомендуется использовать detect_and_validate_db_type().
    """
    db_type, is_valid, error = detect_and_validate_db_type(connection_string)
    if not is_valid:
        raise DatabaseTypeDetectionError(error)
    return db_type  # type: ignore[return-value]


def validate_connection_string(connection_string: ConnectionString) -> tuple[bool, str]:
    """
    Валидирует connection string (legacy wrapper).

    Args:
        connection_string: Строка подключения для проверки.

    Returns:
        Кортеж (is_valid, error_message).

    Note:
        Эта функция является legacy wrapper для detect_and_validate_db_type().
        Для нового кода рекомендуется использовать detect_and_validate_db_type().
    """
    _, is_valid, error = detect_and_validate_db_type(connection_string)
    return (is_valid, error)


@log_execution_time
def create_connection(
    connection_string: ConnectionString,
    db_type: DBType | None = None,
    *,
    read_only: bool = False,
    timeout: int = 30,
) -> DatabaseConnection:
    """
    Создает подключение к БД на основе connection string.

    Args:
        connection_string: Строка подключения к БД.
        db_type: Тип БД ('oracle', 'postgresql', 'sqlite').
                 Если None, определяется автоматически.
        read_only: Создать read-only подключение.
        timeout: Таймаут подключения в секундах.

    Returns:
        Объект подключения к БД.

    Raises:
        ValueError: Если тип БД не поддерживается или connection string невалиден.
        DatabaseTypeDetectionError: Если не удалось определить тип БД.
    """
    logger = get_logger('database')

    # Используем новую объединенную функцию
    if db_type is None:
        detected_type, is_valid, error = detect_and_validate_db_type(connection_string)
        if not is_valid:
            raise ValueError(f'Невалидный connection string: {error}')
        db_type = detected_type  # type: ignore[assignment]

    logger.debug('Creating connection to %s database', db_type)

    match db_type:
        case 'oracle':
            return _create_oracle_connection(
                connection_string,
                read_only=read_only,
                thick_mode=True,
                lib_dir=r'd:\instantclient_12_1',
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


def init_oracle_thick_mode(lib_dir: _Path | str | None = None) -> bool:
    """
    Инициализирует Oracle thick client support.

    Args:
        lib_dir: Путь к директории с Oracle instant client.

    Returns:
        True при успешной инициализации.

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
    """Создает подключение к Oracle БД."""
    parsed = urlparse(connection_string)
    host = parsed.hostname
    if not host:
        raise ValueError('Отсутствует hostname в строке подключения Oracle')

    port = parsed.port or 1521
    dsn = oracledb.makedsn(host, port, service_name=parsed.path.lstrip('/'))

    # Включаем thick mode если требуется
    if thick_mode:
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


def close_connection(connection: DatabaseConnection | None) -> None:
    """
    Безопасно закрывает подключение к БД.

    Args:
        connection: Объект подключения или None.
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


def oracle_info(cursor) -> dict[str, str | int]:
    """Получает информацию об Oracle БД."""
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


def postgres_info(cursor) -> dict[str, str | int]:
    """Получает информацию о PostgreSQL БД."""
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


def sqlite_info(cursor) -> dict[str, str | int]:
    """Получает информацию о SQLite БД."""
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


def get_db_info(connection: DatabaseConnection, db_type: DBType) -> dict[str, str | int]:
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
            info.update(info_funcs[db_type](cursor))
        else:
            logger.warning('Unsupported database type: %s', db_type)
        logger.debug('Получена информация о БД: %s', info)
    except Exception as e:
        logger.warning('Не удалось получить информацию о БД: %s', e)
    finally:
        cursor.close()

    return info
