"""
Модуль управления подключениями к базам данных.

Поддерживает Oracle и PostgreSQL через connection strings.
Использует функциональный подход без пула подключений.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Literal, Protocol
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import Generator

from oracle_to_excel.logger import get_logger, log_execution_time

# Type aliases
type DBType = Literal['oracle', 'postgresql']
type ConnectionString = str


class DatabaseConnection(Protocol):
    """Протокол для подключений к БД."""

    def cursor(self): ...  # noqa: D102
    def commit(self) -> None: ...  # noqa: D102
    def rollback(self) -> None: ...  # noqa: D102
    def close(self) -> None: ...  # noqa: D102


def _import_db_libraries() -> tuple[object | None, object | None]:
    """
    Динамически импортирует библиотеки для БД.

    Returns:
        Кортеж (oracledb module, psycopg3 module).
    """
    logger = get_logger('database')

    oracledb_module = None
    psycopg_module = None

    try:
        import oracledb

        oracledb_module = oracledb
        logger.debug('Модуль oracledb успешно загружен')
    except ImportError:
        logger.warning('Модуль oracledb не установлен (требуется для Oracle)')

    try:
        import psycopg

        psycopg_module = psycopg
        logger.debug('Модуль psycopg3 успешно загружен')
    except ImportError:
        logger.warning('Модуль psycopg3 не установлен (требуется для PostgreSQL)')

    return oracledb_module, psycopg_module


_ORACLEDB, _PSYCOPG = _import_db_libraries()


def detect_db_type(connection_string: ConnectionString) -> DBType:
    """
    Определяет тип БД по connection string.

    Args:
        connection_string: Строка подключения к БД.

    Returns:
        Тип базы данных ("oracle" или "postgresql").

    Raises:
        ValueError: Если не удалось определить тип БД.
    """
    logger = get_logger('database')

    try:
        parsed = urlparse(connection_string)
        scheme = parsed.scheme.lower()

        match scheme:
            case 'oracle' | 'oracle+cx_oracle' | 'oracle+oracledb':
                logger.debug('Определен тип БД: Oracle (scheme: %s)', scheme)
                return 'oracle'
            case 'postgresql' | 'postgres' | 'postgresql+psycopg' | 'postgresql+psycopg3':
                logger.debug('Определен тип БД: PostgreSQL (scheme: %s)', scheme)
                return 'postgresql'
            case _:
                if ':1521/' in connection_string or ':1521@' in connection_string:
                    logger.debug('Определен тип БД: Oracle (по порту 1521)')
                    return 'oracle'
                if ':5432/' in connection_string or 'postgresql://' in connection_string.lower():
                    logger.debug('Определен тип БД: PostgreSQL (по порту 5432)')
                    return 'postgresql'

                error_msg = (
                    f'Не удалось определить тип БД из connection string: {connection_string}'
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

    except Exception as e:
        error_msg = f'Ошибка парсинга connection string: {e}'
        logger.exception(error_msg)
        raise ValueError(error_msg) from e


@log_execution_time
def create_connection(
    connection_string: ConnectionString,
    db_type: DBType | None = None,
    *,
    read_only: bool = False,
    timeout: int = 30,
) -> DatabaseConnection:
    """
    Создает подключение к базе данных.

    Args:
        connection_string: Строка подключения к БД.
        db_type: Тип БД (опционально, определяется автоматически).
        read_only: Создать read-only подключение.
        timeout: Таймаут подключения в секундах.

    Returns:
        Объект подключения к БД.

    Raises:
        ValueError: Если тип БД не поддерживается.
        RuntimeError: Если не установлена требуемая библиотека.
        ConnectionError: Если не удалось подключиться.
    """
    logger = get_logger('database')

    if db_type is None:
        db_type = detect_db_type(connection_string)

    logger.info('Создание подключения к БД: %s', db_type)

    match db_type:
        case 'oracle':
            return _create_oracle_connection(
                connection_string, read_only=read_only, timeout=timeout
            )
        case 'postgresql':
            return _create_postgresql_connection(
                connection_string, read_only=read_only, timeout=timeout
            )
        case _:
            error_msg = f'Неподдерживаемый тип БД: {db_type}'
            logger.error(error_msg)
            raise ValueError(error_msg)


def _create_oracle_connection(
    connection_string: ConnectionString,
    *,
    read_only: bool,
    timeout: int,  # noqa: ARG001
) -> DatabaseConnection:
    """Создает подключение к Oracle."""
    logger = get_logger('database.oracle')

    if _ORACLEDB is None:
        error_msg = 'Модуль oracledb не установлен. Установите: pip install oracledb'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    try:
        parsed = urlparse(connection_string)

        user = parsed.username
        password = parsed.password
        host = parsed.hostname
        port = parsed.port or 1521
        service_name = parsed.path.lstrip('/')

        dsn = _ORACLEDB.makedsn(host, port, service_name=service_name)

        logger.debug('Подключение к Oracle: %s@%s:%s/%s', user, host, port, service_name)

        connection = _ORACLEDB.connect(
            user=user, password=password, dsn=dsn, config_dir=None, disable_oob=True
        )

        if read_only:
            cursor = connection.cursor()
            cursor.execute('SET TRANSACTION READ ONLY')
            cursor.close()
            logger.debug('Установлен режим READ ONLY')

        connection.autocommit = False

        logger.info('✓ Подключение к Oracle установлено')
        return connection

    except Exception as e:
        error_msg = f'Ошибка подключения к Oracle: {e}'
        logger.exception(error_msg)
        raise ConnectionError(error_msg) from e


def _create_postgresql_connection(
    connection_string: ConnectionString,
    *,
    read_only: bool,
    timeout: int,
) -> DatabaseConnection:
    """Создает подключение к PostgreSQL."""
    logger = get_logger('database.postgresql')

    if _PSYCOPG is None:
        error_msg = 'Модуль psycopg3 не установлен. Установите: pip install psycopg[binary]'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    try:
        logger.debug('Подключение к PostgreSQL')

        connection = _PSYCOPG.connect(
            connection_string,
            autocommit=False,
            connect_timeout=timeout,
            options=(f'-c default_transaction_read_only={"on" if read_only else "off"}'),
        )

        if read_only:
            logger.debug('Установлен режим READ ONLY')

        logger.info('✓ Подключение к PostgreSQL установлено')
        return connection

    except Exception as e:
        error_msg = f'Ошибка подключения к PostgreSQL: {e}'
        logger.exception(error_msg)
        raise ConnectionError(error_msg) from e


def close_connection(connection: DatabaseConnection | None) -> None:
    """Корректно закрывает подключение к БД."""
    logger = get_logger('database')

    if connection is None:
        logger.debug('Подключение уже закрыто или None')
        return

    try:
        connection.close()
        logger.info('✓ Подключение к БД закрыто')
    except Exception as e:
        logger.warning('Ошибка при закрытии подключения: %s', e)


@log_execution_time
def test_connection(
    connection_string: ConnectionString,
    db_type: DBType | None = None,
) -> bool:
    """
    Тестирует подключение к БД.

    Args:
        connection_string: Строка подключения к БД.
        db_type: Тип БД (опционально, определяется автоматически).

    Returns:
        True если подключение успешно, False иначе.
    """
    logger = get_logger('database')

    if db_type is None:
        db_type = detect_db_type(connection_string)

    logger.info('Тестирование подключения к %s...', db_type)

    connection = None
    try:
        connection = create_connection(connection_string, db_type, read_only=True, timeout=10)

        match db_type:
            case 'oracle':
                test_query = 'SELECT 1 FROM DUAL'
            case 'postgresql':
                test_query = 'SELECT 1'
            case _:
                test_query = 'SELECT 1'

        cursor = connection.cursor()
        cursor.execute(test_query)
        result = cursor.fetchone()
        cursor.close()

        if result:
            logger.info('✓ Тестовое подключение к %s успешно', db_type)
            return True

        logger.error('✗ Тестовый запрос не вернул результат')
        return False

    except Exception as e:
        logger.error('✗ Ошибка при тестировании подключения: %s', e, exc_info=True)
        return False
    finally:
        close_connection(connection)


@contextmanager
def get_connection(
    connection_string: ConnectionString,
    db_type: DBType | None = None,
    *,
    read_only: bool = False,
    timeout: int = 30,
) -> Generator[DatabaseConnection, None, None]:
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
        logger.error('Ошибка в context manager: %s', e, exc_info=True)
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


def get_db_info(connection: DatabaseConnection, db_type: DBType) -> dict[str, str | int]:
    """
    Получает информацию о БД.

    Args:
        connection: Объект подключения к БД.
        db_type: Тип БД.

    Returns:
        Словарь с информацией о БД.
    """
    logger = get_logger('database')

    cursor = connection.cursor()
    info: dict[str, str | int] = {'db_type': db_type}

    try:
        match db_type:
            case 'oracle':
                cursor.execute("SELECT * FROM v$version WHERE banner LIKE 'Oracle%'")
                result = cursor.fetchone()
                if result:
                    info['version'] = result[0]

                cursor.execute("SELECT SYS_CONTEXT('USERENV', 'DB_NAME') FROM DUAL")
                result = cursor.fetchone()
                if result:
                    info['database'] = result[0]

            case 'postgresql':
                cursor.execute('SELECT version()')
                result = cursor.fetchone()
                if result:
                    info['version'] = result[0]

                cursor.execute('SELECT current_database()')
                result = cursor.fetchone()
                if result:
                    info['database'] = result[0]

        cursor.close()
        logger.debug('Получена информация о БД: %s', info)

    except Exception as e:
        logger.warning('Не удалось получить информацию о БД: %s', e)

    return info


def validate_connection_string(connection_string: ConnectionString) -> tuple[bool, str]:
    """
    Валидирует connection string.

    Args:
        connection_string: Строка подключения для проверки.

    Returns:
        Кортеж (валидность, сообщение об ошибке).
    """
    logger = get_logger('database')

    if not connection_string or not isinstance(connection_string, str):
        return False, 'Connection string должен быть непустой строкой'

    try:
        parsed = urlparse(connection_string)

        if not parsed.scheme:
            return (
                False,
                'Отсутствует схема подключения (oracle:// или postgresql://)',
            )

        if not parsed.hostname:
            return False, 'Отсутствует hostname'

        try:
            db_type = detect_db_type(connection_string)
            logger.debug('Connection string валиден для %s', db_type)
            return True, ''
        except ValueError as e:
            return False, str(e)

    except Exception as e:
        return False, f'Ошибка при валидации: {e}'


def _test_module() -> None:
    """Тестирует модуль database."""
    from oracle_to_excel.logger import setup_logging

    logger = setup_logging('DEBUG', console_output=True)
    logger.info('Тестирование модуля database.py...')
    logger.info('=' * 50)

    logger.info('Тест 1: Определение типа БД')
    test_strings = [
        'oracle://user:pass@localhost:1521/ORCL',
        'postgresql://user:pass@localhost:5432/testdb',
        'postgres://user:pass@localhost/db',
    ]

    for conn_str in test_strings:
        try:
            db_type = detect_db_type(conn_str)
            logger.info('  ✓ %s... -> %s', conn_str[:30], db_type)
        except Exception as e:
            logger.error('  ✗ Ошибка: %s', e)

    logger.info('Тест 2: Валидация connection strings')
    valid_strings = [
        'postgresql://user:pass@localhost/db',
        'oracle://user:pass@host:1521/service',
    ]
    invalid_strings = ['', 'invalid_string', 'http://not-a-db']

    for conn_str in valid_strings + invalid_strings:
        valid, error = validate_connection_string(conn_str)
        status = '✓' if valid else '✗'
        logger.info('  %s %s... - %s', status, conn_str[:40], error if error else 'OK')

    logger.info('Тест 3: Context manager')
    logger.info('  Context manager готов к использованию с реальным подключением')

    logger.info('' + '=' * 50)
    logger.info('✓ Все тесты модуля database.py завершены!')


if __name__ == '__main__':
    import sys

    match sys.argv:
        case [_, '--test']:
            _test_module()
        case _:
            print('Использование:')
            print('  python database.py --test  # Запустить тесты')
