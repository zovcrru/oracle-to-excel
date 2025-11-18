from urllib.parse import urlparse

from oracle_to_excel.logger import get_logger


def import_driver(module_name: str, logger_name: str) -> object:
    """
    Dynamically import a database driver and log any errors with full traceback.

    Args:
        module_name: Name of the module to import.
        logger_name: Name to use for logging.

    Returns:
        Imported module object.

    Raises:
        RuntimeError: If the module is not installed.
    """
    logger = get_logger(logger_name)
    try:
        module = __import__(module_name)
        logger.debug('Модуль %s успешно импортирован', module_name)
        return module
    except ImportError as err:
        logger.exception('Модуль %s не установлен.', module_name)  # Сохраняется traceback
        raise RuntimeError(f'Модуль {module_name} не установлен.') from err


def load_db_drivers() -> list[object | None]:
    """
    Try to import all supported database drivers and return them.

    Returns:
        Tuple (oracledb, psycopg, sqlite3). None for missing modules.

    All errors are logged.
    """
    logger = get_logger('database')
    drivers: list[object | None] = []
    for name in ('oracledb', 'psycopg', 'sqlite3'):
        try:
            driver = __import__(name)
            logger.debug('Модуль %s успешно импортирован', name)
        except ImportError as err:
            logger.warning('Модуль %s не установлен: %s', name, err)
            driver = None
        drivers.append(driver)
    return drivers


# Пример использования:
_ORACLEDB, _PSYCOPG, _SQLITE = load_db_drivers()


def detect_db_type(connection_string: str) -> str:
    """Determine DB type by connection string."""
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
            if ':5432/' in connection_string or 'postgresql://' in connection_string.lower():
                return 'postgresql'
            if 'sqlite' in connection_string.lower():
                return 'sqlite'
            raise ValueError(f'Не удалось определить тип БД: {connection_string}')


def create_connection(
    connection_string: str,
    *,
    read_only: bool = False,
    timeout: int = 30,
) -> object:
    """Create DB connection for Oracle, PostgreSQL, or SQLite."""
    logger = get_logger('database')
    db_type = detect_db_type(connection_string)
    logger.info('Создание подключения к БД: %s', db_type)
    match db_type:
        case 'oracle':
            return _create_oracle(
                connection_string,
                read_only,
                timeout,
                logger,
            )
        case 'postgresql':
            return _create_postgres(
                connection_string,
                read_only,
                timeout,
                logger,
            )
        case 'sqlite':
            return _create_sqlite(
                connection_string,
                logger,
            )
        case _:
            logger.error('Неподдерживаемый тип БД: %s', db_type)
            raise ValueError(f'Неподдерживаемый тип БД: {db_type}')


def _create_oracle(
    connection_string: str,
    read_only: bool,  # рекомендуется использовать только как именованный
    timeout: int,
    logger,
) -> object:
    """
    Create a connection to Oracle database.

    Args:
        connection_string: Oracle connection string.
        read_only (bool, keyword-only): If True, set transaction to READ ONLY. Pass as named argument.
        timeout (int, keyword-only): Connection timeout in seconds. Pass as named argument.
        logger: Logging instance.

    Returns:
        Oracle connection object.

    Raises:
        RuntimeError: If driver is missing.
        ValueError: If string is incomplete.
    """
    if _ORACLEDB is None:
        logger.error('Модуль oracledb не установлен.')
        raise RuntimeError('Модуль oracledb не установлен.')
    parsed = urlparse(connection_string)
    if not parsed.hostname:
        raise ValueError('В connection string отсутствует hostname.')
    dsn = _ORACLEDB.makedsn(
        parsed.hostname,
        parsed.port or 1521,
        service_name=parsed.path.lstrip('/'),
    )
    conn = _ORACLEDB.connect(
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


def _create_postgres(
    connection_string: str,
    read_only: bool,  # рекомендуется использовать только как именованный
    timeout: int,
    logger,
) -> object:
    """
    Create a connection to PostgreSQL database.

    Args:
        connection_string: PostgreSQL connection string.
        read_only (bool, keyword-only): If True, set transaction to READ ONLY.
        timeout (int, keyword-only): Connection timeout in seconds.
        logger: Logging instance.

    Returns:
        PostgreSQL connection object.

    Raises:
        RuntimeError: If driver is missing.
    """
    if _PSYCOPG is None:
        logger.error('Модуль psycopg не установлен.')
        raise RuntimeError('Модуль psycopg не установлен.')
    conn = _PSYCOPG.connect(
        connection_string,
        autocommit=False,
        connect_timeout=timeout,
        options=(f'-c default_transaction_read_only={"on" if read_only else "off"}'),
    )
    return conn


def _create_sqlite(
    connection_string: str,
    logger,
) -> object:
    """
    Create a connection to SQLite database.

    Args:
        connection_string: SQLite connection string.
        logger: Logging instance.

    Returns:
        SQLite connection object.

    Raises:
        RuntimeError: If driver is missing.
    """
    if _SQLITE is None:
        logger.error('Модуль sqlite3 не установлен.')
        raise RuntimeError('Модуль sqlite3 не установлен.')
    parsed = urlparse(connection_string)
    database_path = parsed.path or ':memory:'
    conn = _SQLITE.connect(database_path)
    return conn


# Дальнейшая структура: для каждого типа можно добавить отдельные функции, если нужны расширения!
