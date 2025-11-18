"""
Модуль конфигурации для Oracle Excel Exporter.

Загружает и валидирует параметры из .env файла,
используя возможности Python 3.14 и централизованное логирование.
Поддерживает Oracle и PostgreSQL через connection strings.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import NotRequired, TypedDict, cast
from urllib.parse import urlparse

from oracle_to_excel.logger import get_logger

try:
    from dotenv import load_dotenv
except ImportError:
    print('Ошибка: требуется установить python-dotenv')
    print('Выполните: pip install python-dotenv')
    sys.exit(1)


class ConfigDict(TypedDict):
    """Структура конфигурации приложения."""

    DB_TYPE: str
    DB_CONNECT_URI: str
    LOG_LEVEL: NotRequired[str]
    OUTPUT_DIR: NotRequired[str]
    FETCH_ARRAY_SIZE: NotRequired[int]
    CHUNK_SIZE: NotRequired[int]
    QUERY_TIMEOUT: NotRequired[int]
    MAX_COLUMN_WIDTH: NotRequired[int]
    COLUMN_WIDTH_SAMPLE_SIZE: NotRequired[int]


REQUIRED_CONFIG: frozenset[str] = frozenset({'DB_TYPE', 'DB_CONNECT_URI'})

DEFAULT_CONFIG: Mapping[str, int | str] = {
    'LOG_LEVEL': 'INFO',
    'OUTPUT_DIR': './exports',
    'FETCH_ARRAY_SIZE': 1000,
    'CHUNK_SIZE': 5000,
    'QUERY_TIMEOUT': 300,
    'MAX_COLUMN_WIDTH': 50,
    'COLUMN_WIDTH_SAMPLE_SIZE': 1000,
}

VALID_LOG_LEVELS: frozenset[str] = frozenset({'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'})

VALID_DB_TYPES: frozenset[str] = frozenset({'oracle', 'postgresql', 'sqlite'})


def load_config(env_file: str = '.env', *, use_logging: bool = True) -> ConfigDict:
    """
    Загружает конфигурацию из .env файла.

    Args:
        env_file: Путь к файлу с переменными окружения.
        use_logging: Использовать ли систему логирования.

    Returns:
        Словарь с конфигурацией приложения.

    Raises:
        FileNotFoundError: Если .env файл не найден.
        ValueError: Если отсутствуют обязательные параметры.
    """
    logger = get_logger('config') if use_logging else None
    env_path = Path(env_file)

    if not env_path.exists():
        error_msg = (
            f'Файл конфигурации не найден: {env_path.absolute()}'
            'Создайте .env файл на основе .env.example'
        )
        if logger:
            logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    load_dotenv(env_path)
    if logger:
        logger.info('Конфигурация загружена из: %s', env_path.absolute())

    config = _load_required_params(logger)
    _load_optional_params(config, logger)
    _mask_sensitive_data(config, logger)

    if logger:
        logger.info('Конфигурация успешно загружена (%d параметров)', len(config))

    return cast(ConfigDict, config)


def _load_required_params(
    logger: logging.Logger | None,
) -> dict[str, str | int | bool]:
    """Загружает обязательные параметры из окружения."""
    config: dict[str, str | int | bool] = {}
    missing_params = []

    for param in REQUIRED_CONFIG:
        value = os.getenv(param)
        if value:
            config[param] = value
        else:
            missing_params.append(param)

    if missing_params:
        error_msg = (
            f'Отсутствуют обязательные параметры в .env файле:'
            f'{", ".join(missing_params)}'
            'Убедитесь, что все обязательные параметры заданы.'
        )
        if logger:
            logger.error(error_msg)
        raise ValueError(error_msg)

    return config


def _load_optional_params(
    config: dict[str, str | int | bool],
    logger: logging.Logger | None,
) -> None:
    """Загружает опциональные параметры с значениями по умолчанию."""
    for param, default_value in DEFAULT_CONFIG.items():
        env_value = os.getenv(param)

        if isinstance(default_value, int):
            config[param] = _parse_int_param(param, env_value, default_value, logger)
        else:
            config[param] = env_value if env_value else default_value


def _parse_int_param(
    param_name: str, value: str | None, default: int, logger: logging.Logger | None = None
) -> int:
    """Парсит целочисленный параметр из строки."""
    if not value:
        return default

    try:
        parsed = int(value)
    except ValueError:
        _log_parse_warning(param_name, value, default, logger)
        return default

    if parsed <= 0:
        _log_parse_warning(param_name, value, default, logger)
        return default

    return parsed


def _log_parse_warning(
    param_name: str, value: str | None, default: int, logger: logging.Logger | None
) -> None:
    """Логирует предупреждение о некорректном значении параметра."""
    error_msg = (
        f"Некорректное значение параметра {param_name}: '{value}'. "
        f'Ожидается положительное целое число. '
        f'Используется значение по умолчанию: {default}'
    )
    if logger:
        logger.warning(error_msg)


def _mask_sensitive_data(
    config: dict[str, str | int | bool],
    logger: logging.Logger | None = None,
) -> None:
    """Маскирует чувствительные данные в конфигурации."""
    sensitive_keys = {'DB_CONNECT_URI', 'PASSWORD', 'SECRET', 'TOKEN'}
    masked_count = 0

    for key in list(config.keys()):
        if not any(sensitive in key.upper() for sensitive in sensitive_keys):
            continue

        original = config[key]
        config[f'_original_{key}'] = original
        config[key] = _get_masked_value(value=original)
        masked_count += 1

    if logger and masked_count > 0:
        logger.debug('Замаскировано %d чувствительных параметров', masked_count)


def _get_masked_value(*, value: str | int | bool) -> str:
    """Возвращает замаскированное значение."""
    if isinstance(value, str) and '://' in value:
        return _mask_connection_string(value)
    return '***'


def _mask_connection_string(connection_string: str) -> str:
    """Маскирует connection string, оставляя схему и хост."""
    try:
        parsed = urlparse(connection_string)
    except Exception:
        return '***'
    else:
        return f'{parsed.scheme}://***@{parsed.hostname}:***'


def validate_config(
    config: ConfigDict, logger: logging.Logger | None = None
) -> tuple[bool, list[str]]:
    """Валидирует параметры конфигурации."""
    errors: list[str] = []

    if logger:
        logger.debug('Начало валидации конфигурации')

    _validate_required_params(config, errors, logger)
    _validate_db_type(config, errors, logger)
    _validate_connection_string(config, errors, logger)
    _validate_log_level(config, errors, logger)
    _validate_numeric_params(config, errors, logger)
    _validate_output_dir(config, errors, logger)

    is_valid = len(errors) == 0

    if logger:
        if is_valid:
            logger.info('✓ Конфигурация валидна')
        else:
            logger.error('✗ Валидация провалена: %d ошибок', len(errors))

    return (is_valid, errors)


def _validate_required_params(
    config: ConfigDict, errors: list[str], logger: logging.Logger | None
) -> None:
    """Проверяет наличие обязательных параметров."""
    for param in REQUIRED_CONFIG:
        if param not in config or not config.get(param):
            error = f'Отсутствует обязательный параметр: {param}'
            errors.append(error)
            if logger:
                logger.error(error)


def _validate_db_type(config: ConfigDict, errors: list[str], logger: logging.Logger | None) -> None:
    """Валидирует тип базы данных."""
    db_type = config.get('DB_TYPE', '')
    if not isinstance(db_type, str):
        msg = 'DB_TYPE должен быть строкой'
        errors.append(msg)
        if logger:
            logger.error(msg)
        return

    if db_type.lower() in VALID_DB_TYPES:
        if logger:
            logger.debug('DB_TYPE валиден: %s', db_type)
        return

    msg = f"Некорректный DB_TYPE: '{db_type}'. Допустимые значения: {', '.join(VALID_DB_TYPES)}"
    errors.append(msg)
    if logger:
        logger.error(msg)


def _validate_connection_string(
    config: ConfigDict, errors: list[str], logger: logging.Logger | None
) -> None:
    """Валидирует строку подключения."""
    conn_str = config.get('DB_CONNECT_URI')
    if not conn_str or not isinstance(conn_str, str):
        return

    original_str = _get_original_connection_string(config, conn_str)
    if not original_str:
        return

    _check_connection_string_validity(original_str, errors, logger)


def _get_original_connection_string(config: ConfigDict, fallback: str) -> str | None:
    """Получает оригинальную connection string из конфигурации."""
    original_key = '_original_CONNECTION_STRING'
    check_str = config.get(original_key, fallback)
    return check_str if isinstance(check_str, str) else None


def _check_connection_string_validity(
    connection_string: str, errors: list[str], logger: logging.Logger | None
) -> None:
    """Проверяет валидность connection string."""
    try:
        from oracle_to_excel.database import validate_connection_string  # noqa: PLC0415

        valid, error_msg = validate_connection_string(connection_string)
        if not valid:
            error = f'DB_CONNECT_URI невалиден: {error_msg}'
            errors.append(error)
            if logger:
                logger.error(error)
    except ImportError:
        if logger:
            logger.debug('Модуль database недоступен, пропуск валидации DB_CONNECT_URI')


def _validate_log_level(
    config: ConfigDict, errors: list[str], logger: logging.Logger | None
) -> None:
    """Валидирует уровень логирования."""
    log_level = config.get('LOG_LEVEL', 'INFO')
    if not isinstance(log_level, str):
        msg = 'LOG_LEVEL должен быть строкой'
        errors.append(msg)
        if logger:
            logger.error(msg)
        return

    if log_level.upper() not in VALID_LOG_LEVELS:
        msg = (
            f"Некорректный LOG_LEVEL: '{log_level}'. "
            f'Допустимые значения: {", ".join(VALID_LOG_LEVELS)}'
        )
        errors.append(msg)
        if logger:
            logger.error(msg)


def _validate_numeric_params(
    config: ConfigDict, errors: list[str], logger: logging.Logger | None
) -> None:
    """Валидирует числовые параметры."""
    numeric_params = {
        'FETCH_ARRAY_SIZE': (100, 10000),
        'CHUNK_SIZE': (1000, 100000),
        'QUERY_TIMEOUT': (10, 3600),
        'MAX_COLUMN_WIDTH': (10, 200),
        'COLUMN_WIDTH_SAMPLE_SIZE': (100, 10000),
    }

    for param, (min_val, max_val) in numeric_params.items():
        _validate_single_numeric_param(param, min_val, max_val, config, errors, logger)


def _validate_single_numeric_param(
    param: str,
    min_val: int,
    max_val: int,
    config: ConfigDict,
    errors: list[str],
    logger: logging.Logger | None,
) -> None:
    """Валидирует один числовой параметр."""
    value = config.get(param)
    if value is None:
        return

    if not isinstance(value, int):
        error = f'{param} должен быть целым числом'
        errors.append(error)
        if logger:
            logger.error(error)
        return

    if not (min_val <= value <= max_val):
        error = f'{param} = {value} вне допустимого диапазона [{min_val}, {max_val}]'
        errors.append(error)
        if logger:
            logger.error(error)


def _validate_output_dir(
    config: ConfigDict, errors: list[str], logger: logging.Logger | None
) -> None:
    """Валидирует директорию для экспорта."""
    output_dir = config.get('OUTPUT_DIR', './exports')
    if not isinstance(output_dir, str):
        return

    dir_path = Path(output_dir)
    if not _create_output_directory(dir_path, errors, logger):
        return

    _check_directory_permissions(dir_path, errors, logger)


def _create_output_directory(
    dir_path: Path, errors: list[str], logger: logging.Logger | None
) -> bool:
    """Создает директорию для экспорта."""
    try:
        dir_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        error = f'Не удалось создать OUTPUT_DIR: {e}'
        errors.append(error)
        if logger:
            logger.exception(error)
        return False

    if logger:
        logger.debug('Директория для экспорта: %s', dir_path.absolute())
    return True


def _check_directory_permissions(
    dir_path: Path, errors: list[str], logger: logging.Logger | None
) -> None:
    """Проверяет права на запись в директорию."""
    if not os.access(dir_path, os.W_OK):
        error = f'Нет прав на запись в директорию: {dir_path}'
        errors.append(error)
        if logger:
            logger.error(error)


def get_config_value[T](config: ConfigDict, key: str, default: T | None = None) -> T | None:
    """Получает значение из конфигурации."""
    value = config.get(key, default)
    return cast(T | None, value)


def restore_sensitive_data(
    config: dict[str, str | int | bool],
    logger: logging.Logger | None = None,
) -> None:
    """Восстанавливает оригинальные чувствительные данные."""
    keys_to_restore = [k for k in config if k.startswith('_original_')]
    restored_count = 0

    for key in keys_to_restore:
        original_key = key.replace('_original_', '')
        if original_key in config:
            config[original_key] = config[key]
            restored_count += 1
        del config[key]

    if logger and restored_count > 0:
        logger.debug('Восстановлено %d чувствительных параметров', restored_count)


def print_config_summary(
    config: ConfigDict,
    *,
    mask_sensitive: bool = True,
    logger: logging.Logger | None = None,
) -> None:
    """
    Выводит краткую сводку конфигурации.

    Args:
        config: Словарь конфигурации.
        mask_sensitive: Маскировать ли чувствительные данные.
        logger: Логгер для вывода (если None, используется print).
    """
    sections = [
        ('База данных', ['DB_TYPE', 'DB_CONNECT_URI']),
        ('Параметры запросов', ['FETCH_ARRAY_SIZE', 'CHUNK_SIZE', 'QUERY_TIMEOUT']),
        ('Параметры Excel', ['MAX_COLUMN_WIDTH', 'COLUMN_WIDTH_SAMPLE_SIZE']),
        ('Прочее', ['LOG_LEVEL', 'OUTPUT_DIR']),
    ]

    if logger:
        # Используем отдельные вызовы logger для каждой строки
        _log_config_header(logger)

        for section_name, params in sections:
            _log_config_section(
                section_name, params, config, mask_sensitive=mask_sensitive, logger=logger
            )

        _log_config_footer(logger)
    else:
        _print_config_to_console(sections, config, mask_sensitive=mask_sensitive)


def _log_config_header(logger: logging.Logger) -> None:
    """Логирует заголовок сводки конфигурации."""
    logger.info('=' * 60)
    logger.info('КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ')
    logger.info('=' * 60)


def _log_config_section(
    section_name: str,
    params: list[str],
    config: ConfigDict,
    *,
    mask_sensitive: bool,
    logger: logging.Logger,
) -> None:
    """Логирует одну секцию конфигурации."""
    logger.info('')
    logger.info('%s:', section_name)
    logger.info('-' * 40)

    for param in params:
        value = config.get(param, 'не задано')
        if mask_sensitive and 'DB_CONNECT_URI' in param:
            display_value = '***' if value != 'не задано' else value
        else:
            display_value = value
        logger.info('  %-28s %s', param, display_value)


def _log_config_footer(logger: logging.Logger) -> None:
    """Логирует подвал сводки конфигурации."""
    logger.info('')
    logger.info('=' * 60)


def _print_config_to_console(
    sections: list[tuple[str, list[str]]],
    config: ConfigDict,
    *,
    mask_sensitive: bool,
) -> None:
    """Выводит конфигурацию в консоль через print."""
    print()
    print('=' * 60)
    print('КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ')
    print('=' * 60)

    for section_name, params in sections:
        print()
        print(f'{section_name}:')
        print('-' * 40)

        for param in params:
            value = config.get(param, 'не задано')
            if mask_sensitive and 'DB_CONNECT_URI' in param:
                display_value = '***' if value != 'не задано' else value
            else:
                display_value = value
            print(f'  {param:<28} {display_value}')

    print()
    print('=' * 60)
    print()


def export_config_to_dict(
    config: ConfigDict,
) -> dict[str, str | int | bool]:
    """Экспортирует конфигурацию в словарь."""
    return cast(dict[str, str | int | bool], dict(config))


def create_env_example(
    output_path: str = '.env.example', logger: logging.Logger | None = None
) -> None:
    """Создает файл .env.example."""
    template = """# Database Configuration
DB_TYPE=postgresql
# Connection string examples:
# Oracle: oracle://username:password@hostname:1521/service_name
# PostgreSQL: postgresql://username:password@hostname:5432/database_name
CONNECTION_STRING=postgresql://user:password@localhost:5432/mydb

# Query Settings
FETCH_ARRAY_SIZE=1000
CHUNK_SIZE=5000
QUERY_TIMEOUT=300

# Excel Settings
MAX_COLUMN_WIDTH=50
COLUMN_WIDTH_SAMPLE_SIZE=1000

# Output Settings
OUTPUT_DIR=./exports

# Logging
LOG_LEVEL=INFO
"""

    try:
        with Path(output_path).open('w', encoding='utf-8') as f:
            f.write(template)
        msg = f'Файл {output_path} успешно создан'
        if logger:
            logger.info(msg)
        else:
            print(msg)
    except OSError as e:
        msg = f'Ошибка при создании файла: {e}'
        if logger:
            logger.exception(msg)
        else:
            print(msg)


def _test_module() -> None:
    """Тестирует модуль конфигурации."""
    from oracle_to_excel.logger import setup_logging  # noqa: PLC0415

    logger = setup_logging('DEBUG', console_output=True)
    logger.info('Тестирование модуля config.py...')

    test_env = Path('.env.test')
    test_env.write_text(
        """
DB_TYPE=postgresql
CONNECTION_STRING=postgresql://test_user:test_pass@localhost:5432/testdb
LOG_LEVEL=DEBUG
OUTPUT_DIR=./test_exports
FETCH_ARRAY_SIZE=1000
""",
        encoding='utf-8',
    )

    try:
        config = load_config('.env.test')
        logger.info('✓ Конфигурация загружена')

        valid, errors = validate_config(config, logger)
        if valid:
            logger.info('✓ Конфигурация валидна')
        else:
            logger.error('✗ Ошибки валидации: %s', errors)

        print_config_summary(config, logger=logger)
        restore_sensitive_data(cast(dict[str, str | int | bool], config), logger)
        logger.info('✓ Чувствительные данные восстановлены')

    finally:
        if test_env.exists():
            test_env.unlink()
            logger.info('✓ Тестовый файл удален')

        test_dir = Path('./test_exports')
        if test_dir.exists():
            test_dir.rmdir()
            logger.info('✓ Тестовая директория удалена')


if __name__ == '__main__':
    import sys

    match sys.argv:
        case [_, '--test']:
            _test_module()
        case [_, '--create-example']:
            create_env_example()
        case _:
            print('Использование:')
            print('  python config.py --test           # Запустить тесты')
            print('  python config.py --create-example # Создать .env.example')
