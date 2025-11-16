"""
Модуль конфигурации для Oracle Excel Exporter.

Загружает и валидирует параметры из .env файла,
используя возможности Python 3.14 и централизованное логирование.
"""

# PEP 649: Deferred evaluation annotations - новая фича Python 3.14
from __future__ import annotations

import logging
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import NotRequired, TypedDict

try:
    from dotenv import load_dotenv
except ImportError:
    # Когда dotenv нет, используем print
    print('Ошибка: требуется python-dotenv')
    print('Выполните: pip install python-dotenv')
    sys.exit(1)

from oracle_to_excel.logger import get_logger


# Определение типов конфигурации с использованием TypedDict
class ConfigDict(TypedDict):
    """Структура конфигурации приложения."""

    # Обязательные параметры
    ORACLE_USER: str
    ORACLE_PASSWORD: str
    ORACLE_DSN: str

    # Опциональные параметры (NotRequired из Python 3.11+)
    POOL_MIN: NotRequired[int]
    POOL_MAX: NotRequired[int]
    POOL_INCREMENT: NotRequired[int]
    LOG_LEVEL: NotRequired[str]
    OUTPUT_DIR: NotRequired[str]
    FETCH_ARRAY_SIZE: NotRequired[int]
    CHUNK_SIZE: NotRequired[int]
    QUERY_TIMEOUT: NotRequired[int]
    MAX_COLUMN_WIDTH: NotRequired[int]
    COLUMN_WIDTH_SAMPLE_SIZE: NotRequired[int]


# Константы для валидации
REQUIRED_CONFIG: frozenset[str] = frozenset(
    {
        'ORACLE_USER',
        'ORACLE_PASSWORD',
        'ORACLE_DSN',
    }
)

# Значения по умолчанию с использованием frozendict (immutable)
DEFAULT_CONFIG: Mapping[str, int | str] = {
    'POOL_MIN': 2,
    'POOL_MAX': 5,
    'POOL_INCREMENT': 1,
    'LOG_LEVEL': 'INFO',
    'OUTPUT_DIR': './exports',
    'FETCH_ARRAY_SIZE': 1000,
    'CHUNK_SIZE': 5000,
    'QUERY_TIMEOUT': 300,
    'MAX_COLUMN_WIDTH': 50,
    'COLUMN_WIDTH_SAMPLE_SIZE': 1000,
}

VALID_LOG_LEVELS: frozenset[str] = frozenset(
    {
        'DEBUG',
        'INFO',
        'WARNING',
        'ERROR',
        'CRITICAL',
    }
)


def load_config(  # noqa: C901
    env_file: str = '.env',
    *,
    use_logging: bool = True,
) -> ConfigDict:
    """
    Загружает конфигурацию из .env файла.

    Использует pattern matching (улучшенный в Python 3.14) для обработки
    различных сценариев загрузки файла.

    Args:
        env_file: Путь к файлу с переменными окружения.
        use_logging: Использовать ли систему логирования (False для bootstrap).

    Returns:
        Словарь с конфигурацией приложения.

    Raises:
        FileNotFoundError: Если .env файл не найден.
        ValueError: Если отсутствуют обязательные параметры.

    Example:
        >>> config = load_config('.env')
        >>> print(config['ORACLE_USER'])
        'admin'
    """
    # Получаем логгер (или None если логирование еще не настроено)
    logger = get_logger('config') if use_logging else None

    env_path = Path(env_file)

    # Pattern matching для проверки существования файла (Python 3.10+)
    match env_path.exists():
        case True:
            load_dotenv(env_path)
            if logger:
                logger.info(
                    'Конфигурация загружена из: %s',
                    env_path.absolute(),
                )
        case False:
            error_msg = (
                f'Файл конфигурации не найден: {env_path.absolute()}\n'
                f'Создайте .env файл на основе .env.example'
            )
            if logger:
                logger.error(error_msg)
            raise FileNotFoundError(error_msg)

    # Загружаем все переменные
    config: dict[str, str | int] = {}

    # Загружаем обязательные параметры
    missing_params = []
    for param in REQUIRED_CONFIG:
        value = os.getenv(param)
        match value:
            case None | '':
                missing_params.append(param)
            case str() as val:
                config[param] = val

    # Проверка на отсутствующие обязательные параметры
    if missing_params:
        error_msg = (
            f'Отсутствуют обязательные параметры в .env файле:\n'
            f'{", ".join(missing_params)}\n'
            f'Убедитесь, что все обязательные параметры заданы.'
        )
        if logger:
            logger.error(error_msg)
        raise ValueError(error_msg)

    # Загружаем опциональные параметры с значениями по умолчанию
    for param, default_value in DEFAULT_CONFIG.items():
        env_value = os.getenv(param)

        # Pattern matching для обработки типов (Python 3.14)
        match default_value:
            case int():
                config[param] = _parse_int_param(
                    param,
                    env_value,
                    default_value,
                    logger,
                )
            case str():
                config[param] = env_value if env_value else default_value

    # Маскируем пароль для безопасного логирования
    _mask_sensitive_data(config, logger)

    if logger:
        logger.info(
            'Конфигурация загружена (%d параметров)',
            len(config),
        )

    return config  # type: ignore[return-value]


def _parse_int_param(  # noqa: C901
    param_name: str,
    value: str | None,
    default: int,
    logger: logging.Logger | None = None,
) -> int:
    """
    Парсит целочисленный параметр из строки.

    Использует улучшенную обработку исключений Python 3.14
    (PEP 758 - except без скобок).

    Args:
        param_name: Имя параметра для сообщений об ошибках.
        value: Строковое значение параметра.
        default: Значение по умолчанию.
        logger: Логгер для сообщений.

    Returns:
        Целочисленное значение параметра.

    Raises:
        ValueError: Если значение не является корректным целым числом.
    """
    if not value:
        return default

    try:
        parsed = int(value)
    except ValueError:
        error_msg = (
            f'Некорректное значение '
            f'параметра {param_name}: '
            f"'{value}'. Ожидается "
            f'положительное целое число. '
            f'Используется значение '
            f'по умолчанию: {default}'
        )
        if logger:
            logger.warning(error_msg)
        return default
    else:
        if parsed <= 0:
            warning_msg = (
                f'Параметр {param_name} должен '
                f'быть > 0, используется '
                f'значение по умолчанию: {default}'
            )
            if logger:
                logger.warning(warning_msg)
            return default
        return parsed


def _mask_sensitive_data(
    config: dict[str, str | int],
    logger: logging.Logger | None = None,
) -> None:
    """
    Маскирует чувствительные данные в конфигурации для логирования.

    Модифицирует конфигурацию in-place, заменяя пароли на '***'.

    Args:
        config: Словарь с конфигурацией.
        logger: Логгер для сообщений.
    """
    sensitive_keys = {
        'ORACLE_PASSWORD',
        'PASSWORD',
        'SECRET',
        'TOKEN',
    }

    masked_count = 0
    for key, value in list(config.items()):
        if any(sensitive in key.upper() for sensitive in sensitive_keys):
            # Сохраняем оригинальное значение
            config[f'_original_{key}'] = value
            config[key] = '***'
            masked_count += 1

    if logger and masked_count > 0:
        logger.debug(
            'Замаскировано %d чувствительных параметров',
            masked_count,
        )


def validate_config(  # noqa: C901
    config: ConfigDict,
    logger: logging.Logger | None = None,
) -> tuple[bool, list[str]]:
    """
    Валидирует параметры конфигурации.

    Использует pattern matching для проверки различных типов ошибок
    (улучшен в Python 3.14 с поддержкой nested patterns).

    Args:
        config: Словарь с конфигурацией для проверки.
        logger: Логгер для сообщений.

    Returns:
        Кортеж (валидность, список ошибок).

    Example:
        >>> config = {'ORACLE_USER': 'admin'}
        >>> valid, errors = validate_config(config)  # type: ignore
        >>> if not valid:
        ...     print(f'Ошибки: {errors}')
    """
    errors: list[str] = []

    if logger:
        logger.debug('Начало валидации конфигурации')

    # Проверка наличия обязательных параметров
    for param in REQUIRED_CONFIG:
        value = config.get(param)
        if not value:
            error = f'Отсутствует обязательный параметр: {param}'
            errors.append(error)
            if logger:
                logger.error(error)

    # Валидация LOG_LEVEL
    log_level = config.get('LOG_LEVEL', 'INFO')
    match log_level:
        case str(level) if level.upper() in VALID_LOG_LEVELS:
            pass  # Валидный уровень
        case str(level):
            error = (
                f"Некорректный LOG_LEVEL: '{level}'. "
                f'Допустимые значения: {", ".join(VALID_LOG_LEVELS)}'
            )
            errors.append(error)
            if logger:
                logger.error(error)

    # Валидация числовых параметров
    numeric_params = {
        'POOL_MIN': (1, 100),
        'POOL_MAX': (1, 100),
        'POOL_INCREMENT': (1, 10),
        'FETCH_ARRAY_SIZE': (100, 10000),
        'CHUNK_SIZE': (1000, 100000),
        'QUERY_TIMEOUT': (10, 3600),
        'MAX_COLUMN_WIDTH': (10, 200),
        'COLUMN_WIDTH_SAMPLE_SIZE': (100, 10000),
    }

    for param, (min_val, max_val) in numeric_params.items():
        value = config.get(param)

        # Pattern matching для проверки диапазонов
        match value:
            case int(val) if min_val <= val <= max_val:
                pass  # Валидное значение
            case int(val):
                error = f'{param} = {val} вне допустимого диапазона [{min_val}, {max_val}]'
                errors.append(error)
                if logger:
                    logger.error(error)
            case None:
                pass  # Опциональный параметр
            case _:
                error = f'{param} должен быть целым числом'
                errors.append(error)
                if logger:
                    logger.error(error)

    # Проверка логической согласованности
    pool_min = config.get('POOL_MIN', 2)
    pool_max = config.get('POOL_MAX', 5)

    match (pool_min, pool_max):
        case (int(min_v), int(max_v)) if min_v > max_v:
            error = f'POOL_MIN ({min_v}) не может быть больше POOL_MAX ({max_v})'
            errors.append(error)
            if logger:
                logger.error(error)
        case _:
            pass

    # Валидация OUTPUT_DIR
    output_dir = config.get('OUTPUT_DIR', './exports')
    if isinstance(output_dir, str):
        dir_path = Path(output_dir)
        try:
            # Создаем директорию если не существует
            dir_path.mkdir(parents=True, exist_ok=True)
            if logger:
                logger.debug(
                    'Директория для экспорта: %s',
                    dir_path.absolute(),
                )

            # Проверяем права на запись
            if not os.access(dir_path, os.W_OK):
                error = f'Нет прав на запись в директорию: {dir_path}'
                errors.append(error)
                if logger:
                    logger.error(error)
        except OSError:
            error = 'Ne udalosy sozdat OUTPUT_DIR'
            errors.append(error)
            if logger:
                logger.exception(error)

    is_valid = len(errors) == 0

    if logger:
        if is_valid:
            logger.info('Конфигурация валидна')
        else:
            logger.error(
                'Валидация провалена: %d ошибок',
                len(errors),
            )

    return (is_valid, errors)


def get_config_value(
    config: ConfigDict,
    key: str,
    default: object = None,
) -> object:
    """
    Получает значение из конфигурации.

    Args:
        config: Словарь конфигурации.
        key: Ключ для получения значения.
        default: Значение по умолчанию.

    Returns:
        Значение из конфигурации или default.
    """
    return config.get(key, default)


def export_config_to_dict(
    config: ConfigDict,
) -> dict[str, str | int]:
    """
    Экспортирует конфигурацию в обычный словарь.

    Args:
        config: TypedDict конфигурация.

    Returns:
        Обычный словарь.
    """
    return dict(config)  # type: ignore


def restore_sensitive_data(
    config: dict[str, str | int],
    logger: logging.Logger | None = None,
) -> None:
    """
    Восстанавливает оригинальные чувствительные данные.

    После маскирования для логирования, эта функция восстанавливает
    реальные значения паролей для использования в приложении.

    Args:
        config: Словарь с конфигурацией.
        logger: Логгер для сообщений.
    """
    keys_to_restore = [k for k in config if k.startswith('_original_')]

    restored_count = 0
    for key in keys_to_restore:
        original_key = key.replace('_original_', '')
        if original_key in config:
            config[original_key] = config[key]
            restored_count += 1
        del config[key]

    if logger and restored_count > 0:
        logger.debug(
            'Восстановлено %d чувствительных параметров',
            restored_count,
        )


def print_config_summary(
    config: ConfigDict,
    *,
    mask_sensitive: bool = True,
    logger: logging.Logger | None = None,
) -> None:
    """
    Выводит краткую сводку конфигурации через логгер.

    Args:
        config: Словарь конфигурации.
        mask_sensitive: Маскировать ли чувствительные данные.
        logger: Логгер для вывода (если None, используется print).
    """
    output_lines = []
    output_lines.append('')
    output_lines.append('=' * 50)
    output_lines.append('КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ')
    output_lines.append('=' * 50)

    # Группируем параметры
    db_params = ['ORACLE_USER', 'ORACLE_PASSWORD', 'ORACLE_DSN']
    pool_params = ['POOL_MIN', 'POOL_MAX', 'POOL_INCREMENT']
    query_params = ['FETCH_ARRAY_SIZE', 'CHUNK_SIZE', 'QUERY_TIMEOUT']
    excel_params = ['MAX_COLUMN_WIDTH', 'COLUMN_WIDTH_SAMPLE_SIZE']
    other_params = ['LOG_LEVEL', 'OUTPUT_DIR']

    sections = [
        ('База данных', db_params),
        ('Connection Pool', pool_params),
        ('Параметры запросов', query_params),
        ('Параметры Excel', excel_params),
        ('Прочее', other_params),
    ]

    for section_name, params in sections:
        output_lines.append('')
        output_lines.append(f'{section_name}:')
        output_lines.append('-' * 30)
        for param in params:
            value = config.get(param, 'не задано')

            # Маскируем чувствительные данные при выводе
            if mask_sensitive and 'PASSWORD' in param:
                display_value = '***' if value != 'не задано' else value
            else:
                display_value = value

            output_lines.append(f'  {param:.<30} {display_value}')

    output_lines.append('')
    output_lines.append('=' * 50)
    output_lines.append('')

    # Выводим через логгер или print
    full_output = '\n'.join(output_lines)
    if logger:
        logger.info('Svodka konfiguracii:')
        for line in output_lines:
            logger.info(line)
    else:
        print(full_output)


def create_env_example(
    output_path: str = '.env.example',
    logger: logging.Logger | None = None,
) -> None:
    """
    Создает файл .env.example с шаблоном конфигурации.

    Args:
        output_path: Путь к создаваемому файлу.
        logger: Логгер для сообщений.
    """
    template = """# Oracle Database Connection
ORACLE_USER=your_username
ORACLE_PASSWORD=your_password
ORACLE_DSN=localhost:1521/ORCL

# Connection Pool Settings
POOL_MIN=2
POOL_MAX=5
POOL_INCREMENT=1

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
        msg = f'File {output_path} created successfully'
        if logger:
            logger.info(msg)
        else:
            print(msg)
    except OSError:
        msg = f'Error creating file: {output_path}'
        if logger:
            logger.exception(msg)
        else:
            print(msg)


if __name__ == '__main__':
    # Если модуль запущен напрямую, создаем .env.example
    import sys

    match sys.argv:
        case [_, '--create-example']:
            create_env_example()
        case _:
            print('Использование:')
            print('  python config.py --create-example # Создать .env.example')
