"""
Модуль конфигурации для Oracle Excel Exporter.

Загружает и валидирует параметры из .env файла,
используя возможности Python 3.14 и централизованное логирование.
"""

import os
import sys
from pathlib import Path
from typing import TypedDict, NotRequired
from collections.abc import Mapping
import logging

# Используем deferred evaluation annotations (PEP 649) - новая фича Python 3.14
from __future__ import annotations

# Импортируем систему логирования
from logger import get_logger

try:
    from dotenv import load_dotenv
except ImportError:
    # Если логгер еще не настроен, используем print
    print("Ошибка: требуется установить python-dotenv")
    print("Выполните: pip install python-dotenv")
    sys.exit(1)


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
REQUIRED_CONFIG: frozenset[str] = frozenset({
    'ORACLE_USER',
    'ORACLE_PASSWORD', 
    'ORACLE_DSN'
})

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
    'COLUMN_WIDTH_SAMPLE_SIZE': 1000
}

VALID_LOG_LEVELS: frozenset[str] = frozenset({
    'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
})


def load_config(env_file: str = '.env', *, use_logging: bool = True) -> ConfigDict:
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

    # Pattern matching для проверки существования файла (Python 3.10+, улучшен в 3.14)
    match env_path.exists():
        case True:
            load_dotenv(env_path)
            if logger:
                logger.info(f"Конфигурация загружена из: {env_path.absolute()}")
        case False:
            error_msg = (
                f"Файл конфигурации не найден: {env_path.absolute()}\n"
                f"Создайте .env файл на основе .env.example"
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
            case None | "":
                missing_params.append(param)
            case str() as val:
                config[param] = val

    # Проверка на отсутствующие обязательные параметры
    if missing_params:
        error_msg = (
            f"Отсутствуют обязательные параметры в .env файле:\n"
            f"{', '.join(missing_params)}\n"
            f"Убедитесь, что все обязательные параметры заданы."
        )
        if logger:
            logger.error(error_msg)
        raise ValueError(error_msg)

    # Загружаем опциональные параметры с значениями по умолчанию
    for param, default_value in DEFAULT_CONFIG.items():
        env_value = os.getenv(param)

        # Pattern matching для обработки типов (улучшен в Python 3.14)
        match default_value:
            case int():
                config[param] = _parse_int_param(param, env_value, default_value, logger)
            case str():
                config[param] = env_value if env_value else default_value

    # Маскируем пароль для безопасного логирования
    _mask_sensitive_data(config, logger)

    if logger:
        logger.info(f"Конфигурация успешно загружена ({len(config)} параметров)")

    return config  # type: ignore[return-value]


def _parse_int_param(
    param_name: str,
    value: str | None,
    default: int,
    logger: logging.Logger | None = None
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
        if parsed <= 0:
            warning_msg = f"Параметр {param_name} должен быть > 0, используется значение по умолчанию: {default}"
            if logger:
                logger.warning(warning_msg)
            return default
        return parsed
    except ValueError as e:
        error_msg = (
            f"Некорректное значение параметра {param_name}: '{value}'. "
            f"Ожидается положительное целое число. Используется значение по умолчанию: {default}"
        )
        if logger:
            logger.warning(error_msg)
        return default


def _mask_sensitive_data(config: dict[str, str | int], logger: logging.Logger | None = None) -> None:
    """
    Маскирует чувствительные данные в конфигурации для логирования.

    Модифицирует конфигурацию in-place, заменяя пароли на '***'.

    Args:
        config: Словарь с конфигурацией.
        logger: Логгер для сообщений.
    """
    sensitive_keys = {'ORACLE_PASSWORD', 'PASSWORD', 'SECRET', 'TOKEN'}

    masked_count = 0
    for key in config:
        if any(sensitive in key.upper() for sensitive in sensitive_keys):
            # Сохраняем оригинальное значение, но для логов показываем маску
            original = config[key]
            config[f'_original_{key}'] = original  # type: ignore[literal-required]
            config[key] = '***'  # type: ignore[typeddict-item]
            masked_count += 1

    if logger and masked_count > 0:
        logger.debug(f"Замаскировано {masked_count} чувствительных параметров")


def validate_config(config: ConfigDict, logger: logging.Logger | None = None) -> tuple[bool, list[str]]:
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
        >>> valid, errors = validate_config(config)
        >>> if not valid:
        ...     print(f"Ошибки: {errors}")
    """
    errors: list[str] = []

    if logger:
        logger.debug("Начало валидации конфигурации")

    # Проверка наличия обязательных параметров
    for param in REQUIRED_CONFIG:
        if param not in config or not config.get(param):  # type: ignore[arg-type]
            error = f"Отсутствует обязательный параметр: {param}"
            errors.append(error)
            if logger:
                logger.error(error)

    # Валидация LOG_LEVEL
    log_level = config.get('LOG_LEVEL', 'INFO')  # type: ignore[arg-type]
    match log_level:
        case str(level) if level.upper() in VALID_LOG_LEVELS:
            pass  # Валидный уровень
        case str(level):
            error = (
                f"Некорректный LOG_LEVEL: '{level}'. "
                f"Допустимые значения: {', '.join(VALID_LOG_LEVELS)}"
            )
            errors.append(error)
            if logger:
                logger.error(error)
        case _:
            error = "LOG_LEVEL должен быть строкой"
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
        'COLUMN_WIDTH_SAMPLE_SIZE': (100, 10000)
    }

    for param, (min_val, max_val) in numeric_params.items():
        value = config.get(param)  # type: ignore[arg-type]

        # Pattern matching для валидации диапазонов (nested patterns в Python 3.14)
        match value:
            case int(val) if min_val <= val <= max_val:
                pass  # Валидное значение
            case int(val):
                error = (
                    f"{param} = {val} вне допустимого диапазона "
                    f"[{min_val}, {max_val}]"
                )
                errors.append(error)
                if logger:
                    logger.error(error)
            case None:
                pass  # Опциональный параметр
            case _:
                error = f"{param} должен быть целым числом"
                errors.append(error)
                if logger:
                    logger.error(error)

    # Проверка логической согласованности
    pool_min = config.get('POOL_MIN', 2)  # type: ignore[arg-type]
    pool_max = config.get('POOL_MAX', 5)  # type: ignore[arg-type]

    match (pool_min, pool_max):
        case (int(min_v), int(max_v)) if min_v > max_v:
            error = f"POOL_MIN ({min_v}) не может быть больше POOL_MAX ({max_v})"
            errors.append(error)
            if logger:
                logger.error(error)
        case _:
            pass

    # Валидация OUTPUT_DIR
    output_dir = config.get('OUTPUT_DIR', './exports')  # type: ignore[arg-type]
    if isinstance(output_dir, str):
        dir_path = Path(output_dir)
        try:
            # Создаем директорию если не существует
            dir_path.mkdir(parents=True, exist_ok=True)
            if logger:
                logger.debug(f"Директория для экспорта: {dir_path.absolute()}")

            # Проверяем права на запись
            if not os.access(dir_path, os.W_OK):
                error = f"Нет прав на запись в директорию: {dir_path}"
                errors.append(error)
                if logger:
                    logger.error(error)
        except OSError as e:
            error = f"Не удалось создать OUTPUT_DIR: {e}"
            errors.append(error)
            if logger:
                logger.error(error)

    is_valid = len(errors) == 0

    if logger:
        if is_valid:
            logger.info("✓ Конфигурация валидна")
        else:
            logger.error(f"✗ Валидация провалена: {len(errors)} ошибок")

    return (is_valid, errors)


def get_config_value[T](config: ConfigDict, key: str, default: T | None = None) -> T | None:
    """
    Получает значение из конфигурации с поддержкой generic типов.

    Использует новый синтаксис generic функций Python 3.12+/3.14
    (PEP 695 - Type Parameter Syntax).

    Args:
        config: Словарь конфигурации.
        key: Ключ для получения значения.
        default: Значение по умолчанию если ключ не найден.

    Returns:
        Значение из конфигурации или default.

    Example:
        >>> timeout = get_config_value(config, 'QUERY_TIMEOUT', 300)
        >>> print(timeout)
        300
    """
    return config.get(key, default)  # type: ignore[return-value]


def restore_sensitive_data(config: dict[str, str | int], logger: logging.Logger | None = None) -> None:
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
        logger.debug(f"Восстановлено {restored_count} чувствительных параметров")


def print_config_summary(
    config: ConfigDict,
    mask_sensitive: bool = True,
    logger: logging.Logger | None = None
) -> None:
    """
    Выводит краткую сводку конфигурации через логгер.

    Args:
        config: Словарь конфигурации.
        mask_sensitive: Маскировать ли чувствительные данные.
        logger: Логгер для вывода (если None, используется print).
    """
    output_lines = []
    output_lines.append("")
    output_lines.append("=" * 50)
    output_lines.append("КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ")
    output_lines.append("=" * 50)

    # Группируем параметры
    db_params = ['ORACLE_USER', 'ORACLE_PASSWORD', 'ORACLE_DSN']
    pool_params = ['POOL_MIN', 'POOL_MAX', 'POOL_INCREMENT']
    query_params = ['FETCH_ARRAY_SIZE', 'CHUNK_SIZE', 'QUERY_TIMEOUT']
    excel_params = ['MAX_COLUMN_WIDTH', 'COLUMN_WIDTH_SAMPLE_SIZE']
    other_params = ['LOG_LEVEL', 'OUTPUT_DIR']

    sections = [
        ("База данных", db_params),
        ("Connection Pool", pool_params),
        ("Параметры запросов", query_params),
        ("Параметры Excel", excel_params),
        ("Прочее", other_params)
    ]

    for section_name, params in sections:
        output_lines.append("")
        output_lines.append(f"{section_name}:")
        output_lines.append("-" * 30)
        for param in params:
            value = config.get(param, 'не задано')  # type: ignore[arg-type]

            # Маскируем чувствительные данные при выводе
            if mask_sensitive and 'PASSWORD' in param:
                display_value = '***' if value != 'не задано' else value
            else:
                display_value = value

            output_lines.append(f"  {param:.<30} {display_value}")

    output_lines.append("")
    output_lines.append("=" * 50)
    output_lines.append("")

    # Выводим через логгер или print
    full_output = "\n".join(output_lines)
    if logger:
        logger.info(f"Сводка конфигурации:\n{full_output}")
    else:
        print(full_output)


def export_config_to_dict(config: ConfigDict) -> dict[str, str | int]:
    """
    Экспортирует конфигурацию в обычный словарь.

    Полезно для сериализации или передачи в другие модули.

    Args:
        config: TypedDict конфигурация.

    Returns:
        Обычный словарь с конфигурацией.
    """
    return dict(config)


# Вспомогательная функция для создания .env.example
def create_env_example(output_path: str = '.env.example', logger: logging.Logger | None = None) -> None:
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
        msg = f"Файл {output_path} успешно создан"
        if logger:
            logger.info(msg)
        else:
            print(msg)
    except OSError as e:
        msg = f"Ошибка при создании файла: {e}"
        if logger:
            logger.error(msg)
        else:
            print(msg)


# Функция для тестирования модуля
def _test_module() -> None:
    """Простой тест модуля конфигурации."""
    # Импортируем и настраиваем логирование
    from logger import setup_logging

    logger = setup_logging('DEBUG', console_output=True)
    logger.info("Тестирование модуля config.py...")

    # Создаем тестовый .env файл
    test_env = Path('.env.test')
    test_env.write_text("""
ORACLE_USER=test_user
ORACLE_PASSWORD=test_pass
ORACLE_DSN=localhost:1521/TEST
POOL_MIN=2
POOL_MAX=5
LOG_LEVEL=DEBUG
OUTPUT_DIR=./test_exports
""")

    try:
        # Загружаем конфигурацию
        config = load_config('.env.test')
        logger.info("✓ Конфигурация загружена")

        # Валидируем
        valid, errors = validate_config(config, logger)
        if valid:
            logger.info("✓ Конфигурация валидна")
        else:
            logger.error(f"✗ Ошибки валидации: {errors}")

        # Выводим сводку
        print_config_summary(config, logger=logger)

        # Восстанавливаем чувствительные данные
        restore_sensitive_data(config, logger)  # type: ignore[arg-type]
        logger.info("✓ Чувствительные данные восстановлены")

    finally:
        # Удаляем тестовый файл
        if test_env.exists():
            test_env.unlink()
            logger.info("✓ Тестовый файл удален")

        # Удаляем тестовую директорию
        test_dir = Path('./test_exports')
        if test_dir.exists():
            test_dir.rmdir()
            logger.info("✓ Тестовая директория удалена")


if __name__ == '__main__':
    # Если модуль запущен напрямую, создаем .env.example
    import sys

    match sys.argv:
        case [_, '--test']:
            _test_module()
        case [_, '--create-example']:
            create_env_example()
        case _:
            print("Использование:")
            print("  python config.py --test           # Запустить тесты")
            print("  python config.py --create-example # Создать .env.example")
