"""
Модуль системы логирования для Oracle Excel Exporter.

Настраивает централизованное логирование с поддержкой
ротации файлов и фильтрации чувствительных данных.
"""

# PEP 649: Deferred annotation evaluation
from __future__ import annotations

import logging
import logging.handlers
import re
import sys
from collections.abc import Callable, Sequence
from functools import wraps
from pathlib import Path
from time import perf_counter
from typing import ParamSpec, TypeAlias, TypeVar

# Type aliases для улучшенной читаемости (Python 3.12+)
LogLevel: TypeAlias = str | int
FilterFunc: TypeAlias = Callable[[logging.LogRecord], bool]

# Generic types для декораторов (PEP 695)
P = ParamSpec('P')
R = TypeVar('R')

# Константы
DEFAULT_LOG_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DEFAULT_DATE_FORMAT: str = '%Y-%m-%d %H:%M:%S'
MAX_LOG_FILE_SIZE: int = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT: int = 3

# Паттерны для маскирования чувствительных данных
SENSITIVE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r'password["\']?\s*[:=]\s*["\']?([^"\'\\s]+)', r'password=***'),
    (r'PASSWORD["\']?\s*[:=]\s*["\']?([^"\'\\s]+)', r'PASSWORD=***'),
    (r'token["\']?\s*[:=]\s*["\']?([^"\'\\s]+)', r'token=***'),
    (r'secret["\']?\s*[:=]\s*["\']?([^"\'\\s]+)', r'secret=***'),
    (r'apikey["\']?\s*[:=]\s*["\']?([^"\'\\s]+)', r'apikey=***'),
)


def setup_logging(
    log_level: LogLevel = 'INFO',
    log_file: str | Path | None = None,
    logger_name: str = 'oracle_exporter',
    console_output: bool = True,
    mask_sensitive: bool = True,
) -> logging.Logger:
    """
    Настраивает систему логирования с поддержкой консоли и файлов.

    Использует улучшенную систему типов Python 3.14 и pattern matching
    для гибкой конфигурации логгеров.

    Args:
        log_level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Путь к файлу логов (опционально).
        logger_name: Имя логгера.
        console_output: Выводить ли логи в консоль.
        mask_sensitive: Маскировать ли чувствительные данные.

    Returns:
        Настроенный объект Logger.

    Example:
        >>> logger = setup_logging('DEBUG', 'app.log')
        >>> logger.info('Приложение запущено')
    """
    # Получаем или создаем логгер
    logger = logging.getLogger(logger_name)

    # Очищаем существующие handlers (избегаем дублирования)
    logger.handlers.clear()

    # Устанавливаем уровень логирования
    numeric_level = _parse_log_level(log_level)
    logger.setLevel(numeric_level)

    # Создаем форматтер
    formatter = _create_formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)

    # Pattern matching для настройки handlers (улучшен в Python 3.14)
    match (console_output, log_file):
        case (True, None):
            # Только консольный вывод
            _add_console_handler(logger, formatter)
        case (False, str() | Path() as file):
            # Только файловый вывод
            _add_file_handler(logger, formatter, file)
        case (True, str() | Path() as file):
            # Оба варианта
            _add_console_handler(logger, formatter)
            _add_file_handler(logger, formatter, file)
        case (False, None):
            # Fallback: хотя бы консоль
            _add_console_handler(logger, formatter)
            logger.warning('Логирование не настроено должным образом, используется консоль')

    # Добавляем фильтр для маскирования чувствительных данных
    if mask_sensitive:
        logger.addFilter(_create_sensitive_filter())

    logger.debug(f"Логгер '{logger_name}' настроен с уровнем {logging.getLevelName(numeric_level)}")

    return logger


def _parse_log_level(level: LogLevel) -> int:
    """
    Преобразует строковый уровень логирования в числовой.

    Использует pattern matching для обработки различных форматов.

    Args:
        level: Уровень логирования (строка или число).

    Returns:
        Числовой уровень логирования.

    Raises:
        ValueError: Если уровень некорректен.
    """
    match level:
        case int() as numeric_level if numeric_level in {0, 10, 20, 30, 40, 50}:
            return numeric_level
        case str() as string_level:
            upper_level = string_level.upper()
            if hasattr(logging, upper_level):
                return getattr(logging, upper_level)
            raise ValueError(f'Некорректный уровень логирования: {level}')
        case _:
            raise ValueError(f'Неподдерживаемый тип уровня логирования: {type(level)}')


def _create_formatter(fmt: str, datefmt: str) -> logging.Formatter:
    """
    Создает форматтер для логов.

    Args:
        fmt: Формат сообщения.
        datefmt: Формат даты и времени.

    Returns:
        Объект Formatter.
    """
    return logging.Formatter(fmt=fmt, datefmt=datefmt)


def _add_console_handler(logger: logging.Logger, formatter: logging.Formatter) -> None:
    """
    Добавляет handler для вывода в консоль.

    Args:
        logger: Логгер для настройки.
        formatter: Форматтер для handler.
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Цветной вывод для консоли (если терминал поддерживает)
    if _supports_color():
        console_handler.setFormatter(_create_colored_formatter())

    logger.addHandler(console_handler)


def _add_file_handler(
    logger: logging.Logger, formatter: logging.Formatter, log_file: str | Path
) -> None:
    """
    Добавляет RotatingFileHandler для записи в файл.

    Args:
        logger: Логгер для настройки.
        formatter: Форматтер для handler.
        log_file: Путь к файлу логов.
    """
    file_path = Path(log_file)

    # Создаем директорию если не существует
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Rotating handler для автоматической ротации файлов
    file_handler = logging.handlers.RotatingFileHandler(
        filename=file_path, maxBytes=MAX_LOG_FILE_SIZE, backupCount=BACKUP_COUNT, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def _supports_color() -> bool:
    """
    Проверяет, поддерживает ли терминал цветной вывод.

    Returns:
        True если поддерживается, False иначе.
    """
    return (
        hasattr(sys.stdout, 'isatty')
        and sys.stdout.isatty()
        and sys.platform != 'win32'  # Windows требует дополнительной настройки
    )


def _create_colored_formatter() -> logging.Formatter:
    """
    Создает форматтер с цветным выводом для консоли.

    Returns:
        Форматтер с ANSI escape codes для цветов.
    """
    # ANSI color codes
    colors = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[32m',  # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',  # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m',
    }

    class ColoredFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            levelname = record.levelname
            if levelname in colors:
                record.levelname = f'{colors[levelname]}{levelname}{colors["RESET"]}'
            return super().format(record)

    return ColoredFormatter(fmt=DEFAULT_LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT)


def _create_sensitive_filter() -> FilterFunc:
    """
    Создает фильтр для маскирования чувствительных данных в логах.

    Returns:
        Функция-фильтр для логов.
    """
    compiled_patterns = [
        (re.compile(pattern, re.IGNORECASE), replacement)
        for pattern, replacement in SENSITIVE_PATTERNS
    ]

    def filter_sensitive(record: logging.LogRecord) -> bool:
        """Маскирует чувствительные данные в сообщении лога."""
        original_msg = record.getMessage()

        # Применяем все паттерны маскирования
        filtered_msg = original_msg
        for pattern, replacement in compiled_patterns:
            filtered_msg = pattern.sub(replacement, filtered_msg)

        # Обновляем сообщение если что-то изменилось
        if filtered_msg != original_msg:
            record.msg = filtered_msg
            record.args = ()

        return True  # Всегда пропускаем запись

    return filter_sensitive


def log_execution_time[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    """
    Декоратор для логирования времени выполнения функций.

    Использует новый синтаксис generic функций Python 3.14 (PEP 695).

    Args:
        func: Декорируемая функция.

    Returns:
        Обернутая функция с логированием времени.

    Example:
        >>> @log_execution_time
        ... def process_data():
        ...     # some processing
        ...     pass
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        logger = logging.getLogger('oracle_exporter.performance')

        func_name = func.__name__
        module_name = func.__module__

        logger.debug(f'Начало выполнения: {module_name}.{func_name}')
        start_time = perf_counter()

        try:
            result = func(*args, **kwargs)
            elapsed_time = perf_counter() - start_time

            logger.info(f'Завершено: {module_name}.{func_name} (время: {elapsed_time:.4f}s)')

            return result
        except Exception as e:
            elapsed_time = perf_counter() - start_time
            logger.error(
                f'Ошибка в {module_name}.{func_name} после {elapsed_time:.4f}s: {e}', exc_info=True
            )
            raise

    return wrapper


def log_function_call[**P, R](
    log_args: bool = True, log_result: bool = False
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Декоратор для подробного логирования вызовов функций.

    Args:
        log_args: Логировать ли аргументы функции.
        log_result: Логировать ли результат выполнения.

    Returns:
        Декоратор для функции.

    Example:
        >>> @log_function_call(log_args=True, log_result=True)
        ... def calculate(x: int, y: int) -> int:
        ...     return x + y
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            logger = logging.getLogger('oracle_exporter.trace')

            func_name = f'{func.__module__}.{func.__name__}'

            # Логируем вызов
            if log_args:
                args_repr = ', '.join(repr(arg) for arg in args)
                kwargs_repr = ', '.join(f'{k}={v!r}' for k, v in kwargs.items())
                all_args = ', '.join(filter(None, [args_repr, kwargs_repr]))
                logger.debug(f'Вызов: {func_name}({all_args})')
            else:
                logger.debug(f'Вызов: {func_name}')

            # Выполняем функцию
            result = func(*args, **kwargs)

            # Логируем результат
            if log_result:
                logger.debug(f'Результат {func_name}: {result!r}')

            return result

        return wrapper

    return decorator


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Получает логгер по имени.

    Args:
        name: Имя логгера. Если None, возвращает root logger приложения.

    Returns:
        Объект Logger.

    Example:
        >>> logger = get_logger('oracle_exporter.database')
        >>> logger.info('Подключение установлено')
    """
    if name is None:
        return logging.getLogger('oracle_exporter')

    # Добавляем префикс если его нет
    if not name.startswith('oracle_exporter.'):
        name = f'oracle_exporter.{name}'

    return logging.getLogger(name)


def log_exception(
    logger: logging.Logger | None = None,
    message: str = 'Произошла ошибка',
    level: int = logging.ERROR,
) -> None:
    """
    Логирует текущее исключение с трейсбеком.

    Args:
        logger: Логгер для использования. Если None, использует root.
        message: Сообщение об ошибке.
        level: Уровень логирования.

    Example:
        >>> try:
        ...     risky_operation()
        ... except Exception:
        ...     log_exception(logger, "Операция провалилась")
    """
    if logger is None:
        logger = get_logger()

    logger.log(level, message, exc_info=True)


def create_context_logger(logger: logging.Logger, **context: str | int | float) -> Callable:
    """
    Создает функцию логирования с дополнительным контекстом.

    Useful для добавления метаданных ко всем сообщениям (session_id, user_id и т.д.).

    Args:
        logger: Базовый логгер.
        **context: Дополнительные поля контекста.

    Returns:
        Функция для логирования с контекстом.

    Example:
        >>> logger = get_logger()
        >>> log_with_context = create_context_logger(logger, session='SES001', user='admin')
        >>> log_with_context('INFO', 'Обработка данных')
    """

    def log_with_context(level: str, message: str, **extra: str | int | float) -> None:
        # Объединяем контекст и дополнительные параметры
        full_context = {**context, **extra}
        context_str = ' | '.join(f'{k}={v}' for k, v in full_context.items())
        full_message = f'[{context_str}] {message}'

        # Логируем с учетом уровня
        numeric_level = _parse_log_level(level)
        logger.log(numeric_level, full_message)

    return log_with_context


def configure_module_logger(
    module_name: str,
    level: LogLevel | None = None,
    handlers: Sequence[logging.Handler] | None = None,
) -> logging.Logger:
    """
    Настраивает логгер для конкретного модуля.

    Args:
        module_name: Имя модуля.
        level: Уровень логирования (опционально).
        handlers: Список handlers (опционально).

    Returns:
        Настроенный логгер модуля.

    Example:
        >>> logger = configure_module_logger('database', 'DEBUG')
    """
    logger = get_logger(module_name)

    if level is not None:
        numeric_level = _parse_log_level(level)
        logger.setLevel(numeric_level)

    if handlers is not None:
        logger.handlers.clear()
        for handler in handlers:
            logger.addHandler(handler)

    return logger


def shutdown_logging() -> None:
    """
    Корректно завершает работу системы логирования.

    Закрывает все handlers и сбрасывает буферы.
    """
    logging.shutdown()


# Функция для тестирования модуля
def _test_module() -> None:
    """Тестирует модуль логирования."""
    print('\nТестирование модуля logger.py...')
    print('=' * 50)

    # Настройка логгера
    logger = setup_logging(
        log_level='DEBUG', log_file='test_app.log', console_output=True, mask_sensitive=True
    )

    # Тестовые сообщения разных уровней
    logger.debug('Это DEBUG сообщение')
    logger.info('Это INFO сообщение')
    logger.warning('Это WARNING сообщение')
    logger.error('Это ERROR сообщение')
    logger.critical('Это CRITICAL сообщение')

    # Тест маскирования чувствительных данных
    logger.info('Подключение с password=secret123 и token=abc456')

    # Тест декоратора времени выполнения
    @log_execution_time
    def slow_function() -> str:
        import time

        time.sleep(0.1)
        return 'Готово'

    result = slow_function()
    logger.info(f'Результат функции: {result}')

    # Тест логирования исключения
    try:
        1 / 0
    except ZeroDivisionError:
        log_exception(logger, 'Тестовое исключение')

    # Тест контекстного логирования
    context_log = create_context_logger(logger, session='TEST001', user='admin')
    context_log('INFO', 'Операция выполнена успешно')

    print('\n' + '=' * 50)
    print('✓ Все тесты пройдены!')
    print('✓ Логи записаны в файл: test_app.log')

    # Очистка
    shutdown_logging()

    # Удаляем тестовый файл
    test_log = Path('test_app.log')
    if test_log.exists():
        test_log.unlink()
        print('✓ Тестовый файл удален')


if __name__ == '__main__':
    match sys.argv:
        case [_, '--test']:
            _test_module()
        case _:
            print('Использование:')
            print('  python logger.py --test  # Запустить тесты')
