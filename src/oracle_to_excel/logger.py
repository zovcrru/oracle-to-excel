"""
Centralized logging system module for Oracle Excel Exporter.

Configures centralized logging with support for
file rotation and sensitive data filtering.
"""

from __future__ import annotations

import logging
import logging.handlers
import re
import sys
from collections.abc import Callable, Sequence
from functools import wraps
from pathlib import Path
from time import perf_counter
from typing import ParamSpec, TypeVar

# Type aliases for improved readability (Python 3.14+)
type LogLevel = str | int
type FilterFunc = Callable[[logging.LogRecord], bool]

# Generic types for decorators (PEP 695)
P = ParamSpec('P')
R = TypeVar('R')

# Constants
DEFAULT_LOG_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DEFAULT_DATE_FORMAT: str = '%Y-%m-%d %H:%M:%S'
MAX_LOG_FILE_SIZE: int = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT: int = 3

# Patterns for masking sensitive data
SENSITIVE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"password[\"']?\s*[:=]\s*[\"']?([^\"'\\s]+)", r'password=***'),
    (r"PASSWORD[\"']?\s*[:=]\s*[\"']?([^\"'\\s]+)", r'PASSWORD=***'),
    (r"token[\"']?\s*[:=]\s*[\"']?([^\"'\\s]+)", r'token=***'),
    (r"secret[\"']?\s*[:=]\s*[\"']?([^\"'\\s]+)", r'secret=***'),
    (r"apikey[\"']?\s*[:=]\s*[\"']?([^\"'\\s]+)", r'apikey=***'),
)


def setup_logging(
    log_level: LogLevel = 'INFO',
    log_file: str | Path | None = None,
    logger_name: str = 'oracle_exporter',
    *,
    console_output: bool = True,
    mask_sensitive: bool = True,
) -> logging.Logger:
    """
    Configure logging system with console and file support.

    Uses enhanced Python 3.14 type system and pattern matching
    for flexible logger configuration.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Path to log file (optional).
        logger_name: Logger name.
        console_output: Whether to output logs to console.
        mask_sensitive: Whether to mask sensitive data.

    Returns:
        Configured Logger object.

    Example:
        >>> logger = setup_logging('DEBUG', 'app.log')
        >>> logger.info('Application started')
    """
    # Get or create logger
    logger = logging.getLogger(logger_name)

    # Clear existing handlers (avoid duplication)
    logger.handlers.clear()

    # Set logging level
    numeric_level = _parse_log_level(log_level)
    logger.setLevel(numeric_level)

    # Create formatter
    formatter = _create_formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)

    # Pattern matching for handler configuration (improved in Python 3.14)
    match (console_output, log_file):
        case (True, None):
            # console only
            _add_console_handler(logger, formatter)
        case (False, str() | Path() as file):
            # file only
            _add_file_handler(logger, formatter, file)
        case (True, str() | Path() as file):
            # both console and file
            _add_console_handler(logger, formatter)
            _add_file_handler(logger, formatter, file)
        case (False, None):
            # Fallback: at least console
            _add_console_handler(logger, formatter)
            logger.warning('Logging not configured properly, using console')

    # Add filter for masking sensitive data
    if mask_sensitive:
        logger.addFilter(_create_sensitive_filter())

    logger.debug(
        'Logger %r configured with level %s',
        logger_name,
        logging.getLevelName(numeric_level),
    )

    return logger


def _parse_log_level(level: LogLevel) -> int:
    """
    Convert string logging level to numeric.

    Uses pattern matching for handling various formats.

    Args:
        level: Logging level (string or number).

    Returns:
        Numeric logging level.

    Raises:
        ValueError: If level is invalid.
    """
    match level:
        case int() as numeric_level if numeric_level in {
            0,
            10,
            20,
            30,
            40,
            50,
        }:
            return numeric_level
        case str() as string_level:
            upper_level = string_level.upper()
            if hasattr(logging, upper_level):
                return getattr(logging, upper_level)
            raise ValueError(f'Invalid logging level: {level}')
        case _:
            raise ValueError(f'Unsupported logging level type: {type(level)}')


def _create_formatter(
    fmt: str,
    datefmt: str,
) -> logging.Formatter:
    """
    Create formatter for logs.

    Args:
        fmt: Message format.
        datefmt: Date and time format.

    Returns:
        Formatter object.
    """
    return logging.Formatter(fmt=fmt, datefmt=datefmt)


def _add_console_handler(
    logger: logging.Logger,
    formatter: logging.Formatter,
) -> None:
    """
    Add console handler to logger.

    Args:
        logger: Logger to configure.
        formatter: Formatter for handler.
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Colored console output (if terminal supports it)
    if _supports_color():
        console_handler.setFormatter(_create_colored_formatter())

    logger.addHandler(console_handler)


def _add_file_handler(
    logger: logging.Logger,
    formatter: logging.Formatter,
    log_file: str | Path,
) -> None:
    """
    Add rotating file handler to logger.

    Args:
        logger: Logger to configure.
        formatter: Formatter for handler.
        log_file: Path to log file.
    """
    file_path = Path(log_file)

    # Create directory if it doesn't exist
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Rotating handler for automatic file rotation
    file_handler = logging.handlers.RotatingFileHandler(
        filename=file_path,
        maxBytes=MAX_LOG_FILE_SIZE,
        backupCount=BACKUP_COUNT,
        encoding='utf-8',
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def _supports_color() -> bool:
    """
    Check if terminal supports colored output.

    Returns:
        True if supported, False otherwise.
    """
    return (
        hasattr(sys.stdout, 'isatty')
        and sys.stdout.isatty()
        and sys.platform != 'win32'  # Windows requires extra setup
    )


def _create_colored_formatter() -> logging.Formatter:
    """
    Create colored formatter for console.

    Returns:
        Formatter with ANSI escape codes for colors.
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
                colored = f'{colors[levelname]}{levelname}{colors["RESET"]}'
                original = record.levelname
                try:
                    record.levelname = colored
                    return super().format(record)
                finally:
                    record.levelname = original
            return super().format(record)

    return ColoredFormatter(
        fmt=DEFAULT_LOG_FORMAT,
        datefmt=DEFAULT_DATE_FORMAT,
    )


def _create_sensitive_filter() -> logging.Filter:
    """Create a logging.Filter instance that masks sensitive data.

    Returns:
        Instance of logging.Filter implementing .filter(record).
    """
    compiled_patterns = [
        (re.compile(pattern, re.IGNORECASE), replacement)
        for pattern, replacement in SENSITIVE_PATTERNS
    ]

    class SensitiveDataFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            original_msg = record.getMessage()
            filtered_msg = original_msg
            for pattern, replacement in compiled_patterns:
                filtered_msg = pattern.sub(replacement, filtered_msg)

            if filtered_msg != original_msg:
                record.msg = filtered_msg
                record.args = ()

            return True

    return SensitiveDataFilter()


def log_execution_time[**P, R](
    func: Callable[P, R],
) -> Callable[P, R]:
    """
    Decorator to log function execution time.

    Uses new generic function syntax in Python 3.14 (PEP 695).

    Args:
        func: Function to decorate.

    Returns:
        Wrapped function with timing logs.

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

        logger.debug(
            'Starting execution: %s.%s',
            module_name,
            func_name,
        )
        start_time = perf_counter()

        try:
            result = func(*args, **kwargs)
        except Exception:
            elapsed_time = perf_counter() - start_time
            logger.exception(
                'Error in %s.%s after %.4fs',
                module_name,
                func_name,
                elapsed_time,
            )
            raise
        else:
            elapsed_time = perf_counter() - start_time
            logger.info(
                'Completed: %s.%s (time: %.4fs)',
                module_name,
                func_name,
                elapsed_time,
            )
            return result

    return wrapper


def log_function_call[**P, R](
    *,
    log_args: bool = True,
    log_result: bool = False,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator for detailed function call logging.

    Args:
        log_args: Whether to log function arguments.
        log_result: Whether to log execution result.

    Returns:
        Decorator for function.

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

            # Log call
            if log_args:
                args_repr = ', '.join(repr(arg) for arg in args)
                kwargs_repr = ', '.join(f'{k}={v!r}' for k, v in kwargs.items())
                all_args = ', '.join(filter(None, [args_repr, kwargs_repr]))
                logger.debug(
                    'Call: %s(%s)',
                    func_name,
                    all_args,
                )
            else:
                logger.debug('Call: %s', func_name)

            # Execute function
            result = func(*args, **kwargs)

            # Log result
            if log_result:
                logger.debug(
                    'Result %s: %r',
                    func_name,
                    result,
                )

            return result

        return wrapper

    return decorator


def get_logger(
    name: str | None = None,
) -> logging.Logger:
    """
    Get logger by name.

    Args:
        name: Logger name. If None, returns root app logger.

    Returns:
        Logger object.

    Example:
        >>> logger = get_logger('oracle_exporter.database')
        >>> logger.info('Connection established')
    """
    if name is None:
        return logging.getLogger('oracle_exporter')

    # Add prefix if not present
    if not name.startswith('oracle_exporter.'):
        name = f'oracle_exporter.{name}'

    return logging.getLogger(name)


def log_exception(
    logger: logging.Logger | None = None,
    message: str = 'An error occurred',
    level: int = logging.ERROR,
) -> None:
    """
    Log current exception with traceback.

    Args:
        logger: Logger to use. If None, uses root.
        message: Error message.
        level: Logging level.

    Example:
        >>> from oracle_to_excel.logger import get_logger
        >>> logger = get_logger()
        >>> try:  # doctest: +SKIP
        ...     1 / 0
        ... except ZeroDivisionError:
        ...     log_exception(logger, 'Operation failed')
    """
    if logger is None:
        logger = get_logger()

    logger.log(level, message, exc_info=True)


def create_context_logger(
    logger: logging.Logger,
    **context: str | int | float,
) -> Callable[
    [str, str],
    None,
]:
    """
    Create logging function with additional context.

    Useful for adding metadata to all messages (session_id, user_id etc).

    Args:
        logger: Base logger.
        **context: Additional context fields.

    Returns:
        Logging function with context.

    Example:
        >>> logger = get_logger()
        >>> log_ctx = create_context_logger(
        ...     logger,
        ...     session='SES001',
        ...     user='admin',
        ... )
        >>> log_ctx('INFO', 'Processing data')
    """

    def log_with_context(
        level: str,
        message: str,
        **extra: str | int | float,
    ) -> None:
        # Merge context and extra parameters
        full_context = {**context, **extra}
        context_str = ' | '.join(f'{k}={v}' for k, v in full_context.items())
        full_message = f'[{context_str}] {message}'

        # Log with level
        numeric_level = _parse_log_level(level)
        logger.log(numeric_level, full_message)

    return log_with_context


def configure_module_logger(
    module_name: str,
    level: LogLevel | None = None,
    handlers: Sequence[logging.Handler] | None = None,
) -> logging.Logger:
    """
    Configure logger for specific module.

    Args:
        module_name: Module name.
        level: Logging level (optional).
        handlers: List of handlers (optional).

    Returns:
        Configured module logger.

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
    Properly shutdown logging system.

    Closes all handlers and flushes buffers.
    """
    logging.shutdown()
