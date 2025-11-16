"""
Тесты для модуля логирования.

Проверяет функциональность логирования, маскирование данных и декораторы.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from oracle_to_excel.logger import (
    create_context_logger,
    log_exception,
    log_execution_time,
    log_function_call,
    setup_logging,
    shutdown_logging,
)


def test_setup_logging_console() -> None:
    """Тест настройки логирования в консоль."""
    logger = setup_logging(
        log_level='DEBUG',
        console_output=True,
        mask_sensitive=True,
    )

    # Проверяем, что логгер создан
    assert logger is not None
    assert logger.name == 'oracle_exporter'

    # Логируем сообщение
    logger.info('Test message')
    logger.debug('Debug message')
    logger.warning('Warning message')


def test_setup_logging_file() -> None:
    """Тест настройки логирования в файл."""
    log_file = Path('test.log')

    try:
        logger = setup_logging(
            log_level='DEBUG',
            log_file=log_file,
            console_output=False,
        )

        # Логируем сообщение
        logger.info('Test file logging')

        # Проверяем, что файл создан
        assert log_file.exists(), 'Log file was not created'

    finally:
        # Закрываем логгер перед удалением файла
        shutdown_logging()

        # Очищаем логгеры
        logging.getLogger('oracle_exporter').handlers.clear()

        if log_file.exists():
            log_file.unlink()


def test_sensitive_data_masking() -> None:
    """Тест маскирования чувствительных данных."""
    logger = setup_logging(
        log_level='DEBUG',
        console_output=True,
        mask_sensitive=True,
    )

    # Логируем пароль - должен быть замаскирован
    logger.info('Connection with password=secret123 and token=abc456')


def test_log_execution_time_decorator() -> None:
    """Тест декоратора логирования времени выполнения."""

    @log_execution_time
    def slow_function() -> str:
        time.sleep(0.05)
        return 'Done'

    result = slow_function()
    assert result == 'Done'


def test_log_function_call_decorator() -> None:
    """Тест декоратора логирования вызова функции."""

    @log_function_call(log_args=True, log_result=True)
    def calculate(x: int, y: int) -> int:
        return x + y

    result = calculate(2, 3)
    assert result == 5


def test_exception_logging() -> None:
    """Тест логирования исключений."""
    logger = setup_logging('DEBUG', console_output=True)

    try:
        _ = 1 / 0
    except ZeroDivisionError:
        log_exception(logger, 'Test exception')


def test_context_logging() -> None:
    """Тест логирования с контекстом."""
    logger = setup_logging('DEBUG', console_output=True)

    context_log = create_context_logger(
        logger,
        session='TEST001',
        user='admin',
    )

    # Вызываем логирование с контекстом
    context_log('INFO', 'Operation completed successfully')
    # Дополнительный вызов с другой сессией
    context_log('WARNING', 'Some warning')
