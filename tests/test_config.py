"""
Тесты для модуля конфигурации.

Проверяет загрузку, валидацию и управление конфигурацией приложения.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from oracle_to_excel.config import (
    create_env_example,
    load_config,
    print_config_summary,
    restore_sensitive_data,
    validate_config,
)
from oracle_to_excel.logger import setup_logging


def test_load_config_success() -> None:
    """Тест успешной загрузки конфигурации."""
    logger = setup_logging('DEBUG', console_output=True)
    logger.info('Тестирование модуля config.py...')

    # Создаем тестовый .env файл
    test_env = Path('.env.test')
    test_env.write_text(
        """
DB_TYPE = "sqlite"
DB_CONNECT_URI = "sqlite:///lice.sqlite3"
# Application Settings
LOG_LEVEL=DEBUG
LOG_FILE=./logs/oracle_export.log
OUTPUT_DIR=./exports
"""
    )

    try:
        # Загружаем конфигурацию
        config = load_config('.env.test')
        logger.info('✓ Конфигурация загружена')

        # Валидируем
        valid, errors = validate_config(config, logger)
        if valid:
            logger.info('Конфигурация валидна')
        else:
            logger.error(
                'Ошибки валидации: %d',
                len(errors),
            )

        # Выводим сводку
        print_config_summary(config, logger=logger)

        # Восстанавливаем чувствительные данные
        restore_sensitive_data(cast(dict[str, str | int], config), logger)
        logger.info('✓ Чувствительные данные восстановлены')

        # Проверяем, что конфигурация загружена
        if config.get('DB_TYPE') == 'oracle':
            assert config.get('ORACLE_USER') == 'test_user'
            assert config.get('ORACLE_PASSWORD') == 'test_pass'

    finally:
        # Удаляем тестовый файл
        if test_env.exists():
            test_env.unlink()
            logger.info('✓ Тестовый файл удален')

        # Удаляем тестовую директорию
        test_dir = Path('./test_exports')
        if test_dir.exists():
            test_dir.rmdir()
            logger.info('✓ Тестовая директория удалена')


def test_validate_config() -> None:
    """Тест валидации конфигурации."""
    logger = setup_logging('DEBUG', console_output=True)

    # Создаем тестовый .env файл
    test_env = Path('.env.test')
    test_env.write_text(
        """
DB_TYPE = "sqlite"
DB_CONNECT_URI = "sqlite:///lice.sqlite3"
# Application Settings
LOG_LEVEL=DEBUG
LOG_FILE=./logs/oracle_export.log
OUTPUT_DIR=./exports
"""
    )

    try:
        config = load_config('.env.test')
        valid, errors = validate_config(config, logger)

        assert valid, f'Конфигурация должна быть валидна, ошибки: {errors}'
        assert len(errors) == 0

    finally:
        # Удаляем тестовый файл
        if test_env.exists():
            test_env.unlink()

        # Удаляем тестовую директорию
        test_dir = Path('./test_exports')
        if test_dir.exists():
            test_dir.rmdir()


def test_create_env_example() -> None:
    """Тест создания примера .env файла."""
    output_file = Path('.env.example.test')

    try:
        create_env_example(str(output_file))
        assert output_file.exists(), 'Файл .env.example не создан'
        assert output_file.stat().st_size > 0, 'Файл .env.example пуст'

    finally:
        if output_file.exists():
            output_file.unlink()
