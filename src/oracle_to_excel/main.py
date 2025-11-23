"""Main entrypoint for oracle_to_excel tool."""

import logging
import sys
from pathlib import Path
from typing import cast

from oracle_to_excel.database import (
    DBType,
    get_connection,
    get_db_info,
)
from oracle_to_excel.env_config import (
    DEFAULT_CONFIG,
    Settings,
    load_config,
    print_config_summary,
)
from oracle_to_excel.logger import setup_logging


def _load_config() -> Settings | None:
    """Загружает конфигурацию из .env файла."""
    try:
        return load_config()
    except (FileNotFoundError, ValueError):
        logger = logging.getLogger('oracle_exporter.main')
        logger.error('Ошибка при загрузке конфигурации')  # noqa: TRY400
        return None


def _setup_logger_from_config(config: Settings) -> logging.Logger:
    """Настраивает логгер на основе конфигурации."""
    log_level = config.log_level
    log_file_path = Path(config.log_file)

    return setup_logging(log_level=log_level, log_file=log_file_path)


def _setup_logger_from_default(path: Path) -> logging.Logger:
    """Настраивает логгер с дефолтными настройками."""
    return setup_logging(log_level=logging.ERROR, log_file=path)


def main() -> None:
    """Основная точка входа приложения."""
    # Создаем логгер с дефолтным лог-файлом сразу
    default_logfile = Path(DEFAULT_CONFIG['LOG_FILE'])
    logger = _setup_logger_from_default(default_logfile)

    # 1. Загружаем конфигурацию
    config = _load_config()
    if config is None:
        logger.error(
            'Не удалось загрузить конфигурацию. Завершение.',
        )
        sys.exit(1)

    # 2. Пересоздаем логгер с параметрами из конфига (если путь к логу или log_level отличаются)
    logfile_path = Path(config.log_file)
    if (
        logfile_path != default_logfile
        or logging.getLevelName(logger.getEffectiveLevel()) != config.log_level
    ):
        logger = _setup_logger_from_config(config)
        logger.info('Логгер перенастроен по конфигу')
    logger.info('Запуск приложения oracle_to_excel')

    # 3. Выводим сводку конфигурации (теперь с logger!)
    print_config_summary(config, logger=logger)

    # 4. Нормализуем db_type к DBType
    db_type = cast(DBType, config.db_type)

    # 5. Валидируем connection string
    if config._original_db_connect_uri:
        connection_string = config._original_db_connect_uri
    else:
        connection_string = config.db_connect_uri

    # 6. Получаем информацию о БД
    logger.info('Подключение к %s БД...', db_type)
    try:
        # Передаём lib_dir только для Oracle
        # Для PostgreSQL и SQLite config.lib_dir будет None (игнорируется)
        with get_connection(
            connection_string, db_type, lib_dir=config.lib_dir if db_type == 'oracle' else None
        ) as conn:
            db_info = get_db_info(conn, db_type)
            logger.info('Подключение успешно: %s', db_info)
            print(f'\n✓ Подключение к {db_type} успешно установлено')
            print(f'  База данных: {db_info.get("database", "N/A")}')
            print(f'  Версия: {db_info.get("version", "N/A")}')
            print(f'  Пользователь: {db_info.get("user", "N/A")}')

    except Exception:
        logger.exception('Ошибка при подключении к БД')
        sys.exit(1)

    logger.info('Приложение завершено успешно')


if __name__ == '__main__':
    main()
