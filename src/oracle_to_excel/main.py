# PEP 649: Deferred evaluation annotations - новая фича Python 3.14
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import cast

from oracle_to_excel.config import ConfigDict, load_config, print_config_summary
from oracle_to_excel.database import (
    DBType,
    get_connection,
    get_db_info,
    validate_connection_string,
)
from oracle_to_excel.logger import setup_logging


def _load_config() -> ConfigDict | None:
    try:
        return load_config()
    except (FileNotFoundError, ValueError) as e:
        print(f'Ошибка при загрузке конфигурации: {e}')
        return None


def _setup_logger_from_config(config: ConfigDict) -> logging.Logger:
    log_level_raw = config.get('LOG_LEVEL')
    log_file_raw = config.get('LOG_FILE')

    log_level = log_level_raw if isinstance(log_level_raw, str) else 'INFO'
    log_file_path = (
        Path(log_file_raw) if isinstance(log_file_raw, str) else Path('.logs/oracle_export.log')
    )

    return setup_logging(log_level=log_level, log_file=log_file_path)


def _connect_and_log(conn_str: str, db_t: DBType | None, logger: logging.Logger) -> bool:
    try:
        with get_connection(
            connection_string=conn_str,
            db_type=db_t,
            read_only=True,
            timeout=30,
        ) as connection:
            logger.info('Подключение к БД установлено')
            final_db_type = cast(DBType, db_t or 'oracle')
            db_info = get_db_info(connection, final_db_type)
            logger.info('Информация о БД: %s', db_info)
            return True
    except Exception:
        logger.exception('Ошибка при подключении к БД')
        return False


def main() -> int:
    """
    Основная точка входа приложения.

    Загружает конфигурацию, настраивает логирование и выводит сводку.

    Returns:
        Код выхода (0 для успеха, 1 для ошибки).
    """
    config = _load_config()
    if config is None:
        return 1

    logger = _setup_logger_from_config(config)
    logger.info('Конфигурация загружена.')
    print_config_summary(config, logger=logger)

    db_type_raw = config.get('DB_TYPE')
    db_type = cast(DBType | None, db_type_raw) if isinstance(db_type_raw, str) else None
    connection_string = config.get('DB_CONNECT_URI')

    if not connection_string or not isinstance(connection_string, str):
        logger.error('Отсутствует или некорректен параметр DB_CONNECT_URI')
        return 1

    is_valid, error_message = validate_connection_string(connection_string)
    if not is_valid:
        logger.error('Невалидный connection string: %s', error_message)
        return 1

    logger.info('Connection string %s прошел валидацию', connection_string)

    if not _connect_and_log(connection_string, db_type, logger):
        return 1

    logger.info('Работа завершена успешно')
    return 0


if __name__ == '__main__':
    sys.exit(main())
