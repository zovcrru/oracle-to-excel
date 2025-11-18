# PEP 649: Deferred evaluation annotations - новая фича Python 3.14
from __future__ import annotations

from pathlib import Path

from oracle_to_excel.config import load_config, print_config_summary
from oracle_to_excel.database import get_connection, get_db_info, validate_connection_string
from oracle_to_excel.logger import setup_logging


def main() -> int:
    """
    Основная точка входа приложения.

    Загружает конфигурацию, настраивает логирование и выводит сводку.

    Returns:
        Код выхода (0 для успеха, 1 для ошибки).
    """
    try:
        # Загружаем конфигурацию из файла .env с логированием событий загрузки
        config = load_config()
    except (FileNotFoundError, ValueError) as e:
        print(f'Ошибка при загрузке конфигурации: {e}')
        return 1

    # Настраиваем логирование в соответствии с конфигом
    log_level_raw = config.get('LOG_LEVEL')
    log_file_raw = config.get('LOG_FILE')

    # Приводим типы к нужным значениям
    log_level = log_level_raw if isinstance(log_level_raw, str) else 'INFO'
    log_file_path = (
        Path(log_file_raw) if isinstance(log_file_raw, str) else Path('.logs/oracle_export.log')
    )

    logger = setup_logging(
        log_level=log_level,
        log_file=log_file_path,
    )

    logger.info('Конфигурация загружена.')
    print_config_summary(config, logger=logger)
    # Получаем параметры подключения к БД
    db_type = config.get('DB_TYPE')
    connection_string = config.get('DB_CONNECT_URI')

    if not connection_string or not isinstance(connection_string, str):
        logger.error('Отсутствует или некорректен параметр DB_CONNECT_URI')
        return 1

    # Валидируем connection string
    is_valid, error_message = validate_connection_string(connection_string)
    if not is_valid:
        logger.error('Невалидный connection string: %s', error_message)
        return 1

    logger.info('Connection string %s прошел валидацию', connection_string)

    # Подключаемся к БД и получаем информацию
    try:
        with get_connection(
            connection_string=connection_string,
            db_type=db_type,
            read_only=True,
            timeout=30,
        ) as connection:
            logger.info('Подключение к БД установлено')

            # Получаем информацию о БД
            db_info = get_db_info(connection, db_type or 'oracle')
            logger.info('Информация о БД: %s', db_info)

            # Здесь можно добавить дополнительную логику работы с БД

    except Exception as e:
        logger.error('Ошибка при подключении к БД: %s', e)
        return 1

    logger.info('Работа завершена успешно')
    return 0


if __name__ == '__main__':
    main()
