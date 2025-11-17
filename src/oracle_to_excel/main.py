# PEP 649: Deferred evaluation annotations - новая фича Python 3.14
from __future__ import annotations

from pathlib import Path

from oracle_to_excel.config import load_config, print_config_summary
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
    return 0


if __name__ == '__main__':
    main()
