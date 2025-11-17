# PEP 649: Deferred evaluation annotations - новая фича Python 3.14
from __future__ import annotations

import sys
from pathlib import Path

from oracle_to_excel.config import load_config, print_config_summary
from oracle_to_excel.logger import setup_logging


def main():
    try:
        # Загружаем конфигурацию из файла .env с логированием событий загрузки
        config = load_config()
    except (FileNotFoundError, ValueError) as e:
        print(f'Ошибка при загрузке конфигурации: {e}')
        sys.exit(1)

    # Настраиваем логирование в соответствии с конфигом
    log_level = config.get('LOGLEVEL', 'INFO')
    print(log_level, type(log_level))
    log_file = Path(config.get('LOGFILE', '.logs/oracle_export.log'))

    logger = setup_logging(
        log_level=log_level,
        log_file=log_file,
    )

    logger.info('Конфигурация загружена.')
    print_config_summary(config, logger=logger)


if __name__ == '__main__':
    main()
