"""
Модуль загрузки конфигурации из .env файла с использованием Pydantic.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Final, Mapping

from pydantic import Field, ValidationError, field_validator, model_validator
from pydantic_core import PydanticUseDefault
from pydantic_settings import BaseSettings, SettingsConfigDict

# Импортируем logger, если доступен
try:
    from .logger import get_logger, setup_logging

    LOGGER_AVAILABLE = True
except ImportError:
    LOGGER_AVAILABLE = False


# Допустимые типы БД
VALID_DB_TYPES: Final[frozenset[str]] = frozenset(
    {
        'oracle',
        'postgres',
        'postgresql',
        'sqlite',
        'sqlite3',
    }
)

# Централизованные default значения
DEFAULT_CONFIG: Mapping[str, int | str | bool] = {
    'LOG_LEVEL': 'INFO',
    'OUTPUT_DIR': './exports',
    'LOG_FILE': './logs/oracle_export.log',
    'FETCH_ARRAY_SIZE': 1000,
    'CHUNK_SIZE': 5000,
    'QUERY_TIMEOUT': 300,
    'MAX_COLUMN_WIDTH': 50,
    'COLUMN_WIDTH_SAMPLE_SIZE': 1000,
    'NULL_VALUE_REPLACEMENT': '',
    'WRAP_LONG_TEXT': True,
    'MAX_ROWS_PER_SHEET': 1000000,
    'ENABLE_BATCH_PROCESSING': True,
    'BATCH_SIZE': 50000,
    'SHOW_PROGRESS_BAR': True,
    'PROGRESS_UPDATE_INTERVAL': 100,
}

# Ключи с чувствительными данными
SENSITIVE_KEYS: Final[frozenset[str]] = frozenset(
    {
        'DB_CONNECT_URI',
        'PASSWORD',
        'SECRET',
        'TOKEN',
        'API_KEY',
        'PRIVATE_KEY',
    }
)


def _get_masked_value(value: Any) -> str:
    """
    Получает замаскированное значение.

    Args:
        value: Исходное значение

    Returns:
        Замаскированное значение
    """
    if value is None:
        return 'None'

    str_value = str(value)
    if len(str_value) <= 8:
        return '***'

    # Показываем первые 3 и последние 3 символа
    return f'{str_value[:3]}...{str_value[-3:]}'


class Settings(BaseSettings):
    """Конфигурация приложения из .env файла."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='forbid',
        case_sensitive=True,
        str_strip_whitespace=True,
    )

    # Database Configuration (обязательные параметры)
    db_type: str = Field(alias='DB_TYPE', description='Тип базы данных')
    db_connect_uri: str = Field(alias='DB_CONNECT_URI', description='URI для подключения к БД')
    lib_dir: Path | None = Field(
        default=None,
        alias='LIB_DIR',
        description='Путь к Oracle Instant Client (только для Oracle)',
    )

    # Database Settings (опциональные с дефолтами)
    fetch_array_size: int = Field(
        default=DEFAULT_CONFIG['FETCH_ARRAY_SIZE'],
        alias='FETCH_ARRAY_SIZE',
        description='Размер массива для fetch',
    )
    chunk_size: int = Field(
        default=DEFAULT_CONFIG['CHUNK_SIZE'], alias='CHUNK_SIZE', description='Размер чанка данных'
    )
    query_timeout: int | None = Field(
        default=DEFAULT_CONFIG.get('QUERY_TIMEOUT'),
        alias='QUERY_TIMEOUT',
        description='Таймаут запроса в секундах',
    )

    # Application Settings
    log_level: str = Field(
        default=DEFAULT_CONFIG['LOG_LEVEL'], alias='LOG_LEVEL', description='Уровень логирования'
    )
    log_file: Path = Field(
        default=Path(DEFAULT_CONFIG['LOG_FILE']), alias='LOG_FILE', description='Путь к файлу логов'
    )
    output_dir: Path = Field(
        default=Path(DEFAULT_CONFIG['OUTPUT_DIR']),
        alias='OUTPUT_DIR',
        description='Директория для экспорта',
    )

    # Excel Settings
    max_column_width: int = Field(
        default=DEFAULT_CONFIG['MAX_COLUMN_WIDTH'],
        alias='MAX_COLUMN_WIDTH',
        description='Максимальная ширина колонки в Excel',
    )
    column_width_sample_size: int = Field(
        default=DEFAULT_CONFIG['COLUMN_WIDTH_SAMPLE_SIZE'],
        alias='COLUMN_WIDTH_SAMPLE_SIZE',
        description='Размер выборки для вычисления ширины колонки',
    )
    null_value_replacement: str = Field(
        default=DEFAULT_CONFIG['NULL_VALUE_REPLACEMENT'],
        alias='NULL_VALUE_REPLACEMENT',
        description='Замена для NULL значений',
    )
    wrap_long_text: bool = Field(
        default=DEFAULT_CONFIG['WRAP_LONG_TEXT'],
        alias='WRAP_LONG_TEXT',
        description='Перенос длинного текста',
    )
    max_rows_per_sheet: int = Field(
        default=DEFAULT_CONFIG['MAX_ROWS_PER_SHEET'],
        alias='MAX_ROWS_PER_SHEET',
        description='Максимальное количество строк на лист',
    )

    # Performance Settings
    enable_batch_processing: bool = Field(
        default=DEFAULT_CONFIG['ENABLE_BATCH_PROCESSING'],
        alias='ENABLE_BATCH_PROCESSING',
        description='Включить пакетную обработку',
    )
    batch_size: int = Field(
        default=DEFAULT_CONFIG['BATCH_SIZE'], alias='BATCH_SIZE', description='Размер пакета'
    )
    show_progress_bar: bool = Field(
        default=DEFAULT_CONFIG['SHOW_PROGRESS_BAR'],
        alias='SHOW_PROGRESS_BAR',
        description='Показывать прогресс-бар',
    )
    progress_update_interval: int = Field(
        default=DEFAULT_CONFIG['PROGRESS_UPDATE_INTERVAL'],
        alias='PROGRESS_UPDATE_INTERVAL',
        description='Интервал обновления прогресса',
    )

    @field_validator('db_type')
    @classmethod
    def validate_db_type(cls, v: str) -> str:
        """Валидация типа БД."""
        if v not in VALID_DB_TYPES:
            raise ValueError(
                f'DB_TYPE должен быть одним из {sorted(VALID_DB_TYPES)}, получено: {v}'
            )
        return v

    @field_validator('query_timeout', mode='before')
    @classmethod
    def empty_str_to_none_optional(cls, v: Any) -> Any:
        """Конвертирует пустую строку в None для Optional полей."""
        if v == '':
            return None
        return v

    @field_validator(
        'fetch_array_size',
        'chunk_size',
        'max_column_width',
        'column_width_sample_size',
        'max_rows_per_sheet',
        'batch_size',
        'progress_update_interval',
        'enable_batch_processing',
        'show_progress_bar',
        'wrap_long_text',
        mode='before',
    )
    @classmethod
    def empty_str_use_default(cls, v: Any) -> Any:
        """Использует default значение для пустых строк."""
        if v == '':
            raise PydanticUseDefault()
        return v

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Валидация уровня логирования."""
        allowed_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        v_upper = v.upper()
        if v_upper not in allowed_levels:
            raise ValueError(f'LOG_LEVEL должен быть одним из {allowed_levels}, получено: {v}')
        return v_upper

    @model_validator(mode='after')
    def validate_oracle_lib_dir(self) -> Settings:
        """Проверка наличия LIB_DIR для Oracle."""
        if self.db_type == 'oracle' and self.lib_dir is None:
            raise ValueError('LIB_DIR обязателен для DB_TYPE=oracle')
        return self

    def model_dump_masked(
        self,
        *,
        mode: str = 'python',
        by_alias: bool = True,
    ) -> dict[str, Any]:
        """
        Возвращает словарь с замаскированными чувствительными данными.

        Args:
            mode: Режим сериализации ('python' или 'json')
            by_alias: Использовать alias имена полей

        Returns:
            Словарь с замаскированными чувствительными данными
        """
        # Получаем полный dump
        data = self.model_dump(mode=mode, by_alias=by_alias)

        masked_count = 0
        masked_data = {}

        for key, value in data.items():
            # Проверяем, содержит ли ключ чувствительные слова
            key_upper = key.upper()
            is_sensitive = any(sensitive in key_upper for sensitive in SENSITIVE_KEYS)

            if is_sensitive:
                # Сохраняем оригинал с префиксом
                masked_data[f'_original_{key}'] = value
                # Маскируем значение
                masked_data[key] = _get_masked_value(value)
                masked_count += 1
            else:
                masked_data[key] = value

        return masked_data

    def __repr__(self) -> str:
        """
        Строковое представление с маскированием чувствительных данных.

        Returns:
            Строковое представление модели
        """
        masked_data = self.model_dump_masked(by_alias=False)
        # Удаляем _original_ ключи для repr
        display_data = {k: v for k, v in masked_data.items() if not k.startswith('_original_')}

        fields_str = ', '.join(f'{k}={v!r}' for k, v in display_data.items())
        return f'{self.__class__.__name__}({fields_str})'


def get_settings() -> Settings:
    """
    Фабричная функция для получения настроек.

    Returns:
        Settings: Объект с настройками приложения

    Raises:
        ValidationError: Если .env файл содержит некорректные данные
    """
    # Настраиваем базовое логирование для ошибок загрузки конфигурации
    if LOGGER_AVAILABLE:
        # Используем минимальный уровень для bootstrap логирования
        logger = setup_logging(
            log_level='INFO',
            log_file=Path(DEFAULT_CONFIG['LOG_FILE']),
            logger_name='oracle_exporter.config',
            console_output=True,
            mask_sensitive=True,
        )
    else:
        # Fallback на стандартное логирование
        import logging

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(DEFAULT_CONFIG['LOG_FILE'], encoding='utf-8'),
            ],
        )
        logger = logging.getLogger('oracle_exporter.config')

    try:
        settings = Settings()
        logger.info('✅ Конфигурация успешно загружена из .env')

        # Логируем замаскированную конфигурацию
        masked_config = settings.model_dump_masked()
        logger.debug('Загруженная конфигурация:')
        for key, value in masked_config.items():
            if not key.startswith('_original_'):
                logger.debug('  %s: %s', key, value)

        return settings

    except ValidationError as e:
        logger.error('❌ Ошибка валидации конфигурации из .env файла', exc_info=True)
        logger.error('Детали ошибок валидации:')
        for error in e.errors():
            field = ' -> '.join(str(loc) for loc in error['loc'])
            logger.error('  Поле: %s | Тип: %s | Сообщение: %s', field, error['type'], error['msg'])
        raise

    except Exception as e:
        logger.error('❌ Неожиданная ошибка при загрузке конфигурации', exc_info=True)
        raise


def print_config_summary(
    config: Settings,
    *,
    mask_sensitive: bool = True,
    logger: logging.Logger | None = None,
) -> None:
    """
    Выводит краткую сводку конфигурации.

    Args:
        config: Объект Settings с конфигурацией.
        mask_sensitive: Маскировать ли чувствительные данные.
        logger: Логгер для вывода (если None, используется print).
    """
    sections = [
        ('База данных', ['DB_TYPE', 'DB_CONNECT_URI', 'LIB_DIR']),
        ('Параметры запросов', ['FETCH_ARRAY_SIZE', 'CHUNK_SIZE', 'QUERY_TIMEOUT']),
        (
            'Параметры Excel',
            [
                'MAX_COLUMN_WIDTH',
                'COLUMN_WIDTH_SAMPLE_SIZE',
                'NULL_VALUE_REPLACEMENT',
                'WRAP_LONG_TEXT',
                'MAX_ROWS_PER_SHEET',
            ],
        ),
        (
            'Производительность',
            [
                'ENABLE_BATCH_PROCESSING',
                'BATCH_SIZE',
                'SHOW_PROGRESS_BAR',
                'PROGRESS_UPDATE_INTERVAL',
            ],
        ),
        ('Прочее', ['LOG_LEVEL', 'LOG_FILE', 'OUTPUT_DIR']),
    ]

    # Получаем данные (замаскированные или обычные)
    if mask_sensitive:
        config_data = config.model_dump_masked(by_alias=True)
        # Убираем _original_ ключи из отображения
        config_data = {k: v for k, v in config_data.items() if not k.startswith('_original_')}
    else:
        config_data = config.model_dump(by_alias=True)

    if logger:
        # Используем отдельные вызовы logger для каждой строки
        _log_config_header(logger)

        for section_name, params in sections:
            _log_config_section(section_name, params, config_data, logger=logger)

        _log_config_footer(logger)
    else:
        _print_config_to_console(sections, config_data)


def _log_config_header(logger: logging.Logger) -> None:
    """Логирует заголовок сводки конфигурации."""
    logger.info('=' * 60)
    logger.info('КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ')
    logger.info('=' * 60)


def _log_config_section(
    section_name: str,
    params: list[str],
    config_data: dict[str, Any],
    *,
    logger: logging.Logger,
) -> None:
    """Логирует одну секцию конфигурации."""
    logger.info('')
    logger.info('%s:', section_name)
    logger.info('-' * 40)

    for param in params:
        value = config_data.get(param)
        if value is None:
            display_value = 'не задано'
        else:
            display_value = value
        logger.info('  %-28s %s', param, display_value)


def _log_config_footer(logger: logging.Logger) -> None:
    """Логирует подвал сводки конфигурации."""
    logger.info('')
    logger.info('=' * 60)


def _print_config_to_console(
    sections: list[tuple[str, list[str]]],
    config_data: dict[str, Any],
) -> None:
    """Выводит конфигурацию в консоль через print."""
    print()
    print('=' * 60)
    print('КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ')
    print('=' * 60)

    for section_name, params in sections:
        print()
        print(f'{section_name}:')
        print('-' * 40)

        for param in params:
            value = config_data.get(param)
            if value is None:
                display_value = 'не задано'
            else:
                display_value = value
            print(f'  {param:<28} {display_value}')

    print()
    print('=' * 60)
    print()


# Для удобного импорта
settings = get_settings()


if __name__ == '__main__':
    # Настройка логирования для теста
    logging.basicConfig(
        level=logging.INFO, format='%(message)s', handlers=[logging.StreamHandler(sys.stdout)]
    )
    logger = logging.getLogger(__name__)

    try:
        config = get_settings()

        # Вывод через print (для консоли)
        # print_config_summary(config, mask_sensitive=True)

        # Или вывод через logger (для логов)
        logger.info('\nВывод через logger:')
        print_config_summary(config, mask_sensitive=True, logger=logger)

        # Вывод в лог без маскировки (для отладки)
        print_config_summary(config, mask_sensitive=False, logger=logger)

    except ValidationError as e:
        print('\n❌ Ошибка валидации конфигурации\n')
        print('=' * 60)
        for error in e.errors():
            print(f'Поле: {" -> ".join(str(loc) for loc in error["loc"])}')
            print(f'Тип: {error["type"]}')
            print(f'Сообщение: {error["msg"]}')
            print('-' * 60)
        sys.exit(1)

    except Exception as e:
        print(f'\n❌ Неожиданная ошибка: {e}\n')
        sys.exit(1)
