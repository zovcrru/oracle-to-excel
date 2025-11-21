"""Модуль загрузки конфигурации из .env файла с использованием Pydantic."""
# ruff: noqa
import re
import logging

import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from pydantic import Field, ValidationError, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from urllib.parse import urlparse
# Импортируем setup_logging, если доступен
try:
    from .logger import get_logger
    LOGGER_AVAILABLE = True
except ImportError:
    LOGGER_AVAILABLE = False

# from .database import check_non_empty_string,ConnectionString,try_parse_url,check_url_parts,
# Допустимые типы БД
VALID_DB_TYPES: Final[frozenset[str]] = frozenset(
    (
        'oracle',
        'postgres',
        'postgresql',
        'sqlite',
        'sqlite3',
    )
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
    'NULL_VALUE_REPLACEMENT': '',
    'WRAP_LONG_TEXT': True,
    'MAX_ROWS_PER_SHEET': 1_000_000,
    'ENABLE_BATCH_PROCESSING': False,
    'BATCH_SIZE': 50_000,
    'SHOW_PROGRESS_BAR': True,
    'PROGRESS_UPDATE_INTERVAL': 100,
}


class Settings(BaseSettings):
    """Pydantic Settings для загрузки конфигурации из .env."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore',
    )

    # === Обязательные параметры ===
    db_type: str = Field(..., description='Тип базы данных: oracle, postgres, sqlite')
    db_connect_uri: str = Field(..., description='Connection string для БД')

    # === Опциональные параметры с дефолтами ===
    lib_dir: str | None = Field(None, description='Путь к Oracle Instant Client (только для Oracle)')

    # Логирование
    log_level: str = Field(default=DEFAULT_CONFIG['LOG_LEVEL'], description='Уровень логирования')
    log_file: str = Field(default=DEFAULT_CONFIG['LOG_FILE'], description='Путь к файлу логов')

    # Экспорт
    output_dir: str = Field(default=DEFAULT_CONFIG['OUTPUT_DIR'], description='Директория для экспорта')

    # Производительность БД
    fetch_array_size: int = Field(default=DEFAULT_CONFIG['FETCH_ARRAY_SIZE'], ge=1, description='Размер массива для fetchmany()')
    chunk_size: int = Field(default=DEFAULT_CONFIG['CHUNK_SIZE'], ge=1, description='Размер чанка для обработки')
    query_timeout: int = Field(default=DEFAULT_CONFIG['QUERY_TIMEOUT'], ge=0, description='Таймаут запроса (секунды)')

    # Excel параметры
    max_column_width: int = Field(default=DEFAULT_CONFIG['MAX_COLUMN_WIDTH'], ge=1, description='Макс. ширина колонки')
    null_value_replacement: str = Field(default=DEFAULT_CONFIG['NULL_VALUE_REPLACEMENT'], description='Замена для NULL')
    wrap_long_text: bool = Field(default=DEFAULT_CONFIG['WRAP_LONG_TEXT'], description='Перенос длинного текста')
    max_rows_per_sheet: int = Field(default=DEFAULT_CONFIG['MAX_ROWS_PER_SHEET'], ge=1, description='Макс. строк на лист')

    # Батч обработка
    enable_batch_processing: bool = Field(default=DEFAULT_CONFIG['ENABLE_BATCH_PROCESSING'], description='Включить батч обработку')
    batch_size: int = Field(default=DEFAULT_CONFIG['BATCH_SIZE'], ge=1, description='Размер батча')
    show_progress_bar: bool = Field(default=DEFAULT_CONFIG['SHOW_PROGRESS_BAR'], description='Показывать прогресс бар')
    progress_update_interval: int = Field(default=DEFAULT_CONFIG['PROGRESS_UPDATE_INTERVAL'], ge=1, description='Интервал обновления прогресса')

    # Приватное поле для хранения оригинального connection string
    _original_db_connect_uri: str | None = None

    @field_validator(
        'chunk_size',
        'query_timeout',
        'max_column_width',
        'fetch_array_size',
        'batch_size',
        'max_rows_per_sheet',
        'progress_update_interval',
        mode='before',
    )
    @classmethod
    def parse_empty_int(cls, v: Any, info) -> Any:
        """Преобразует пустые строки в дефолтные значения для int полей."""
        if v == '' or v is None:
            field_name = info.field_name
            default_value = DEFAULT_CONFIG.get(field_name.upper())
            return default_value if default_value is not None else v
        return v

    @field_validator('enable_batch_processing', 'show_progress_bar', 'wrap_long_text', mode='before')
    @classmethod
    def parse_empty_bool(cls, v: Any, info) -> Any:
        """Преобразует пустые строки и строковые bool в дефолтные значения."""
        if v == '' or v is None:
            field_name = info.field_name
            default_value = DEFAULT_CONFIG.get(field_name.upper())
            return default_value if default_value is not None else v
        # Парсинг строковых boolean
        if isinstance(v, str):
            lower_v = v.lower().strip()
            if lower_v in ('true', '1', 'yes', 'on'):
                return True
            if lower_v in ('false', '0', 'no', 'off'):
                return False
        return v

    @field_validator('null_value_replacement', 'log_level', 'log_file', 'output_dir', mode='before')
    @classmethod
    def parse_empty_str(cls, v: Any, info) -> Any:
        """Преобразует пустые строки в дефолтные значения для str полей."""
        if v == '' or v is None:
            field_name = info.field_name
            default_value = DEFAULT_CONFIG.get(field_name.upper())
            return default_value if default_value is not None else ''
        return v

    @field_validator('db_type')
    @classmethod
    def normalize_db_type(cls, v: str) -> str:
        """Нормализует и валидирует db_type."""
        if not v or v == '':
            raise ValueError("DB_TYPE не может быть пустым")

        normalized = v.strip().lower()
        if normalized not in VALID_DB_TYPES:
            raise ValueError(
                f"Недопустимый DB_TYPE='{v}'. Допустимые значения: {', '.join(sorted(VALID_DB_TYPES))}"
            )
        # Приводим к каноническому виду
        if normalized in ('postgres', 'postgresql'):
            return 'postgresql'
        if normalized in ('sqlite', 'sqlite3'):
            return 'sqlite'
        return normalized


    @field_validator('db_connect_uri')
    @classmethod
    def validate_db_connect_uri(cls, v: str, info: ValidationInfo) -> str:
        if not v or v.strip() == '':
            raise ValueError("DB_CONNECT_URI не может быть пустым")
        uri = v.strip()
        db_type = info.data.get('db_type', '').lower()
        if not db_type:
            return uri
        if db_type in ('postgres', 'postgresql'):
            db_type = 'postgresql'
        elif db_type in ('sqlite', 'sqlite3'):
            db_type = 'sqlite'

        def mask_uri(uri: str) -> str:
            if not uri:
                return uri
            # 1. Стандартный случай - username:password@host
            uri = re.sub(r'([a-zA-Z0-9_.-]+):([^\s@:/]+)@', r'\1:***@', uri)
            # 2. Кривой случай - username:passwordhost:port (нет @, host сразу после password)
            uri = re.sub(r'([a-zA-Z0-9_.-]+):([^\s@:/]+)([a-zA-Z0-9_.-]+):(\d+)', r'\1:***\3:\4', uri, count=1)
            # 3. Кривой вариант - username:passwordhost
            uri = re.sub(r'([a-zA-Z0-9_.-]+):([^\s@:/]+)([a-zA-Z0-9_.-]+)', r'\1:***\3', uri, count=1)
            # 4. Одиночный вариант username:password
            uri = re.sub(r'([a-zA-Z0-9_.-]+):([^\s@:/]+)$', r'\1:***', uri)
            return uri

        # SQLite
        if db_type == 'sqlite':
            if uri.startswith('sqlite://'):
                try:
                    parsed = urlparse(uri)
                    if parsed.scheme != 'sqlite':
                        raise ValueError(f"Неверная схема для SQLite URI: {parsed.scheme}")
                except Exception as e:
                    raise ValueError(f"Некорректный SQLite URI: {e}")
            elif uri == ':memory:':
                pass
            else:
                if '://' in uri:
                    raise ValueError(
                        f"Для SQLite ожидается путь к файлу или 'sqlite:///', получено: {mask_uri(uri)}"
                    )
            return uri

        # Oracle/PostgreSQL: дополнительная проверка символа @!
        if db_type in ('oracle', 'postgresql'):
            if '://' in uri:
                rest = uri.split('://', 1)[1]
                if ':' in rest and '@' not in rest:
                    raise ValueError(
                        f"URI для {db_type.upper()} должен содержать символ '@' для разделения "
                        f"credentials (user:password) и хоста.\n"
                        f"Формат: {db_type}://username:password@hostname:port/database\n"
                        f"Получено: {mask_uri(uri)}"
                    )
                try:
                    parsed = urlparse(uri)
                    # Схема
                    if db_type == 'oracle' and parsed.scheme not in ('oracle', 'oracle+cx_oracle', 'oracle+oracledb'):
                        raise ValueError(
                            f"Неверная схема для Oracle URI: '{parsed.scheme}'. "
                            f"Ожидается: oracle, oracle+cx_oracle, или oracle+oracledb. "
                            f"URI: {mask_uri(uri)}"
                        )
                    if db_type == 'postgresql' and parsed.scheme not in (
                        'postgres', 'postgresql', 'postgresql+psycopg2', 'postgresql+psycopg'):
                        raise ValueError(
                            f"Неверная схема для PostgreSQL URI: '{parsed.scheme}'. "
                            f"Ожидается: postgres, postgresql, postgresql+psycopg2, или postgresql+psycopg. "
                            f"URI: {mask_uri(uri)}"
                        )
                    if not parsed.hostname:
                        raise ValueError(f"URI не содержит hostname: {mask_uri(uri)}")
                    # Проверка порта
                    if not parsed.port:
                        default_port = 1521 if db_type == 'oracle' else 5432
                        raise ValueError(
                            f"URI не содержит порт. "
                            f"Укажите порт явно (стандартный для {db_type.upper()}: {default_port}). "
                            f"URI: {mask_uri(uri)}"
                        )
                    if db_type == 'postgresql' and not parsed.path.lstrip('/'):
                        raise ValueError(f"PostgreSQL URI не содержит имя базы данных: {mask_uri(uri)}")
                except ValueError:
                    raise
                except Exception as e:
                    raise ValueError(
                        f"Некорректный URI для {db_type.upper()}: {type(e).__name__}: {str(e)}\n"
                        f"URI: {mask_uri(uri)}"
                    ) from e
            else:
                if db_type == 'oracle':
                    if not uri.replace('_', '').replace('-', '').isalnum():
                        raise ValueError(
                            f"Для Oracle ожидается URI (oracle://...) или TNS name, получено: {mask_uri(uri)}"
                        )
                else:
                    raise ValueError(
                        f"Для PostgreSQL требуется URI со схемой (postgresql://...), получено: {mask_uri(uri)}"
                    )
            return uri

        return uri

    @model_validator(mode='after')
    def validate_oracle_lib_dir(self) -> Settings:
        """Проверяет наличие lib_dir для Oracle."""
        if self.db_type == 'oracle' and not self.lib_dir:
            raise ValueError("LIB_DIR обязателен для DB_TYPE='oracle'")
        return self

    def model_post_init(self, __context: Any) -> None:
        """Сохраняем оригинальный connection string после инициализации."""
        self._original_db_connect_uri = self.db_connect_uri

    def model_dump_masked(self) -> dict[str, Any]:
        """Возвращает словарь с замаскированным db_connect_uri."""
        data = self.model_dump()
        if self.db_connect_uri:
            data['db_connect_uri'] = self._mask_connection_string(self.db_connect_uri)
        return data

    @staticmethod
    def _mask_connection_string(uri: str) -> str:
        """Маскирует пароль в connection string."""
        if '://' not in uri:
            return uri
        try:
            scheme, rest = uri.split('://', 1)
            if '@' in rest:
                creds, host_part = rest.split('@', 1)
                if ':' in creds:
                    user, _ = creds.split(':', 1)
                    return f"{scheme}://{user}:***@{host_part}"
            return uri
        except Exception:
            return uri


def load_config(env_file: str = '.env') -> Settings:
    """
    Загружает конфигурацию из .env файла.

    Args:
        env_file: Путь к .env файлу (по умолчанию '.env')

    Returns:
        Settings: Объект конфигурации

    Raises:
        FileNotFoundError: Если .env файл не найден
        ValidationError: Если конфигурация невалидна
    """
    env_path = Path(env_file)
    if not env_path.exists():
        error_msg = f"Файл конфигурации не найден: {env_path.absolute()}"
        # Пытаемся залогировать, если логгер доступен
        # logger = get_logger('config') if LOGGER_AVAILABLE else get_temporary_logger()
        logger = get_logger('config')
        # logger = get_temporary_logger()
        logger.error(error_msg)

        raise FileNotFoundError(error_msg)

    try:
        settings = Settings(_env_file=str(env_path))
        return settings
    except ValidationError as e:
        # Форматируем ошибки валидации
        error_messages = []
        for error in e.errors():
            field = ' -> '.join(str(loc) for loc in error['loc'])
            msg = error['msg']
            error_messages.append(f"  • {field}: {msg}")

        formatted_errors = '\n'.join(error_messages)
        full_error_msg = f"Ошибка валидации конфигурации:\n{formatted_errors}"

        # Пытаемся залогировать, если логгер доступен
        if LOGGER_AVAILABLE:
            try:
                logger = get_logger('oracle_exporter.config')
                logger.error(full_error_msg)
            except Exception:
                pass

        raise ValueError(full_error_msg) from e


def print_config_summary(
    config: Settings,
    *,
    mask_sensitive: bool = True,
    logger: logging.Logger | None = None,
) -> None:
    """
    Выводит сводку конфигурации (с маскировкой чувствительных данных).

    Args:
        config: Объект конфигурации Settings
        mask_sensitive: Маскировать ли чувствительные данные (по умолчанию True)
        logger: Logger для вывода. Если None, выводит в консоль через print()
    """
    # Получаем данные (замаскированные или оригинальные)
    if mask_sensitive:
        masked = config.model_dump_masked()
    else:
        masked = config.model_dump()

    # Определяем секции для вывода
    sections = [
        ('База данных', ['db_type', 'db_connect_uri', 'lib_dir']),
        ('Логирование', ['log_level', 'log_file']),
        ('Экспорт', ['output_dir']),
        ('Производительность', ['fetch_array_size', 'chunk_size', 'query_timeout']),
        ('Excel', ['max_column_width', 'max_rows_per_sheet', 'wrap_long_text', 'null_value_replacement']),
        ('Батч обработка', ['enable_batch_processing', 'batch_size', 'show_progress_bar', 'progress_update_interval']),
    ]

    # Если передан logger - используем его, иначе print()
    if logger:
        _log_config_header(logger)
        for section_name, params in sections:
            _log_config_section(section_name, params, masked, logger)
        _log_config_footer(logger)
    else:
        _print_config_to_console(sections, masked)


def _log_config_header(logger: logging.Logger) -> None:
    """Выводит заголовок конфигурации в logger."""
    logger.info("=" * 60)
    logger.info("КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ")
    logger.info("=" * 60)


def _log_config_section(
    section_name: str,
    params: list[str],
    config_data: dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Выводит секцию конфигурации в logger."""
    logger.info("")
    logger.info("[%s]", section_name)
    logger.info("-" * 40)

    for param in params:
        value = config_data.get(param)
        # Пропускаем None значения (кроме lib_dir для не-Oracle)
        if value is None and param != 'lib_dir':
            continue

        # Форматируем название параметра
        display_name = param.replace('_', ' ').title()

        # Специальная обработка для некоторых параметров
        if param == 'lib_dir' and value is None:
            continue  # Не показываем lib_dir если не задан

        logger.info("  %-28s: %s", display_name, value)


def _log_config_footer(logger: logging.Logger) -> None:
    """Выводит футер конфигурации в logger."""
    logger.info("")
    logger.info("=" * 60)


def _print_config_to_console(
    sections: list[tuple[str, list[str]]],
    config_data: dict[str, Any],
) -> None:
    """Выводит конфигурацию в консоль через print()."""
    print("\n" + "=" * 60)
    print("КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ")
    print("=" * 60)

    for section_name, params in sections:
        print(f"\n[{section_name}]")
        print("-" * 40)

        for param in params:
            value = config_data.get(param)
            # Пропускаем None значения (кроме lib_dir для не-Oracle)
            if value is None and param != 'lib_dir':
                continue

            # Форматируем название параметра
            display_name = param.replace('_', ' ').title()

            # Специальная обработка для некоторых параметров
            if param == 'lib_dir' and value is None:
                continue  # Не показываем lib_dir если не задан

            print(f"  {display_name:28}: {value}")

    print("=" * 60 + "\n")


def main() -> None:
    """Демонстрация работы модуля."""
    try:
        config = load_config()
        print_config_summary(config)
        print("\n✓ Конфигурация успешно загружена из .env")
        print("✓ DB_TYPE: {config.db_type}")
        print("✓ Оригинальный URI доступен через: config._original_db_connect_uri")

    except (FileNotFoundError, ValueError) as e:
        print(f"\n✗ Ошибка: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
