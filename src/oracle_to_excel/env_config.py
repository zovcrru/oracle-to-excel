"""Модуль загрузки конфигурации из .env файла с использованием Pydantic."""

from __future__ import annotations

import logging
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Final, cast

from dotenv import load_dotenv
from pydantic import Field, ValidationError, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError

try:
    # Предпочтительно использовать свой логгер — если доступен
    from .logger import get_logger

    LOGGER_AVAILABLE = True
except Exception:
    LOGGER_AVAILABLE = False

VALID_DB_TYPES: Final[frozenset[str]] = frozenset(
    ('oracle', 'postgres', 'postgresql', 'sqlite', 'sqlite3')
)


def _get_uri_separator(uri: str) -> str | None:
    """Определить разделитель схемы в URI.

    Возвращает '://', ':/', '//' или None.
    """
    if '://' in uri:
        return '://'
    if ':/' in uri:
        return ':/'
    if '//' in uri:
        return '//'
    return None


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

    db_type: str = Field(..., description='Тип базы данных: oracle, postgres, sqlite')
    db_connect_uri: str = Field(..., description='Connection string для БД')

    lib_dir: str | None = Field(
        None, description='Путь к Oracle Instant Client (только для Oracle)'
    )

    log_level: str = Field(
        default=cast(str, DEFAULT_CONFIG['LOG_LEVEL']),
        description='Уровень логирования',
    )
    log_file: str = Field(
        default=cast(str, DEFAULT_CONFIG['LOG_FILE']),
        description='Путь к файлу логов',
    )
    output_dir: str = Field(
        default=cast(str, DEFAULT_CONFIG['OUTPUT_DIR']),
        description='Директория для экспорта',
    )

    fetch_array_size: int = Field(
        default=cast(int, DEFAULT_CONFIG['FETCH_ARRAY_SIZE']),
        ge=1,
        description='Размер массива для fetchmany()',
    )
    chunk_size: int = Field(
        default=cast(int, DEFAULT_CONFIG['CHUNK_SIZE']),
        ge=1,
        description='Размер чанка для обработки',
    )
    query_timeout: int = Field(
        default=cast(int, DEFAULT_CONFIG['QUERY_TIMEOUT']),
        ge=0,
        description='Таймаут запроса (секунды)',
    )

    max_column_width: int = Field(
        default=cast(int, DEFAULT_CONFIG['MAX_COLUMN_WIDTH']),
        ge=1,
        description='Макс. ширина колонки',
    )
    null_value_replacement: str = Field(
        default=cast(str, DEFAULT_CONFIG['NULL_VALUE_REPLACEMENT']),
        description='Замена для NULL',
    )
    wrap_long_text: bool = Field(
        default=cast(bool, DEFAULT_CONFIG['WRAP_LONG_TEXT']),
        description='Перенос длинного текста',
    )
    max_rows_per_sheet: int = Field(
        default=cast(int, DEFAULT_CONFIG['MAX_ROWS_PER_SHEET']),
        ge=1,
        description='Макс. строк на лист',
    )

    enable_batch_processing: bool = Field(
        default=cast(bool, DEFAULT_CONFIG['ENABLE_BATCH_PROCESSING']),
        description='Включить батч обработку',
    )
    batch_size: int = Field(
        default=cast(int, DEFAULT_CONFIG['BATCH_SIZE']),
        ge=1,
        description='Размер батча',
    )
    show_progress_bar: bool = Field(
        default=cast(bool, DEFAULT_CONFIG['SHOW_PROGRESS_BAR']),
        description='Показывать прогресс бар',
    )
    progress_update_interval: int = Field(
        default=cast(int, DEFAULT_CONFIG['PROGRESS_UPDATE_INTERVAL']),
        ge=1,
        description='Интервал обновления прогресса',
    )

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
    def parse_empty_int(cls, v: str | int | None, info: ValidationInfo) -> int | str | None:
        """Преобразует пустые строки в дефолтные значения для int полей."""
        if v == '' or v is None:
            field_name = info.field_name or ''
            default_value = DEFAULT_CONFIG.get(field_name.upper())
            return default_value if default_value is not None else v
        return v

    @field_validator(
        'enable_batch_processing',
        'show_progress_bar',
        'wrap_long_text',
        mode='before',
    )
    @classmethod
    def parse_empty_bool(cls, v: object, info: ValidationInfo) -> object:
        """Преобразует пустые строки и строковые bool в дефолтные значения."""
        if v == '' or v is None:
            field_name = info.field_name or ''
            default_value = DEFAULT_CONFIG.get(field_name.upper())
            return default_value if default_value is not None else v
        if isinstance(v, str):
            lower_v = v.lower().strip()
            if lower_v in ('true', '1', 'yes', 'on'):
                return True
            if lower_v in ('false', '0', 'no', 'off'):
                return False
        return v

    @field_validator('null_value_replacement', 'log_level', 'log_file', 'output_dir', mode='before')
    @classmethod
    def parse_empty_str(cls, v: object, info: ValidationInfo) -> object:
        """Преобразует пустые строки в дефолтные значения для str полей."""
        if v == '' or v is None:
            field_name = info.field_name or ''
            default_value = DEFAULT_CONFIG.get(field_name.upper())
            return default_value if default_value is not None else ''
        return v

    @field_validator('db_type')
    @classmethod
    def normalize_db_type(cls, v: str) -> str:
        """Нормализует и валидирует db_type."""
        if not v or v == '':
            raise ValueError('DB_TYPE не может быть пустым')
        normalized = v.strip().lower()
        if normalized not in VALID_DB_TYPES:
            valid_types = ', '.join(sorted(VALID_DB_TYPES))
            raise ValueError(f"Недопустимый DB_TYPE='{v}'. Допустимые значения: {valid_types}")
        if normalized in ('postgres', 'postgresql'):
            return 'postgresql'
        if normalized in ('sqlite', 'sqlite3'):
            return 'sqlite'
        return normalized

    @field_validator('db_connect_uri')
    @classmethod
    def validate_db_connect_uri(cls, v: str, info: ValidationInfo) -> str:
        """Валидирует строку подключения к БД с помощью SQLAlchemy make_url."""
        if not v or v.strip() == '':
            raise ValueError('DB_CONNECT_URI не может быть пустым')
        uri = v.strip()
        masked_uri = cls.mask_connection_string(uri)

        db_type = info.data.get('db_type', '').lower()
        if not db_type:
            return uri
        db_type = cls._normalize_db_type_for_validation(db_type)

        # SQLite has special handling
        if db_type == 'sqlite':
            if not uri.startswith('sqlite:'):
                raise ValueError(f'Для SQLite URI должен начинаться с "sqlite:": {masked_uri}')
            return uri

        # Parse URL for other databases
        cls._validate_url_format(uri, db_type, masked_uri)
        return uri

    @staticmethod
    def _validate_url_format(uri: str, db_type: str, masked_uri: str) -> None:
        """Валидирует формат URL для Oracle и PostgreSQL."""
        try:
            url_obj = make_url(uri)
        except ArgumentError:
            error_msg = (
                f'Некорректный URI для {db_type.upper()}: некорректный формат URL\n'
                f'URI: {masked_uri}'
            )
            raise ValueError(error_msg) from None

        Settings._check_scheme_allowed(url_obj.drivername, db_type, masked_uri)
        Settings._check_host_and_port(url_obj, db_type, masked_uri)
        if db_type == 'postgresql' and not url_obj.database:
            raise ValueError(f'PostgreSQL URI не содержит имя базы данных: {masked_uri}')

    @staticmethod
    def _check_host_and_port(url_obj: object, db_type: str, masked_uri: str) -> None:
        """Проверяет наличие hostname и port в URL."""
        if not getattr(url_obj, 'host', None):
            raise ValueError(f'URI не содержит hostname: {masked_uri}')
        if getattr(url_obj, 'port', None) is None:
            default_port = 1521 if db_type == 'oracle' else 5432
            raise ValueError(
                f'URI не содержит порт. Укажите порт явно (стандартный для '
                f'{db_type.upper()}: {default_port}). URI: {masked_uri}'
            )

    @staticmethod
    def _normalize_db_type_for_validation(db_type: str) -> str:
        if db_type in ('postgres', 'postgresql'):
            return 'postgresql'
        if db_type in ('sqlite', 'sqlite3'):
            return 'sqlite'
        return db_type

    @staticmethod
    def _check_scheme_allowed(drivername: str, db_type: str, masked_uri: str) -> None:
        if db_type == 'oracle':
            allowed = ('oracle', 'oracle+cx_oracle', 'oracle+oracledb')
        elif db_type == 'postgresql':
            allowed = (
                'postgresql',
                'postgresql+psycopg2',
                'postgresql+psycopg',
                'postgresql+psycopg3',
            )
        else:
            return
        if drivername not in allowed:
            msg = (
                f'Неверная схема для {db_type.title()} URI: {drivername!r}. '
                f'Ожидается одно из {allowed}. URI: {masked_uri}'
            )
            raise ValueError(msg)

    @staticmethod
    def mask_connection_string(uri: str) -> str:
        """Mask password in URI with simple parsing, not SQLAlchemy.

        SQLAlchemy render_as_string() URL-encodes password: ':***@' -> ':%2A%2A%2A@'.
        Use simple parsing to preserve readability.

        Handles edge cases like:
        - Single slash (postgresql:/postgres:pass@host) - malformed
        - Double slash without colon (postgresql//postgres:pass@host) - malformed
        - Multiple @ in password (user:p@ss@rd@host)
        - No password (user@host or host)
        """
        if not uri:
            return uri

        # Detect scheme patterns and determine separator
        separator = _get_uri_separator(uri)
        if not separator:
            return uri

        # Split by appropriate separator
        scheme_part, rest = uri.split(separator, 1)

        if '@' not in rest:
            return uri

        # Find the last @ (separator between credentials and host)
        last_at_idx = rest.rfind('@')
        credentials_part = rest[:last_at_idx]
        host_part = rest[last_at_idx + 1 :]

        # No credentials or no password means no masking needed
        if not credentials_part or ':' not in credentials_part:
            return uri

        # Split on first colon to find user/password separator
        colon_idx = credentials_part.find(':')
        user_part = credentials_part[:colon_idx]

        # Reconstruct with masked password
        return f'{scheme_part}{separator}{user_part}:***@{host_part}'

    @model_validator(mode='after')
    def validate_oracle_lib_dir(self) -> Settings:
        """Проверяет наличие lib_dir для Oracle."""
        if self.db_type == 'oracle' and not self.lib_dir:
            raise ValueError("LIB_DIR обязателен для DB_TYPE='oracle'")
        return self

    def model_post_init(self, _context: object) -> None:
        """Сохраняем оригинальный connection string после инициализации."""
        self._original_db_connect_uri = self.db_connect_uri

    def model_dump_masked(self) -> dict[str, object]:
        """Возвращает словарь с замаскированным db_connect_uri."""
        data = self.model_dump()
        if self.db_connect_uri:
            data['db_connect_uri'] = self.mask_connection_string(self.db_connect_uri)
        return data


def load_config(env_file: str = '.env') -> Settings:
    """Загружает конфигурацию из .env файла."""
    env_path = Path(env_file)
    if not env_path.exists():
        error_msg = f'Файл конфигурации не найден: {env_path.absolute()}'
        logger = get_logger('config') if LOGGER_AVAILABLE else logging.getLogger('config')
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    try:
        # Use python-dotenv to load variables from specified file
        load_dotenv(env_path)
        return Settings()  # type: ignore[call-arg]
    except ValidationError as e:
        full_error_msg = _format_validation_error(e)
        raise ValueError(full_error_msg) from e


def _format_validation_error(e: ValidationError) -> str:
    error_messages = []
    for error in e.errors():
        field = ' -> '.join(str(loc) for loc in error['loc'])
        msg = error['msg']
        error_messages.append(f' • {field}: {msg}')
    formatted_errors = '\n'.join(error_messages)
    full_error_msg = f'Ошибка валидации конфигурации:\n{formatted_errors}'
    if LOGGER_AVAILABLE:
        try:
            logger = get_logger('oracle_exporter.config')
            logger.error(full_error_msg)
        except Exception:
            # чтобы в логи не выводился traceback c открытым паролем
            logging.getLogger('oracle_exporter.config').error('Failed to log validation error')  # noqa: TRY400
    return full_error_msg


def print_config_summary(
    config: Settings,
    *,
    mask_sensitive: bool = True,
    logger: logging.Logger | None = None,
) -> None:
    """Выводит сводку конфигурации с маскировкой чувствительных данных."""
    masked = config.model_dump_masked() if mask_sensitive else config.model_dump()
    sections = [
        ('База данных', ['db_type', 'db_connect_uri', 'lib_dir']),
        ('Логирование', ['log_level', 'log_file']),
        ('Экспорт', ['output_dir']),
        ('Производительность', ['fetch_array_size', 'chunk_size', 'query_timeout']),
        (
            'Excel',
            ['max_column_width', 'max_rows_per_sheet', 'wrap_long_text', 'null_value_replacement'],
        ),
        (
            'Батч обработка',
            [
                'enable_batch_processing',
                'batch_size',
                'show_progress_bar',
                'progress_update_interval',
            ],
        ),
    ]
    if logger:
        _log_config_header(logger)
        for section_name, params in sections:
            _log_config_section(section_name, params, masked, logger)
        _log_config_footer(logger)
    else:
        _print_config_to_console(sections, masked)


def _log_config_header(logger: logging.Logger) -> None:
    logger.info('=' * 60)
    logger.info('КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ')
    logger.info('=' * 60)


def _log_config_section(
    section_name: str,
    params: list[str],
    config_data: dict[str, object],
    logger: logging.Logger,
) -> None:
    logger.info('')
    logger.info('[%s]', section_name)
    logger.info('-' * 40)
    for param in params:
        value = config_data.get(param)
        if value is None and param != 'lib_dir':
            continue
        display_name = param.replace('_', ' ').title()
        if param == 'lib_dir' and value is None:
            continue
        logger.info(' %-28s: %s', display_name, value)


def _log_config_footer(logger: logging.Logger) -> None:
    logger.info('')
    logger.info('=' * 60)


def _print_config_to_console(
    sections: list[tuple[str, list[str]]],
    config_data: dict[str, object],
) -> None:
    print('\n' + '=' * 60)
    print('КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ')
    print('=' * 60)
    for section_name, params in sections:
        print(f'\n[{section_name}]')
        print('-' * 40)
        for param in params:
            value = config_data.get(param)
            if value is None and param != 'lib_dir':
                continue
            display_name = param.replace('_', ' ').title()
            if param == 'lib_dir' and value is None:
                continue
            print(f' {display_name:28}: {value}')
    print('=' * 60 + '\n')


def main() -> None:
    """Демонстрация работы модуля."""
    try:
        # Демонстрация маскирования пароля в URI
        test_uri = 'oracle+cx_oracle://scott:Tiger2024@localhost:1521/xe'
        masked = Settings.mask_connection_string(test_uri)
        print(f'Маскирование URI: {masked}')

        # Загрузка конфигурации из .env
        config = load_config()
        print_config_summary(config)
        print('\n✓ Конфигурация успешно загружена из .env')
        print(f'✓ DB_TYPE: {config.db_type}')
        print('✓ Оригинальный URI доступен через: config._original_db_connect_uri')
    except (FileNotFoundError, ValueError) as e:
        print(f'\n✗ Ошибка: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
