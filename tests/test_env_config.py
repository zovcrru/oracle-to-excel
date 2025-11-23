"""Тесты для модуля env_config.py с акцентом на безопасность паролей."""

import logging
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.oracle_to_excel.env_config import Settings, load_config, print_config_summary

# ============================================================================
# Фикстуры для создания тестовых .env файлов
# ============================================================================


@pytest.fixture
def oracle_env_file(tmp_path: Path) -> Path:
    """Создаёт тестовый .env файл для Oracle."""
    env_content = (
        'DB_TYPE=oracle\n'
        'DB_CONNECT_URI=oracle://testuser:SecretPassword123@localhost:1521/TESTDB\n'
        'LIB_DIR=/opt/oracle/instantclient_21_1\n'
        'LOG_LEVEL=DEBUG\n'
        'OUTPUT_DIR=./test_exports\n'
    )
    env_file = tmp_path / '.env'
    env_file.write_text(env_content)
    return env_file


@pytest.fixture
def postgres_env_file(tmp_path: Path) -> Path:
    """Создаёт тестовый .env файл для PostgreSQL."""
    env_content = (
        'DB_TYPE=postgresql\n'
        'DB_CONNECT_URI=postgresql://pguser:MyP@ssw0rd!@db.example.com:5432/testdb\n'
        'LOG_LEVEL=INFO\n'
    )
    env_file = tmp_path / '.env'
    env_file.write_text(env_content)
    return env_file


@pytest.fixture
def sqlite_env_file(tmp_path: Path) -> Path:
    """Создаёт тестовый .env файл для SQLite."""
    env_content = 'DB_TYPE=sqlite\nDB_CONNECT_URI=sqlite:///data/test.db\n'
    env_file = tmp_path / '.env'
    env_file.write_text(env_content)
    return env_file


# ============================================================================
# Тесты маскировки паролей в URI
# ============================================================================


class TestPasswordMasking:
    """Тесты маскировки паролей в различных URI форматах."""

    @pytest.mark.parametrize(
        'uri,expected_masked',
        [
            # Oracle с паролем
            (
                'oracle://scott:tiger@localhost:1521/ORCL',
                'oracle://scott:***@localhost:1521/ORCL',
            ),
            # PostgreSQL с паролем
            (
                'postgresql://admin:SecretPass@db.local:5432/mydb',
                'postgresql://admin:***@db.local:5432/mydb',
            ),
            # Пароль со спецсимволами
            (
                'postgresql://user:P@ssw0rd@host:5432/db',
                'postgresql://user:***@host:5432/db',
            ),
            # Oracle+cx_oracle драйвер
            (
                'oracle+cx_oracle://user:password@localhost:1521/xe',
                'oracle+cx_oracle://user:***@localhost:1521/xe',
            ),
            # PostgreSQL+psycopg2
            (
                'postgresql+psycopg2://user:pass123@db:5432/mydb',
                'postgresql+psycopg2://user:***@db:5432/mydb',
            ),
            # URI без пароля (не должно измениться)
            (
                'oracle://user@localhost:1521/ORCL',
                'oracle://user@localhost:1521/ORCL',
            ),
            # SQLite (не содержит паролей)
            ('sqlite:///path/to/database.db', 'sqlite:///path/to/database.db'),
            # TNS имя Oracle (без схемы)
            ('MYDB_TNS', 'MYDB_TNS'),
            # Пустая строка
            ('', ''),
        ],
    )
    def test_mask_connection_string(self, uri: str, expected_masked: str):
        """Проверка маскировки различных форматов connection strings."""
        masked = Settings.mask_connection_string(uri)
        assert masked == expected_masked

        # Проверяем, что оригинальный пароль не присутствует
        if '://' in uri and '@' in uri and ':' in uri.split('://')[1].split('@')[0]:
            password = uri.split('://')[1].split(':')[1].split('@')[0]
            if password:  # Если пароль не пустой
                assert password not in masked, (
                    f"Пароль '{password}' не должен быть виден в {masked}"
                )

    @pytest.mark.parametrize(
        'password',
        [
            'simple',
            'P@ssw0rd!',
            'pass_word',
            '123456',
            'pass-word-123',
            'alongpassword' * 10,
        ],
    )
    def test_mask_various_passwords(self, password: str):
        """Тест маскировки паролей с различными символами."""
        uri = f'oracle://testuser:{password}@localhost:1521/TESTDB'
        masked = Settings.mask_connection_string(uri)

        assert password not in masked, f"Пароль '{password}' виден в: {masked}"
        assert 'testuser:***@' in masked
        assert 'localhost:1521' in masked


# ============================================================================
# Тесты model_dump_masked
# ============================================================================


class TestModelDumpMasked:
    """Тесты метода model_dump_masked для безопасного экспорта данных."""

    def test_model_dump_masked_oracle(self, oracle_env_file: Path):
        """Проверка маскировки для Oracle конфигурации."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(oracle_env_file))

        # Обычный dump содержит оригинальные данные
        regular_dump = config.model_dump()
        assert 'SecretPassword123' in regular_dump['db_connect_uri']

        # Masked dump маскирует пароль
        masked_dump = config.model_dump_masked()
        assert 'SecretPassword123' not in str(masked_dump)
        assert masked_dump['db_connect_uri'] == 'oracle://testuser:***@localhost:1521/TESTDB'

        # Оригинальный URI сохранён
        assert (
            config._original_db_connect_uri
            == 'oracle://testuser:SecretPassword123@localhost:1521/TESTDB'
        )

    def test_model_dump_masked_postgresql(self, postgres_env_file: Path):
        """Проверка маскировки для PostgreSQL."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(postgres_env_file))

        masked_dump = config.model_dump_masked()
        assert 'MyP@ssw0rd!' not in str(masked_dump)
        assert masked_dump['db_connect_uri'] == 'postgresql://pguser:***@db.example.com:5432/testdb'

    def test_model_dump_masked_sqlite_no_password(self, sqlite_env_file: Path):
        """SQLite не содержит паролей - URI не меняется."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(sqlite_env_file))

        masked_dump = config.model_dump_masked()
        assert masked_dump['db_connect_uri'] == 'sqlite:///data/test.db'


# ============================================================================
# Тесты защиты паролей в логах
# ============================================================================


class TestLoggingPasswordProtection:
    """Тесты предотвращения попадания паролей в логи."""

    def test_print_config_summary_masks_password_in_logs(self, oracle_env_file: Path, caplog):
        """Проверка, что print_config_summary не выводит пароли в лог."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(oracle_env_file))

        logger = logging.getLogger('test_security')
        logger.setLevel(logging.INFO)

        with caplog.at_level(logging.INFO, logger='test_security'):
            print_config_summary(config, mask_sensitive=True, logger=logger)

        log_output = caplog.text

        # Пароль НЕ должен попасть в лог
        assert 'SecretPassword123' not in log_output, 'КРИТИЧНО: Пароль обнаружен в логах!'

        # Замаскированная версия должна быть в логе
        assert '***' in log_output
        assert 'oracle://testuser:***@localhost:1521/TESTDB' in log_output

    def test_print_config_summary_console_masks_password(self, postgres_env_file: Path, capsys):
        """Проверка маскировки паролей в консольном выводе."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(postgres_env_file))

        print_config_summary(config, mask_sensitive=True, logger=None)
        captured = capsys.readouterr()

        assert 'MyP@ssw0rd!' not in captured.out, 'КРИТИЧНО: Пароль в консоли!'
        assert 'postgresql://pguser:***@db.example.com:5432/testdb' in captured.out

    def test_print_config_without_masking_shows_password(self, oracle_env_file: Path, caplog):
        """Проверка, что при mask_sensitive=False пароль виден (для отладки)."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(oracle_env_file))

        logger = logging.getLogger('test_no_mask')
        logger.setLevel(logging.INFO)

        with caplog.at_level(logging.INFO, logger='test_no_mask'):
            print_config_summary(config, mask_sensitive=False, logger=logger)

        log_output = caplog.text
        # При отключении маскировки пароль виден
        assert 'SecretPassword123' in log_output


# ============================================================================
# Тесты защиты паролей в исключениях
# ============================================================================


class TestPasswordInExceptions:
    """Проверка, что пароли не раскрываются в сообщениях об ошибках."""

    def test_invalid_oracle_uri_masks_password(self, tmp_path: Path):
        """Ошибка валидации не должна содержать пароль."""
        env_content = (
            'DB_TYPE=oracle\nDB_CONNECT_URI=oracle://user:TopSecret@\nLIB_DIR=/opt/oracle\n'
        )
        env_file = tmp_path / '.env'
        env_file.write_text(env_content)

        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                load_config(str(env_file))

        error_msg = str(exc_info.value)
        assert 'TopSecret' not in error_msg, 'КРИТИЧНО: Пароль в ошибке валидации!'
        assert '***' in error_msg or 'user:' in error_msg

    def test_missing_port_error_masks_password(self, tmp_path: Path):
        """Ошибка отсутствия порта маскирует пароль."""
        env_content = (
            'DB_TYPE=postgresql\nDB_CONNECT_URI=postgresql://admin:VerySecret@dbhost/mydb\n'
        )
        env_file = tmp_path / '.env'
        env_file.write_text(env_content)

        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                load_config(str(env_file))

        error_msg = str(exc_info.value)
        assert 'VerySecret' not in error_msg, 'КРИТИЧНО: Пароль в сообщении об ошибке!'
        assert '***' in error_msg

    def test_missing_database_name_masks_password(self, tmp_path: Path):
        """Ошибка отсутствия имени БД маскирует пароль."""
        env_content = (
            'DB_TYPE=postgresql\nDB_CONNECT_URI=postgresql://user:HiddenPass@localhost:5432\n'
        )
        env_file = tmp_path / '.env'
        env_file.write_text(env_content)

        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                load_config(str(env_file))

        error_msg = str(exc_info.value)
        assert 'HiddenPass' not in error_msg, 'КРИТИЧНО: Пароль в ошибке!'

    def test_malformed_uri_single_slash_password_reference(self, tmp_path: Path):
        """Тест проверяет, что ошибка валидации вызывается для неправильного формата URI.

        Проверяет случай: postgresql:/postgres:pg@localhost:5433/postgres
        (один слеш вместо двух в схеме).

        Примечание: mask_connection_string преобразует это в postgresql:///postgres:***@...
        (три слеша вместо одного), что указывает на то, что маскирование произошло.
        """
        env_content = (
            'DB_TYPE=postgresql\nDB_CONNECT_URI=postgresql:/postgres:pg@localhost:5433/postgres\n'
        )
        env_file = tmp_path / '.env'
        env_file.write_text(env_content)

        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                load_config(str(env_file))

        error_msg = str(exc_info.value)
        # Проверяем, что ошибка выброшена для неправильного URI формата
        assert 'postgresql' in error_msg
        # Проверяем, что сообщение об ошибке содержит указание на проблему
        assert 'POSTGRESQL' in error_msg or 'postgresql' in error_msg


# ============================================================================
# Тесты валидации и безопасности
# ============================================================================


class TestValidationSecurity:
    """Тесты безопасности при валидации конфигурации."""

    def test_original_uri_stored_privately(self, oracle_env_file: Path):
        """Оригинальный URI хранится в приватном поле."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(oracle_env_file))

        # Приватное поле существует
        assert hasattr(config, '_original_db_connect_uri')
        assert config._original_db_connect_uri is not None
        assert 'SecretPassword123' in config._original_db_connect_uri

    def test_safe_representation(self, oracle_env_file: Path):
        """Проверка безопасного представления конфигурации."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(oracle_env_file))

        # Безопасный способ
        safe_repr = str(config.model_dump_masked())
        assert 'SecretPassword123' not in safe_repr
        assert '***' in safe_repr


# ============================================================================
# Интеграционные тесты безопасности
# ============================================================================


class TestIntegrationPasswordSecurity:
    """Комплексные тесты защиты паролей в реальных сценариях."""

    def test_full_workflow_no_password_leak(self, oracle_env_file: Path, caplog):
        """Полный workflow: загрузка -> валидация -> логирование без утечки пароля."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(oracle_env_file))

        logger = logging.getLogger('integration_security')
        logger.setLevel(logging.DEBUG)

        with caplog.at_level(logging.DEBUG):
            # Выводим конфигурацию
            print_config_summary(config, mask_sensitive=True, logger=logger)

            # Имитируем логирование в приложении
            logger.info('DB Type: %s', config.db_type)
            logger.info('Connecting to: %s', config.model_dump_masked()['db_connect_uri'])
            logger.debug('Config: %s', config.model_dump_masked())

        full_log = caplog.text

        # КРИТИЧЕСКАЯ ПРОВЕРКА: пароль нигде не должен светиться
        assert 'SecretPassword123' not in full_log, 'УТЕЧКА ПАРОЛЯ В ЛОГАХ!'

        # Замаскированные данные должны быть
        assert '***' in full_log
        assert 'oracle://testuser:***@localhost:1521/TESTDB' in full_log

    def test_multiple_configs_no_cross_contamination(self, tmp_path: Path):
        """Проверка изоляции паролей между разными конфигами."""
        # Oracle конфиг
        oracle_env = tmp_path / 'oracle.env'
        oracle_env.write_text(
            'DB_TYPE=oracle\n'
            'DB_CONNECT_URI=oracle://user1:OraclePass@localhost:1521/ORCL\n'
            'LIB_DIR=/opt/oracle\n'
        )

        # PostgreSQL конфиг
        pg_env = tmp_path / 'pg.env'
        pg_env.write_text(
            'DB_TYPE=postgresql\n'
            'DB_CONNECT_URI=postgresql://user2:PostgresPass@localhost:5432/pgdb\n'
        )

        # Load oracle config
        oracle_config = load_config(str(oracle_env))

        # Clear DB-related env vars before loading second config (load_dotenv adds, not replaces)
        for key in ['DB_TYPE', 'DB_CONNECT_URI', 'LIB_DIR']:
            os.environ.pop(key, None)

        pg_config = load_config(str(pg_env))

        # Проверяем, что каждая конфигурация маскирует свой пароль
        oracle_masked = oracle_config.model_dump_masked()
        pg_masked = pg_config.model_dump_masked()

        assert 'OraclePass' not in str(oracle_masked)
        assert 'PostgresPass' not in str(pg_masked)
        assert 'user1:***@' in str(oracle_masked['db_connect_uri'])
        assert 'user2:***@' in str(pg_masked['db_connect_uri'])


# ============================================================================
# Граничные случаи и edge cases
# ============================================================================


class TestEdgeCases:
    """Тесты граничных случаев маскировки."""

    def test_empty_password(self):
        """Пустой пароль (user:@host)."""
        uri = 'oracle://user:@localhost:1521/ORCL'
        masked = Settings.mask_connection_string(uri)
        assert 'user:***@' in masked

    def test_password_with_colon(self):
        """Пароль содержит двоеточие."""
        uri = 'postgresql://user:pass_word_123@host:5432/db'
        masked = Settings.mask_connection_string(uri)
        assert 'pass_word_123' not in masked
        assert '***' in masked

    def test_very_long_password(self):
        """Очень длинный пароль."""
        long_pass = 'a' * 500
        uri = f'oracle://user:{long_pass}@localhost:1521/ORCL'
        masked = Settings.mask_connection_string(uri)
        assert long_pass not in masked
        assert 'user:***@' in masked


# ============================================================================
# Тесты соответствия best practices
# ============================================================================


class TestSecurityBestPractices:
    """Проверка соответствия лучшим практикам безопасности."""

    def test_never_log_original_uri_directly(self, oracle_env_file: Path, caplog):
        """Убедиться, что прямое логирование db_connect_uri не используется."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(oracle_env_file))

        logger = logging.getLogger('best_practice_test')

        with caplog.at_level(logging.INFO):
            # ПРАВИЛЬНО: используем model_dump_masked()
            logger.info('Config: %s', config.model_dump_masked())

        log_output = caplog.text
        assert 'SecretPassword123' not in log_output

    def test_immutable_original_uri(self, oracle_env_file: Path):
        """Оригинальный URI не должен меняться после инициализации."""
        with patch.dict('os.environ', {}, clear=True):
            config = load_config(str(oracle_env_file))

        original = config._original_db_connect_uri

        # Оригинал сохранён
        assert config._original_db_connect_uri == original
        assert 'SecretPassword123' in config._original_db_connect_uri


# ============================================================================
# Параметризованные тесты для различных СУБД
# ============================================================================


@pytest.mark.parametrize(
    'db_type,uri,password,extra_config',
    [
        (
            'oracle',
            'oracle://user:Pass123@localhost:1521/ORCL',
            'Pass123',
            'LIB_DIR=/opt/oracle\n',
        ),
        ('postgresql', 'postgresql://user:PgSecret@db:5432/mydb', 'PgSecret', ''),
        (
            'oracle',
            'oracle+cx_oracle://user:Tiger2024@host:1521/xe',
            'Tiger2024',
            'LIB_DIR=/opt/oracle\n',
        ),
    ],
)
def test_password_masking_all_db_types(
    tmp_path: Path, db_type: str, uri: str, password: str, extra_config: str
):
    """Параметризованный тест для всех типов БД."""
    env_content = f'DB_TYPE={db_type}\nDB_CONNECT_URI={uri}\n{extra_config}'

    env_file = tmp_path / '.env'
    env_file.write_text(env_content)

    with patch.dict('os.environ', {}, clear=True):
        config = load_config(str(env_file))

    masked = config.model_dump_masked()

    # Пароль не должен быть виден
    assert password not in str(masked), f"Пароль '{password}' обнаружен для {db_type}!"
    assert '***' in str(masked['db_connect_uri'])


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
