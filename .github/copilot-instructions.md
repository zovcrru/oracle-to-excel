<!-- Copilot / AI agent instructions for `oracle-to-excel` -->

Короткие, практичные указания для быстрого старта работы с кодовой базой.

- **Коротко — что это**: CLI-утилита для экспорта строк из БД в Excel. Точка входа: `src/oracle_to_excel/main.py` (скрипт `oracle-to-excel` в `pyproject.toml`).

- **Ключевые модули**: `config.py` (runtime `.env`), `env_config.py` (Pydantic variant), `logger.py`, `database.py` (использует `main.py`), `database_refactored.py` (рефактор — не менять импорты), `queries/` (билдеры + стриминг), `transformers.py` (конвертеры типов).

- **Runtime / deps**: Python >= 3.14 (см. `pyproject.toml`). Установка для разработки: `pip install -e .[dev]`. Windows venv: `.venv\\Scripts\\activate.bat`.

- **Конфигурация**: приложение читает `.env` через `src/oracle_to_excel/config.py`. Обязательные переменные: `DB_TYPE` и `DB_CONNECT_URI`. `env_config.py` предоставляет Pydantic `Settings` — полезно для валидации, но `main.py` по умолчанию использует `config.py`.

- **Секреты и маскирование**: оригинальный URI хранится в `_original_db_connect_uri`. Используйте `model_dump_masked()` / `Settings.mask_connection_string()` из `env_config.py` при логировании.

- **DB и зависимости**: импорты `psycopg` и `oracledb` могут падать при отсутствии пакетов — установите зависимости перед запуском тестов. `database.py` — текущая реализация; `database_refactored.py` — новая реализация (не переключать без тестов).

- **Обработка данных**: применять стриминг (`queries.base.execute_query_stream`) и `transform_row` из `transformers.py` перед записью в Excel — не загружать весь набор в память.

- **Логирование**: используйте `get_logger('<module>')` и `setup_logging()`; проект уже имеет фильтры маскировки и декораторы `@log_execution_time` / `@log_function_call`.

- **Тесты и качество**: тесты — `pytest`. Запуск: `pytest -v`. Форматирование/линтинг: `ruff`. Проверка типов: `mypy` (см. `pyproject.toml`).

- **Полезные команды**:
  - `pip install -e .[dev]`
  - `.venv\\Scripts\\activate.bat` (Windows)
  - `python -m oracle_to_excel.config --create-example`
  - `pytest -v --maxfail=1`

- **Избегать изменений**: не менять импорты `main.py` на `database_refactored.py` без полного тестирования; не залогировать пароли в явном виде.

- **Файлы для проверки перед изменением**: `src/oracle_to_excel/config.py`, `src/oracle_to_excel/env_config.py`, `src/oracle_to_excel/database.py`, `src/oracle_to_excel/queries/base.py`, `src/oracle_to_excel/transformers.py`, `src/oracle_to_excel/logger.py`.

Если нужно, могу сократить файл дальше, добавить примеры по конкретной задаче или создать PR с изменениями. Укажите, что расширить.
