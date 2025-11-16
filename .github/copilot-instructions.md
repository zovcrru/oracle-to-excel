# Copilot Instructions: oracle-to-excel

## Project Overview

**Oracle to Excel Exporter** — Python 3.14 приложение для экспорта данных из таблицы Oracle `REPORT_EXCEL_SN` в форматированные Excel-файлы с логированием, обработкой ошибок и потоковой обработкой больших объемов данных.

### Architecture

Проект использует **модульную архитектуру с функциональным подходом**, разделённую на специализированные модули:

- **`config.py`** — загрузка и валидация конфигурации из `.env` с маскированием паролей
- **`logger.py`** — централизованное логирование с ротацией файлов и фильтрацией чувствительных данных
- **Планируемые модули** (`database.py`, `queries.py`, `error_handler.py`, `transformers.py`, `excel_writer.py`) — реализация в соответствии с `improved_plan.txt`

Ключевая фишка архитектуры: **streaming-обработка** данных через генераторы (не загружать всё в памяти сразу).

## Key Conventions & Patterns

### 1. Python 3.14 Features (Modern Syntax)

Код активно использует новейшие возможности Python 3.14:
- **Pattern Matching** (`match/case`) для обработки типов и состояний вместо if/elif
- **Type Parameter Syntax** (`def func[T]()`) для generic функций
- **ParamSpec** для типизации декораторов
- **TypedDict** с `NotRequired` для конфигурации
- **PEP 649** `from __future__ import annotations`

**Пример из config.py:**
```python
match default_value:
    case int():
        config[param] = _parse_int_param(param, env_value, default_value, logger)
    case str():
        config[param] = env_value if env_value else default_value
```

### 2. Configuration Management

**Pattern:** Загрузка конфигурации один раз на старте, валидация, маскирование паролей.

- Обязательные параметры в `REQUIRED_CONFIG` frozenset
- Значения по умолчанию в `DEFAULT_CONFIG` (immutable Mapping)
- Функция `validate_config()` возвращает кортеж `(bool, list[str])` — статус + список ошибок
- Маскирование чувствительных данных в логах через замену на `***`
- Восстановление оригинальных значений перед использованием через `restore_sensitive_data()`

**Рекомендация:** При добавлении новых параметров обновите `ConfigDict` TypedDict, `REQUIRED_CONFIG`, `DEFAULT_CONFIG`.

### 3. Logging System (Critical!)

**Every module** должен использовать централизованное логирование:
```python
from logger import get_logger
logger = get_logger('module_name')  # автоматически добавляет префикс 'oracle_exporter.'
```

**Декораторы для логирования:**
- `@log_execution_time` — логирует время выполнения функции
- `@log_function_call(log_args=True, log_result=False)` — логирует вызовы функций

**Маскирование паролей:** Система автоматически маскирует пароли, токены и секреты в логах через `_create_sensitive_filter()`.

### 4. Error Handling Strategy

**Структура, которую нужно реализовать в `error_handler.py`:**
1. Парсинг ORA-кодов из исключений Oracle
2. Специфичные сообщения об ошибках (ORA-00942, ORA-01017, ORA-12170 и т.д.)
3. `is_retryable_error()` — определить, временная ошибка или критическая
4. `retry_with_backoff()` декоратор с экспоненциальной задержкой и jitter
5. Логирование каждой попытки retry

### 5. Streaming Data Processing

**Для работы с большими датасетами используйте генераторы:**

```python
def execute_query_stream(connection, query, params, fetch_size: int) -> Generator:
    cursor = connection.cursor()
    cursor.arraysize = fetch_size  # Настройка размера пакета
    cursor.execute(query, params)
    
    metadata = cursor.description
    while True:
        rows = cursor.fetchmany(fetch_size)
        if not rows:
            break
        yield (metadata, rows)  # Yield кортежа метаданные + пакет данных
    cursor.close()
```

**Почему генераторы?**
- Не загружают всё в памяти
- Применяют трансформации построчно
- Позволяют логировать прогресс каждые N строк

### 6. Excel Writing Pattern

**Функции в `excel_writer.py` должны следовать порядку:**
1. `create_workbook()` — новый workbook
2. `apply_header_formatting()` — заголовки + стили (жирный, синий фон, freeze_panes)
3. `stream_data_to_excel()` — основной цикл записи пакетами через генератор
4. `calculate_column_widths()` — анализ первых N строк для ширины
5. `apply_column_widths()` — применение расчётной ширины
6. `apply_sheet_settings()` — фильтры, печать, сетка
7. `create_metadata_sheet()` — отдельный лист с информацией об экспорте
8. `save_workbook()` — сохранение с обработкой ошибок
9. `export_with_fallback()` — резервный CSV при критических ошибках

### 7. Type Hints & Documentation

**Все функции обязательно имеют:**
- Type hints для параметров и return values
- Docstring в Google style с:
  - Однострочным описанием
  - Args (параметры)
  - Returns (возвращаемое значение)
  - Raises (исключения)
  - Example (пример использования)

```python
def load_config(env_file: str = '.env', *, use_logging: bool = True) -> ConfigDict:
    """
    Загружает конфигурацию из .env файла.
    
    Args:
        env_file: Путь к файлу с переменными окружения.
        use_logging: Использовать ли систему логирования.
        
    Returns:
        Словарь с конфигурацией приложения.
        
    Raises:
        FileNotFoundError: Если .env файл не найден.
        ValueError: Если отсутствуют обязательные параметры.
    """
```

## Development Workflow

### Setup
```bash
# Установка зависимостей
pip install -e .
pip install -r requirements-dev.txt (создать файл)

# Конфигурация
python -m src.oracle_to_excel.config --create-example  # Создаёт .env.example
# Отредактируйте .env с вашими учётными данными Oracle
```

### Testing
```bash
# Запуск всех тестов с coverage
pytest -v --cov=src/oracle_to_excel --cov-report=html

# Запуск тестов конкретного модуля
pytest tests/test_config.py -v

# Тест модуля непосредственно
python -m src.oracle_to_excel.config --test
python -m src.oracle_to_excel.logger --test
```

### Code Quality
```bash
# Форматирование кода
ruff format src/ tests/

# Линтинг
ruff check src/ tests/

# Проверка типов
mypy src/
```

### Running the Exporter
```bash
# Базовый экспорт
python -m src.oracle_to_excel.main --table MY_TABLE --ses SES001

# С кастомной конфигурацией и логированием
python main.py -t MY_TABLE -s SES001 -c .env.prod -l exports.log

# С профилированием производительности
python main.py -t MY_TABLE -s SES001 --profile
```

## Critical Implementation Details

### Connection Pool (database.py)

```python
def create_connection_pool(config: dict) -> oracledb.ConnectionPool:
    """Create pool with min/max from config."""
    return oracledb.create_pool(
        user=config['ORACLE_USER'],
        password=config['ORACLE_PASSWORD'],
        dsn=config['ORACLE_DSN'],
        min=config['POOL_MIN'],
        max=config['POOL_MAX'],
        increment=config['POOL_INCREMENT'],
        getmode=oracledb.POOL_GETMODE_WAIT
    )
```

**Must-have параметры:**
- `getmode=POOL_GETMODE_WAIT` — ждать свободное соединение вместо ошибки
- `read_only=True` — для безопасности при получении подключения
- `cursor.arraysize = FETCH_ARRAY_SIZE` из конфигурации для оптимизации

### Data Transformation (transformers.py)

Oracle возвращает специфичные типы данные, которые надо конвертировать:
- `oracledb.LOB` → строка `.read()`
- `decimal.Decimal` → float/int
- `datetime` → остаётся как есть
- `bytes` → `.decode('utf-8')`
- `None` → `None` (пустые ячейки Excel)

Обработка ошибок конвертации должна логироваться, но не падать процесс.

### Main Entry Point (main.py)

```python
def main() -> int:
    """
    Returns: 0 (успех) или 1 (ошибка)
    """
```

**Обязательная последовательность:**
1. Parse arguments
2. Load & validate config
3. Setup logging
4. Test DB connection
5. Create connection pool
6. Generate output filename
7. Call export function
8. Close pool gracefully
9. Return exit code
```

## Project Structure

```
src/oracle_to_excel/
  __init__.py
  config.py           ✓ Реализовано
  logger.py           ✓ Реализовано
  database.py         (планируется)
  queries.py          (планируется)
  error_handler.py    (планируется)
  transformers.py     (планируется)
  excel_writer.py     (планируется)
  main.py             (stub только)

tests/
  __init__.py
  conftest.py         (фикстуры pytest)
  test_config.py
  test_database.py
  test_queries.py
  test_error_handler.py
  test_transformers.py
  test_excel_writer.py
  test_integration.py

.github/
  copilot-instructions.md ← Вы здесь
```

## Important Notes

1. **Паттерн match/case должен быть везде** — это стиль проекта (Python 3.14), не if/elif
2. **Все модули тестируемы** — используйте `if __name__ == '__main__'` блоки для selbat-test
3. **Логирование важно** — не используйте print(), всегда logger
4. **Type hints обязательны** — проект использует mypy
5. **Docstrings для всех функций** — Google style format
6. **Генераторы для больших данных** — не загружайте датасеты целиком в памяти
7. **Обработка ошибок Oracle специфична** — изучите ORA-коды из error_handler.py плана
8. **Маскирование паролей везде** — система встроена, но нужно помнить при логировании

## See Also

- `improved_plan.txt` — детальный план всех модулей
- `pyproject.toml` — зависимости и инструменты (pytest, ruff, mypy)
- `README.md` — документация для пользователей
