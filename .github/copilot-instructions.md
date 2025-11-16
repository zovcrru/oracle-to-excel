# Copilot Instructions: oracle-to-excel

## Project Overview

**Oracle to Excel Exporter** — Python 3.14 приложение для экспорта данных из таблицы Oracle в форматированные Excel-файлы с логированием, обработкой ошибок и потоковой обработкой больших объемов данных.

Во всех перечислениях, после последнего пункта всегда ставь запятую
### Architecture
Проект использует **модульную архитектуру с функциональным подходом** (PEP 649, Python 3.14+):

- **`config.py`** ✓ — загрузка и валидация конфигурации из `.env` с TypedDict, pattern matching, маскированием паролей
- **`logger.py`** ✓ — централизованное логирование с ротацией, фильтрацией чувствительных данных, цветным выводом
- **`database.py`** (планируется) — пул подключений Oracle с управлением ресурсами
- **`queries.py`** (планируется) — построение и выполнение SQL запросов с потоком данных
- **`error_handler.py`** (планируется) — обработка ORA-ошибок, retry с jitter
- **`transformers.py`** (планируется) — конвертация Oracle типов в Python
- **`excel_writer.py`** (планируется) — streaming запись в Excel с форматированием
- **`main.py`** (stub) — точка входа с парсингом аргументов

**Ключевая архитектура:** Streaming-обработка данных через генераторы + фильтраторы (не загружать всё в памяти).

## Key Conventions & Patterns

### 1. Python 3.14 Features (Modern Syntax)
Проект **обязательно использует** Python 3.14+ фичи (не просто может, а требует):

- **Pattern Matching** (`match/case`) вместо `if/elif` для всех логик
- **Type Aliases** — `type LogLevel = str | int` (PEP 695)
- **Generic функции** — Generic типы с ParamSpec для декораторов
- **TypedDict с NotRequired** — структурированная конфигурация
- **Deferred annotations** — `from __future__ import annotations` в каждом модуле

**Примеры из кода (config.py):**
```python
# Pattern matching для обработки типов (ОБЯЗАТЕЛЕН вместо if/elif)
match default_value:
    case int():
        config[param] = _parse_int_param(param, env_value, default_value, logger)
    case str():
        config[param] = env_value if env_value else default_value

# Generic функция с Type Parameter Syntax (Python 3.14)
def get_config_value(config: ConfigDict, key: str, default=None):
    return config.get(key, default)
```

### 2. Configuration Management Pattern
**Файл: `config.py`**

Конфигурация загружается ровно один раз на старте и имеет 3 этапа:

1. **Загрузка из .env** → `load_config(env_file: str = '.env')`
   - Использует `python-dotenv.load_dotenv()`
   - Проверяет обязательные параметры (frozenset `REQUIRED_CONFIG`)
   - Загружает опциональные с значениями по умолчанию (`DEFAULT_CONFIG: Mapping`)
   - Маскирует пароли автоматически через `_mask_sensitive_data()`

2. **Валидация** → `validate_config(config) -> tuple[bool, list[str]]`
   - Проверяет наличие обязательных параметров
   - Валидирует диапазоны чисел с pattern matching
   - Проверяет логическую согласованность (POOL_MIN <= POOL_MAX)
   - Возвращает кортеж (status, error_list)

3. **Восстановление секретов** → `restore_sensitive_data(config)`
   - Восстанавливает оригинальные пароли перед использованием БД
   - Вызывается ПЕРЕД передачей config в database модуль

**Добавление новых параметров:**
- Обновить `ConfigDict` TypedDict
- Добавить в `REQUIRED_CONFIG` если обязательный
- Добавить в `DEFAULT_CONFIG` если опциональный
- Добавить валидацию в `validate_config()` с pattern matching

### 3. Logging System (CRITICAL!)

**Файл: `logger.py`**

Логирование централизованное и обязательное для всех модулей:

```python
# ВСЕГДА используйте get_logger() в каждом модуле
from logger import get_logger
logger = get_logger('module_name')  # автоматически добавляет префикс 'oracle_exporter.'
```

**Особенности реализации:**
- Цветной вывод в консоль (ANSI коды) на Linux/macOS
- Ротирующийся файл лога (10MB максимум, 3 backup файла)
- Автоматическое маскирование паролей через regex фильтр
- Pattern matching для конфигурации обработчиков

**Декораторы для логирования:**
```python
# Логирует время выполнения функции
@log_execution_time
def slow_operation():
    ...

# Логирует вызовы с аргументами и результатом
@log_function_call(log_args=True, log_result=False)
def calculate(x: int, y: int) -> int:
    return x + y

# Контекстное логирование (добавляет metadata)
context_log = create_context_logger(logger, session='SES001', user='admin')
context_log('INFO', 'Operation started')
```

**Маскирование данных работает автоматически:**
```python
# Это будет залогировано как 'password=***' и 'token=***'
logger.info('Connection with password=secret123 and token=abc456')
```

### 4. Module Testing Pattern

Каждый модуль имеет встроенный self-test в конце:

```python
# В конце каждого модуля
def _test_module() -> None:
    """Простой тест модуля."""
    logger = setup_logging('DEBUG', console_output=True)
    # ... тесты здесь ...

if __name__ == '__main__':
    match sys.argv:
        case [_, '--test']:
            _test_module()
        case [_, '--create-example']:  # для config.py
            create_env_example()
```

Запуск: `python -m src.oracle_to_excel.config --test`

## Development Workflow

### Quick Start
```bash
# Установка проекта с зависимостями
pip install -e .
pip install -r requirements-dev.txt

# Создание .env файла
python -m src.oracle_to_excel.config --create-example
# Отредактируйте .env с вашими учётными данными Oracle
```

### Testing & Code Quality
```bash
# Тест одного модуля (встроенный self-test)
python -m src.oracle_to_excel.config --test
python -m src.oracle_to_excel.logger --test

# Полное тестирование с coverage
pytest -v --cov=src/oracle_to_excel --cov-report=html

# Код-стайл и линтинг (ruff вместо flake8)
ruff format src/ tests/
ruff check src/ tests/

# Проверка типов
mypy src/
```

### Running the App
```bash
# Базовый экспорт (когда будет реализовано)
python -m src.oracle_to_excel.main --table MY_TABLE --ses SES001

# С кастомным выходным файлом
python main.py -t MY_TABLE -s SES001 -o exports/custom.xlsx

# С логированием в файл
python main.py -t MY_TABLE -s SES001 -l exports.log
```

## Critical Implementation Details

### Actual Code Patterns (from config.py & logger.py)

**Обработка исключений с Pattern Matching (Python 3.14):**
```python
# Вместо try-except используйте match где возможно
match env_path.exists():
    case True:
        load_dotenv(env_path)
    case False:
        raise FileNotFoundError(f"Файл не найден: {env_path}")
```

**Connection Pool (когда реализуете database.py):**
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

**Data Transformation (transformers.py):**
Oracle типы требуют конвертации:
- `oracledb.LOB` → строка `.read()`
- `decimal.Decimal` → float/int
- `datetime.datetime` → остаётся как есть
- `bytes` → `.decode('utf-8')`
- `None` → остаётся None

**Streaming Excel Writing:**
Используйте генераторы для больших датасетов:
```python
def execute_query_stream(connection, query, params, fetch_size: int):
    cursor = connection.cursor()
    cursor.arraysize = fetch_size  # из конфига
    cursor.execute(query, params)
    
    metadata = cursor.description
    while True:
        rows = cursor.fetchmany(fetch_size)
        if not rows:
            break
        yield (metadata, rows)
    cursor.close()
```

**Main Entry Point (main.py) должен возвращать exit code:**
```python
def main() -> int:
    """Returns: 0 (success) or 1 (error)."""
    # 1. Parse arguments
    # 2. Load & validate config
    # 3. Setup logging
    # 4. Test DB connection
    # 5. Create connection pool
    # 6. Call export function
    # 7. Close pool gracefully
    return 0  # or 1 on error
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

.github/
  copilot-instructions.md

pyproject.toml        - конфигурация проекта, зависимости, pytest, ruff, mypy
requirements-dev.txt  - зависимости для разработки (создать при необходимости)
```

## Important Notes

1. **Паттерн match/case должен быть везде** — это стиль проекта (Python 3.14), не if/elif
2. **Все модули тестируемы** — используйте `if __name__ == '__main__'` блоки для self-test
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
