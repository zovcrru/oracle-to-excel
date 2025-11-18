"""
Oracle to Excel Exporter.

Пакет для экспорта данных из Oracle и PostgreSQL в Excel файлы.
Использует функциональный подход и возможности Python 3.14.
"""

from __future__ import annotations

__version__ = '1.0.0'
__author__ = 'zovcrru'

# # Экспортируем основные функции из модулей
from oracle_to_excel.config import (
    ConfigDict,
    load_config,
    print_config_summary,
    restore_sensitive_data,
    validate_config,
)
from oracle_to_excel.logger import (
    get_logger,
    log_execution_time,
    log_function_call,
    setup_logging,
)

# Mark these names as used for static analysis (exported symbols)
_EXPORTS = (
    ConfigDict,
    load_config,
    print_config_summary,
    restore_sensitive_data,
    validate_config,
    get_logger,
    log_execution_time,
    log_function_call,
    setup_logging,
)

# Explicit `__all__` is omitted to avoid maintenance burden;
# consumers should import from submodules, e.g. `from oracle_to_excel.config import load_config`,
# or access via `oracle_to_excel.config`/`oracle_to_excel.logger`.
