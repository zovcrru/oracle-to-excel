"""
Oracle to Excel Exporter.

Пакет для экспорта данных из Oracle и PostgreSQL в Excel файлы.
Использует функциональный подход и возможности Python 3.14.
"""

from __future__ import annotations

__version__ = '1.0.0'
__author__ = 'Oracle Excel Team'

# Экспортируем основные функции из модулей
from oracle_to_excel.config import (
    ConfigDict,
    load_config,
    restore_sensitive_data,
    validate_config,
)
from oracle_to_excel.logger import (
    get_logger,
    log_execution_time,
    log_function_call,
    setup_logging,
)

# from oracle_to_excel.database import (
#     create_connection,
#     close_connection,
#     test_connection,
#     get_connection,
#     detect_db_type,
#     validate_connection_string,
#     DBType,
# )

__all__ = [
    # Logger
    'setup_logging',
    'get_logger',
    'log_execution_time',
    'log_function_call',
    # Config
    'load_config',
    'validate_config',
    'restore_sensitive_data',
    'ConfigDict',
    # Database
    # 'create_connection',
    # 'close_connection',
    # 'test_connection',
    # 'get_connection',
    # 'detect_db_type',
    # 'validate_connection_string',
    # 'DBType',
]
