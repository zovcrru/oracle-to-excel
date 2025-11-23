"""Конфигурация pytest для тестов env_config."""

import sys
from pathlib import Path

import pytest

# Добавляем корневую директорию проекта в sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
# Если env_config.py находится в src/, раскомментируйте:
sys.path.insert(0, str(project_root / 'src'))


@pytest.fixture(autouse=True)
def reset_environment(monkeypatch):
    """Автоматически очищает переменные окружения перед каждым тестом."""
    import os

    # Сохраняем критичные переменные
    critical_vars = ['PATH', 'HOME', 'USER', 'PYTHONPATH']
    saved_env = {var: os.getenv(var) for var in critical_vars if os.getenv(var)}

    # Очищаем все переменные окружения, кроме критичных
    for key in list(os.environ.keys()):
        if key not in critical_vars:
            monkeypatch.delenv(key, raising=False)

    yield

    # Восстанавливаем критичные переменные
    for var, value in saved_env.items():
        monkeypatch.setenv(var, value)
