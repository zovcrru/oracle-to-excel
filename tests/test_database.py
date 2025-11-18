"""
Тесты для модуля `database.py` — sqlite-подключения и валидация connection string.
"""

from __future__ import annotations

from oracle_to_excel.database import create_connection, validate_connection_string


def test_create_sqlite_connection_variants() -> None:
    variants = [
        'lice.sqlite3',
        'sqlite:///lice.sqlite3',
        'sqlite:///:memory:',
    ]

    for cs in variants:
        conn = create_connection(cs)
        try:
            # Убедимся, что соединение работает и выполняет простой запрос
            cur = conn.cursor()
            cur.execute('SELECT 1')
            row = cur.fetchone()
            assert row is not None
            # sqlite возвращает (1,)
            assert row[0] == 1
            cur.close()
        finally:
            conn.close()


def test_validate_connection_string() -> None:
    # Должны проходить
    valid = [
        'lice.sqlite3',
        'sqlite:///lice.sqlite3',
        'sqlite:///:memory:',
    ]

    for cs in valid:
        ok, msg = validate_connection_string(cs)
        assert ok, f'{cs} expected valid, got error: {msg}'

    # Неправильные строки
    invalid = ['', 'postgresql://']
    for cs in invalid:
        ok, _ = validate_connection_string(cs)
        assert not ok, f'{cs} expected invalid'
