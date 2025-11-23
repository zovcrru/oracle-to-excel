"""Verify password masking in validation errors."""

import tempfile
from pathlib import Path

from src.oracle_to_excel.env_config import load_config

print('Test 1: Invalid URI format (missing slashes)')
print('=' * 70)

with tempfile.TemporaryDirectory() as tmpdir:
    env_file = Path(tmpdir) / '.env'
    env_file.write_text(
        'DB_TYPE=postgresql\nDB_CONNECT_URI=postgresql:/postgres:pg@localhost:5433/postgres\n'
    )

    try:
        config = load_config(str(env_file))
        print('[FAIL] Should have raised validation error')
    except ValueError as e:
        error_msg = str(e)
        password = 'pg@'

        if password in error_msg:
            print('[FAIL] Password leaked in error message!')
            print(f'Error: {error_msg}')
        else:
            print('[PASS] Password is masked')
            print(f'Error (redacted): {error_msg[:100]}...')

print()
print('Test 2: URI with @ in password')
print('=' * 70)

with tempfile.TemporaryDirectory() as tmpdir:
    env_file = Path(tmpdir) / '.env'
    env_file.write_text(
        'DB_TYPE=postgresql\nDB_CONNECT_URI=postgresql://user:p@ssw@rd@localhost:5432/db\n'
    )

    try:
        config = load_config(str(env_file))
        print('[OK] Config loaded successfully')
    except ValueError as e:
        error_msg = str(e)
        password = 'p@ssw@rd'

        if password in error_msg:
            print(f'[FAIL] Password "{password}" leaked!')
            print(f'Error: {error_msg}')
        else:
            print('[PASS] Password is masked')
            print(f'Error (redacted): {error_msg[:100]}...')

print()
print('=' * 70)
