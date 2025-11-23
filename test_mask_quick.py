#!/usr/bin/env python3
"""Quick test for mask_connection_string fix."""

from src.oracle_to_excel.env_config import Settings

test_cases = [
    ('oracle://scott:tiger@localhost:1521/ORCL', 'oracle://scott:***@localhost:1521/ORCL'),
    (
        'postgresql://admin:SecretPass@db.local:5432/mydb',
        'postgresql://admin:***@db.local:5432/mydb',
    ),
    ('postgresql://user:P@ssw0rd@host:5432/db', 'postgresql://user:***@host:5432/db'),
    (
        'oracle+cx_oracle://user:password@localhost:1521/xe',
        'oracle+cx_oracle://user:***@localhost:1521/xe',
    ),
    (
        'postgresql+psycopg2://user:pass123@db:5432/mydb',
        'postgresql+psycopg2://user:***@db:5432/mydb',
    ),
    ('oracle://user@localhost:1521/ORCL', 'oracle://user@localhost:1521/ORCL'),
    ('sqlite:///path/to/database.db', 'sqlite:///path/to/database.db'),
]

print('=' * 70)
print('Testing mask_connection_string() implementation')
print('=' * 70)
passed = 0
failed = 0

for uri, expected in test_cases:
    result = Settings.mask_connection_string(uri)
    is_pass = result == expected
    status = 'PASS' if is_pass else 'FAIL'

    print(f'\n[{status}] Input:    {uri}')
    if not is_pass:
        print(f'       Expected: {expected}')
        print(f'       Got:      {result}')
        failed += 1
    else:
        passed += 1

print(f'\n{"=" * 70}')
print(f'Results: {passed} passed, {failed} failed out of {len(test_cases)}')
print(f'{"=" * 70}')
