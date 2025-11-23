#!/usr/bin/env python
"""Debug script to test mask_connection_string."""

from src.oracle_to_excel.env_config import Settings

test_uri = 'postgresql:/postgres:pg@localhost:5433/postgres'
masked = Settings.mask_connection_string(test_uri)

print(f'Original URI: {test_uri}')
print(f'Masked URI:   {masked}')
print(f'Are they same: {test_uri == masked}')
print(f'Contains ***: {"***" in masked}')

# Trace through the logic manually
print('\n=== Manual trace ===')
if '://' in test_uri:
    scheme_part, rest = test_uri.split('://', 1)
    print(f'Scheme: {scheme_part!r}')
    print(f'Rest: {rest!r}')

    if '@' in rest:
        last_at_idx = rest.rfind('@')
        credentials_part = rest[:last_at_idx]
        host_part = rest[last_at_idx + 1 :]
        print(f'Last @ at index: {last_at_idx}')
        print(f'Credentials: {credentials_part!r}')
        print(f'Host: {host_part!r}')

        if ':' in credentials_part:
            colon_idx = credentials_part.find(':')
            user_part = credentials_part[:colon_idx]
            print(f'First : at index: {colon_idx}')
            print(f'User part: {user_part!r}')

            result = f'{scheme_part}://{user_part}:***@{host_part}'
            print(f'Result: {result!r}')
        else:
            print("No ':' in credentials_part")
    else:
        print("No '@' in rest")
else:
    print("No '://' in URI")
