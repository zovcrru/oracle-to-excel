import sys

from sqlalchemy.engine.url import make_url


def mask_connection_string(uri: str) -> str:
    if not uri:
        return uri

    uri = uri.strip()
    if '://' not in uri:
        return uri

    try:
        parsed = make_url(uri)
        print(parsed)
        if parsed.password:
            # Reconstruct with masked password
            masked_password = '***'
            return str(parsed._replace(password=masked_password))
        return uri
    except Exception:
        return uri


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python passwd.py <connection_string>')
        sys.exit(1)

    uri = sys.argv[1]
    print(mask_connection_string(uri))
