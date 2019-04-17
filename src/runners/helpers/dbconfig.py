from base64 import b64decode
from random import choice
from os import environ
from string import ascii_uppercase


def random_string(n=10, alphabet=ascii_uppercase):
    return ''.join(choice(alphabet) for _ in range(n))


ENV = environ.get('SA_ENV', 'unset')
if ENV == 'test':
    tail = f'_{random_string(5)}'
    environ['SA_ENV'] = 'testing'
else:
    tail = ''

# database & account properties
REGION = environ.get('REGION', "us-west-2")
REGION_SUBDOMAIN_POSTFIX = '' if REGION == 'us-west-2' else f'.{REGION}'
ACCOUNT = environ.get('SNOWFLAKE_ACCOUNT', '') + REGION_SUBDOMAIN_POSTFIX

USER = environ.get('SA_USER', "snowalert") + tail
PRIVATE_KEY_PASSWORD = environ.get('PRIVATE_KEY_PASSWORD', '').encode('utf-8')
PRIVATE_KEY = b64decode(environ['PRIVATE_KEY']) if environ.get('PRIVATE_KEY') else None

ROLE = environ.get('SA_ROLE', "snowalert") + tail
WAREHOUSE = environ.get('SA_WAREHOUSE', "snowalert") + tail
DATABASE = environ.get('SA_DATABASE', "snowalert") + tail

# connection properties
TIMEOUT = environ.get('SA_TIMEOUT', 500)
