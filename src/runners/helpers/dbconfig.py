from base64 import b64decode
from random import choice
from os import environ
import re
from string import ascii_uppercase


def random_string(n=10, alphabet=ascii_uppercase):
    return ''.join(choice(alphabet) for _ in range(n))


def format_p8_from_key(key, encrypted=False):
    '''Sometimes users put just the "meat" of the p8 key in the env var'''
    pk_type = 'ENCRYPTED PRIVATE' if encrypted else 'PRIVATE'
    start = f'-----BEGIN {pk_type} KEY-----'
    p8_formatted_key_body = '\n'.join(re.findall(r'.{64}', key))
    end = f'-----END {pk_type} KEY-----'
    return f'{start}\n{p8_formatted_key_body}\n{end}'


ENV = environ.get('SA_ENV', 'unset')
if ENV == 'test':
    tail = f'_{random_string(5)}'
    environ['SA_ENV'] = 'testing'
else:
    tail = ''

SA_KMS_REGION = environ.get('SA_KMS_REGION', "us-west-2")

# database & account properties
REGION = environ.get('REGION', "us-west-2")
REGION_SUBDOMAIN_POSTFIX = '' if REGION == 'us-west-2' else f'.{REGION}'
ACCOUNT = environ.get('SNOWFLAKE_ACCOUNT', '') + REGION_SUBDOMAIN_POSTFIX

USER = environ.get('SA_USER', "snowalert") + tail
PRIVATE_KEY_PASSWORD = environ.get('PRIVATE_KEY_PASSWORD', '').encode('utf-8')
pk = environ.get('PRIVATE_KEY')
PRIVATE_KEY = (
    (
        b64decode(pk)
        if pk.startswith('LS0t')  # "LS0t" is base64 of '---'
        else format_p8_from_key(pk, encrypted=PRIVATE_KEY_PASSWORD)
    )
    if pk
    else None
)

ROLE = environ.get('SA_ROLE', "snowalert") + tail
WAREHOUSE = environ.get('SA_WAREHOUSE', "snowalert") + tail
DATABASE = environ.get('SA_DATABASE', "snowalert") + tail

# connection properties
TIMEOUT = environ.get('SA_TIMEOUT', 500)
