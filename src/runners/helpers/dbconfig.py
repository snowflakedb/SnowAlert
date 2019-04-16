from base64 import b64decode
from os import environ

# database & account properties
REGION = environ.get('REGION', "us-west-2")
ACCOUNT = environ.get('SNOWFLAKE_ACCOUNT', '') + ('' if REGION == 'us-west-2' else f'.{REGION}')

USER = environ.get('SA_USER', "snowalert")
PRIVATE_KEY_PASSWORD = environ.get('PRIVATE_KEY_PASSWORD', '').encode('utf-8')
PRIVATE_KEY = b64decode(environ['PRIVATE_KEY']) if environ.get('PRIVATE_KEY') else None

ROLE = environ.get('SA_ROLE', "snowalert")
WAREHOUSE = environ.get('SA_WAREHOUSE', "snowalert")
DATABASE = environ.get('SA_DATABASE', "snowalert")

# connection properties
TIMEOUT = environ.get('SA_TIMEOUT', 500)
