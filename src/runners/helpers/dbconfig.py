from base64 import b64decode
import os

# database & account properties
REGION = os.environ.get('REGION', "us-west-2")
ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT', '') + ('' if REGION == 'us-west-2' else f'.{REGION}')

USER = os.environ.get('SA_USER', "snowalert")
PRIVATE_KEY_PASSWORD = os.environ.get('PRIVATE_KEY_PASSWORD', '').encode('utf-8')
PRIVATE_KEY = b64decode(os.environ['PRIVATE_KEY']) if os.environ.get('PRIVATE_KEY') else None

ROLE = os.environ.get('SA_ROLE', "snowalert")
WAREHOUSE = os.environ.get('SA_WAREHOUSE', "snowalert")
DATABASE = os.environ.get('SA_DATABASE', "snowalert")

# connection properties
TIMEOUT = 500
