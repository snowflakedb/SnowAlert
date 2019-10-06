"""Tenable Settings
Collect Tenable Settings using a Service User’s API Key
"""

from tenable.io import TenableIO
from datetime import datetime

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

CONNECTION_OPTIONS = [
    {
        'type': 'select',
        'options': [{'value': 'user', 'label': "Tenable Users"}],
        'default': 'user',
        'name': 'connection_type',
        'title': "Settings Type",
        'prompt': "The type of Tenable Settings information you are ingesting to Snowflake.",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'token',
        'title': "Tenable API Token",
        'prompt': "The Tenable API Token",
        'placeholder': 'f1234764cd987654we543nt1x456b65a098a1df1233c2986c07efa700f9d2187',
        'required': True,
    },
    {
        'type': 'str',
        'name': 'secret',
        'title': "Tenable API Secret",
        'prompt': "The Secret Token for the Tenable API.",
        'required': True,
        'secret': True,
    },
]

USER_LANDING_TABLE = [
    ('USERNAME', 'STRING(250)'),
    ('ROLE', 'STRING(100)'),
    ('RAW', 'VARIANT'),
    ('SNAPSHOT_AT', 'TIMESTAMP_LTZ'),
    ('UUID', 'STRING(100)'),
    ('ID', 'STRING(100)'),
    ('USER_NAME', 'STRING(250)'),
    ('EMAIL', 'STRING(250)'),
    ('TYPE', 'STRING(100)'),
    ('PERMISSION', 'NUMBER'),
    ('LAST_LOGIN_ATTEMPT', 'TIMESTAMP_LTZ'),
    ('LOGIN_FAIL_COUNT', 'NUMBER'),
    ('LOGIN_FAIL_TOTAL', 'NUMBER'),
    ('ENABLED', 'BOOLEAN'),
    ('TWO_FACTOR', 'VARIANT'),
    ('LAST_LOGIN', 'TIMESTAMP_LTZ'),
    ('UUID_ID', 'STRING(100)'),
]


def ingest_users(tio, table_name):
    users = tio.users.list()
    timestamp = datetime.utcnow()

    for user in users:
        user['role'] = {
            16: 'Basic',
            24: 'Scan Operator',
            32: 'Standard',
            40: 'Scan Manager',
            64: 'Administrator',
        }.get(user['permissions'], 'unknown permissions {permissions}')

    db.insert(
        table=f'data.{table_name}',
        values=[
            (
                user.get('username', None),
                user.get('role', None),
                user,
                timestamp,
                user.get('uuid', None),
                user.get('id', None),
                user.get('user_name', None),
                user.get('email', None),
                user.get('type', None),
                user.get('permissions', None),
                user.get('last_login_attempt', None),
                user.get('login_fail_count', None),
                user.get('login_fail_total', None),
                user.get('enabled', None),
                user.get('two_factor', None),
                user.get('lastlogin', None),
                user.get('uuid_id', None),
            )
            for user in users
        ],
        select="""
            column1, column2, PARSE_JSON(column3), column4, column5, column6,
            column7, column8, column9, column10,
            to_timestamp(column11, 3)::timestamp_ltz, column12, column13,
            column14, PARSE_JSON(column15),
            to_timestamp(column16, 3)::timestamp_ltz, column17
        """,
    )


def create_user_table(connection_name, options):
    table_name = f'data.TENABLE_SETTINGS_{connection_name}_USER_CONNECTION'
    token = options['token']
    secret = options['secret']
    comment = f"""
---
module: tenable_settings
token: {token}
secret: {secret}
"""

    db.create_table(table_name, cols=USER_LANDING_TABLE, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {table_name} TO ROLE {SA_ROLE}')


def connect(connection_name, options):
    if options['connection_type'] == 'user':
        create_user_table(connection_name, options)

    return {
        'newStage': 'finalized',
        'newMessage': 'Landing table created for collectors to populate.',
    }


def ingest(table_name, options):
    tio = TenableIO(options['token'], options['secret'])
    if table_name.endswith('USER_CONNECTION'):
        ingest_users(tio, table_name)
