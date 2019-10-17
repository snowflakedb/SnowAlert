"""Tenable Settings
Collect Tenable Settings using a Service User’s API Key
"""

import requests
from .utils import yaml_dump
from tenable.io import TenableIO
from datetime import datetime, timezone

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE
from runners.utils import groups_of

CONNECTION_OPTIONS = [
    {
        'type': 'select',
        'options': [
            {'value': 'user', 'label': "Tenable Users"},
            {'value': 'agent', 'label': "Tenable Agents"},
            {'value': 'vuln', 'label': "Tenable Vulnerabilites"},
        ],
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

AGENT_LANDING_TABLE = [('RAW', 'VARIANT'), ('EXPORT_AT', 'TIMESTAMP_LTZ')]

VULN_LANDING_TABLE = [('RAW', 'VARIANT'), ('EXPORT_AT', 'TIMESTAMP_LTZ')]


def ingest_vulns(tio, table_name):
    last_export_time = next(
        db.fetch(f'SELECT MAX(export_at) as time FROM data.{table_name}')
    )['TIME']
    timestamp = datetime.now(timezone.utc)

    if (
        last_export_time is None
        or (timestamp - last_export_time).total_seconds() > 86400
    ):
        log.info("Exporting vulnerabilities...")
        vulns = tio.exports.vulns()

        for page in groups_of(10000, vulns):
            db.insert(
                table=f'data.{table_name}',
                values=[(vuln, timestamp) for vuln in page],
                select=db.derive_insert_select(VULN_LANDING_TABLE),
                columns=db.derive_insert_columns(AGENT_LANDING_TABLE),
            )
    else:
        log.info('Not time to import Tenable vulnerabilities yet')


def ingest_users(tio, table_name):
    users = tio.users.list()
    timestamp = datetime.now(timezone.utc)

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


def get_agent_data(tio, access, secret):
    headers = {"X-ApiKeys": f"accessKey={access}; secretKey={secret}"}

    r = requests.get(
        url='https://cloud.tenable.com/scanners/1/agent-groups', headers=headers
    )
    groups = r.json()['groups']

    agents = []
    for group in groups:
        agents += tio.agent_groups.details(group['id'])['agents']

    return agents


def ingest_agents(tio, table_name, options):
    last_export_time = next(
        db.fetch(f'SELECT MAX(export_at) as time FROM data.{table_name}')
    )['TIME']
    timestamp = datetime.now(timezone.utc)

    if (
        last_export_time is None
        or (timestamp - last_export_time).total_seconds() > 86400
    ):
        agents = get_agent_data(tio, options['token'], options['secret'])
        db.insert(
            table=f'data.{table_name}',
            values=[(agent, timestamp) for agent in agents],
            select=db.derive_insert_select(AGENT_LANDING_TABLE),
            columns=db.derive_insert_columns(AGENT_LANDING_TABLE),
        )
    else:
        log.info('Not time to import Tenable Agents')


def create_vuln_table(connection_name, options):
    table_name = f'data.tenable_settings_{connection_name}_vuln_connection'

    comment = yaml_dump(module='tenable_settings', **options)

    db.create_table(table_name, cols=VULN_LANDING_TABLE, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {table_name} TO ROLE {SA_ROLE}')


def create_user_table(connection_name, options):
    table_name = f'data.tenable_settings_{connection_name}_user_connection'
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


def create_agent_table(connection_name, options):
    table_name = f'tenable_settings_{connection_name}_agent_connection'

    comment = yaml_dump(module='tenable_settings', **options)

    db.create_table(table_name, cols=AGENT_LANDING_TABLE, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {table_name} TO ROLE {SA_ROLE}')


def connect(connection_name, options):
    if options['connection_type'] == 'user':
        create_user_table(connection_name, options)
    elif options['connection_type'] == 'agent':
        create_agent_table(connection_name, options)
    elif options['connection_type'] == 'vuln':
        create_vuln_table(connection_name, options)

    return {
        'newStage': 'finalized',
        'newMessage': 'Landing table created for collectors to populate.',
    }


def ingest(table_name, options):
    tio = TenableIO(options['token'], options['secret'])
    if table_name.endswith('USER_CONNECTION'):
        ingest_users(tio, table_name)
    elif table_name.endswith('AGENT_CONNECTION'):
        ingest_agents(tio, table_name, options)
    elif table_name.endswith('VULN_CONNECTION'):
        ingest_vulns(tio, table_name)
