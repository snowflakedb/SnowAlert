"""Tenable.io
Collect Tenable.io Data using a Service User’s API Key
"""

from datetime import datetime, timezone
import requests
from tenable.io import TenableIO

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE
from runners.utils import groups_of

from .utils import yaml_dump

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
        'title': "Data Type",
        'prompt': "The type of Tenable information you are ingesting to Snowflake.",
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

TIO = None  # connection created in `ingest` below
GET = None


def ingest_vulns(table_name):
    last_export_time = next(
        db.fetch(f'SELECT MAX(export_at) as time FROM data.{table_name}')
    )['TIME']
    timestamp = datetime.now(timezone.utc)

    if (
        last_export_time is None
        or (timestamp - last_export_time).total_seconds() > 86400
    ):
        log.info("Exporting vulnerabilities...")
        vulns = TIO.exports.vulns()

        for page in groups_of(10000, vulns):
            db.insert(
                table=f'data.{table_name}',
                values=[(vuln, timestamp) for vuln in page],
                select=db.derive_insert_select(VULN_LANDING_TABLE),
                columns=db.derive_insert_columns(AGENT_LANDING_TABLE),
            )
    else:
        log.info('Not time to import Tenable vulnerabilities yet')


def ingest_users(table_name):
    users = TIO.users.list()
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


def get_group_ids():
    return GET('agent-groups', 'groups', 10, 0)


def get_agent_data():
    groups = get_group_ids()
    for g in groups:
        agents = GET(f'agent-groups/{g["id"]}', 'agents', 5000)
        yield from agents


def ingest_agents(table_name, options):
    last_export_time = next(
        db.fetch(f'SELECT MAX(export_at) as time FROM data.{table_name}')
    )['TIME']
    timestamp = datetime.now(timezone.utc)

    if (
        last_export_time is None
        or (timestamp - last_export_time).total_seconds() > 86400
    ):
        agents = {a['uuid']: a for a in get_agent_data()}.values()
        for page in groups_of(10000, agents):
            db.insert(
                table=f'data.{table_name}',
                values=[(agent, timestamp) for agent in page],
                select=db.derive_insert_select(AGENT_LANDING_TABLE),
                columns=db.derive_insert_columns(AGENT_LANDING_TABLE),
            )
    else:
        log.info('Not time to import Tenable Agents')


def connect(connection_name, options):
    ctype = options['connection_type']
    ctable = f'data.tenable_settings_{connection_name}_{ctype}_connection'
    cols = {
        'user': USER_LANDING_TABLE,
        'agent': AGENT_LANDING_TABLE,
        'vuln': VULN_LANDING_TABLE,
    }[ctype]
    comment = yaml_dump(module='tenable_settings', **options)

    db.create_table(ctable, cols=cols, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {ctable} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': 'Landing table created for collectors to populate.',
    }


def ingest(table_name, options):
    token = options['token']
    secret = options['secret']

    global TIO, GET

    TIO = TenableIO(token, secret)

    def GET(resource, name, limit=100, offset=0):
        response = requests.get(
            url=f'https://cloud.tenable.com/scanners/1/{resource}',
            params={
                'limit': limit,
                'offset': offset,
            },
            headers={
                "X-ApiKeys": f"accessKey={token}; secretKey={secret}"
            },
        )
        result = response.json()
        elements = result.get(name)

        if elements is None:
            log.error(f'no {name} in :', result)
            return

        yield from elements

        pages = result.get('pagination', {})
        total = pages.get('total', 0)
        limit = pages.get('limit', 0)
        offset = pages.get('offset', 0)

        if total > limit + offset:
            yield from GET(resource, name, limit, offset + limit)

    if table_name.endswith('USER_CONNECTION'):
        ingest_users(table_name)

    elif table_name.endswith('AGENT_CONNECTION'):
        ingest_agents(table_name, options)

    elif table_name.endswith('VULN_CONNECTION'):
        ingest_vulns(table_name)
