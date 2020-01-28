"""Tenable.io
Collect Tenable.io Data using a Service User’s API Key
"""

from datetime import datetime, timezone
import requests
from tenable.io import TenableIO

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

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
    ('username', 'STRING(250)'),
    ('role', 'STRING(100)'),
    ('raw', 'VARIANT'),
    ('snapshot_at', 'TIMESTAMP_LTZ'),
    ('uuid', 'STRING(100)'),
    ('id', 'STRING(100)'),
    ('user_name', 'STRING(250)'),
    ('email', 'STRING(250)'),
    ('type', 'STRING(100)'),
    ('permission', 'NUMBER'),
    ('last_login_attempt', 'TIMESTAMP_LTZ'),
    ('login_fail_count', 'NUMBER'),
    ('login_fail_total', 'NUMBER'),
    ('enabled', 'BOOLEAN'),
    ('two_factor', 'VARIANT'),
    ('last_login', 'TIMESTAMP_LTZ'),
    ('uuid_id', 'STRING(100)'),
]

AGENT_LANDING_TABLE = [('raw', 'VARIANT'), ('export_at', 'TIMESTAMP_LTZ')]

VULN_LANDING_TABLE = [('raw', 'VARIANT'), ('export_at', 'TIMESTAMP_LTZ')]

TIO = None  # connection created in `ingest` below
GET = None


def ingest_vulns(table_name):
    last_export_time = next(
        db.fetch(f'SELECT MAX(export_at) AS time FROM data.{table_name}')
    )['TIME']
    timestamp = datetime.now(timezone.utc)

    if (
        last_export_time is None
        or (timestamp - last_export_time).total_seconds() > 86400
    ):
        log.debug('TIO export vulns')

        # insert empty row...
        db.insert(f'data.{table_name}', [{'export_at': timestamp}])

        # ...because this line takes awhile
        vulns = TIO.exports.vulns()

        rows = [{'raw': v, 'export_at': timestamp} for v in vulns]
        db.insert(f'data.{table_name}', rows)
        return len(rows)

    else:
        log.info('Not time to import Tenable vulnerabilities yet')
        return 0


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

    return len(users)


def get_agent_data():
    scanners = list(GET('scanners'))
    log.debug(f'got {len(scanners)} scanners')
    for s in scanners:
        sid = s['id']
        agents = list(GET(f'scanners/{sid}/agents', 'agents', 5000))
        log.debug(f'scanner {sid} has {len(agents)} agents')
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
        all_agents = sorted(get_agent_data(), key=lambda a: a.get('last_connect', 0))
        unique_agents = {a['uuid']: a for a in all_agents}.values()
        rows = [{'raw': ua, 'export_at': timestamp} for ua in unique_agents]
        log.debug(f'inserting {len(unique_agents)} unique (by uuid) agents')
        db.insert(f'data.{table_name}', rows)
        return len(rows)
    else:
        log.info('Not time to import Tenable Agents')
        return 0


def connect(connection_name, options):
    ctype = options['connection_type']
    ctable = f'data.tenable_io_{connection_name}_{ctype}_connection'
    cols = {
        'user': USER_LANDING_TABLE,
        'agent': AGENT_LANDING_TABLE,
        'vuln': VULN_LANDING_TABLE,
    }[ctype]
    comment = yaml_dump(module='tenable_io', **options)

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

    def GET(resource, key=None, limit=100, offset=0):
        if key is None:
            key = resource
        log.debug(f'GET {resource} limit={limit} offset={offset}')
        response = requests.get(
            url=f'https://cloud.tenable.com/{resource}',
            params={'limit': limit, 'offset': offset},
            headers={"X-ApiKeys": f"accessKey={token}; secretKey={secret}"},
        )
        result = response.json()
        elements = result.get(key)

        if elements is None:
            log.error(f'no {key} in :', result)
            return

        yield from elements

        pages = result.get('pagination', {})
        total = pages.get('total', 0)
        limit = pages.get('limit', 0)
        offset = pages.get('offset', 0)

        if total > limit + offset:
            yield from GET(resource, key, limit, offset + limit)

    if table_name.endswith('_USER_CONNECTION'):
        return ingest_users(table_name)

    elif table_name.endswith('_AGENT_CONNECTION'):
        return ingest_agents(table_name, options)

    elif table_name.endswith('_VULN_CONNECTION'):
        return ingest_vulns(table_name)
