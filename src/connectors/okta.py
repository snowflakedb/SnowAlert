"""Okta System Log and Inventory
Collect Okta activity logs, users, and groups using an API Token
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE
from .utils import yaml_dump

import datetime
import requests

CONNECTION_OPTIONS = [
    {
        'name': 'subdomain',
        'title': "Okta Account Name",
        'prompt': "The domain of your Okta login page",
        'type': 'str',
        'prefix': "https://",
        'placeholder': "your-org.okta.com",
        'required': True,
    },
    {
        'name': 'api_key',
        'title': "API Token",
        'prompt': "This available in your Okta settings",
        'type': 'str',
        'secret': True,
        'required': True,
    },
]

LANDING_LOG_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP'),
]

LANDING_USER_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP'),
]

LANDING_GROUP_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP'),
]


def connect(connection_name, options):
    table_name = 'okta' + (
        '' if connection_name in ('default', 'undefined') else f'_{connection_name}'
    )
    landing_log_table = f'data.{table_name}_system_log_connection'
    landing_user_table = f'data.{table_name}_users_connection'
    landing_group_table = f'data.{table_name}_groups_connection'

    comment = yaml_dump(module='okta', **options)

    db.create_table(
        name=landing_log_table,
        cols=LANDING_LOG_TABLE_COLUMNS,
        comment=comment,
        rw_role=SA_ROLE,
    )

    db.create_table(
        name=landing_user_table,
        cols=LANDING_USER_TABLE_COLUMNS,
        comment=comment,
        rw_role=SA_ROLE,
    )

    db.create_table(
        name=landing_group_table,
        cols=LANDING_GROUP_TABLE_COLUMNS,
        comment=comment,
        rw_role=SA_ROLE,
    )

    return {
        'newStage': 'finalized',
        'newMessage': "Okta ingestion table, user table, group table created!",
    }


def ingest_users(url, headers, landing_table, now):
    while 1:
        response = requests.get(url=url, headers=headers)
        if response.status_code != 200:
            log.error('OKTA REQUEST FAILED: ', response.text)
            return

        result = response.json()
        if result == []:
            break

        db.insert(
            landing_table, [{'raw': row, 'event_time': now} for row in result],
        )

        log.info(f'Inserted {len(result)} rows.')
        yield len(result)

        url = ''
        links = requests.utils.parse_header_links(response.headers['Link'])
        for link in links:
            if link['rel'] == 'next':
                url = link['url']

        if len(url) == 0:
            break


def ingest(table_name, options):
    ingest_type = (
        'users'
        if table_name.endswith('_USERS_CONNECTION')
        else 'groups'
        if table_name.endswith('_GROUPS_CONNECTION')
        else 'logs'
    )

    landing_table = f'data.{table_name}'
    api_key = options['api_key']
    subdomain = options['subdomain']

    domain = subdomain if '.' in subdomain else f'{subdomain}.okta.com'

    ingest_urls = {
        'users': f'https://{domain}/api/v1/users',
        'deprovisioned_users': f'https://{domain}/api/v1/users?filter=status+eq+\"DEPROVISIONED\"',
        'groups': f'https://{domain}/api/v1/groups',
        'logs': f'https://{domain}/api/v1/logs',
    }

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'SSWS {api_key}',
    }

    now = datetime.datetime.utcnow()

    if ingest_type == 'groups':
        response = requests.get(url=ingest_urls[ingest_type], headers=headers)

        result = response.json()

        for row in result:
            try:
                row['users'] = requests.get(
                    url=row['_links']['users']['href'], headers=headers
                ).json()
            except TypeError:
                log.info(row)
                raise

        db.insert(
            landing_table, [{'raw': row, 'event_time': now} for row in result],
        )

        log.info(f'Inserted {len(result)} rows.')
        yield len(result)

    elif ingest_type == 'users':
        yield from ingest_users(ingest_urls['users'], headers, landing_table, now)
        yield from ingest_users(
            ingest_urls['deprovisioned_users'], headers, landing_table, now
        )

    else:
        ts = db.fetch_latest(landing_table, 'event_time')
        if ts is None:
            log.error(
                "Unable to find a timestamp of most recent Okta log, "
                "defaulting to one hour ago"
            )
            ts = datetime.datetime.now() - datetime.timedelta(hours=1)

        params = {
            'since': ts.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            'limit': 500,
            'sortOrder': 'ASCENDING',
        }

        i = 0
        url = ingest_urls[ingest_type]
        while 1:
            response = requests.get(url=url, headers=headers, params=params)
            if response.status_code != 200:
                log.error('OKTA REQUEST FAILED: ', response.text)
                return

            result = response.json()
            if result == []:
                break

            db.insert(
                landing_table,
                values=[(row, row['published']) for row in result],
                select='PARSE_JSON(column1), column2',
            )

            log.info(f'Inserted {len(result)} rows from page {i}.')
            i += 1
            yield len(result)

            url = ''
            links = requests.utils.parse_header_links(response.headers['Link'])
            for link in links:
                if link['rel'] == 'next':
                    url = link['url']

            if len(url) == 0:
                break
