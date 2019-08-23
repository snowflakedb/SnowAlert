"""Okta System Log
Collect Okta activity logs using an API Token
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
        'prompt': "The subdomain of your Okta login page",
        'type': 'str',
        'postfix': ".okta.com",
        'prefix': "https://",
        'placeholder': "account-name",
        'required': True
    },
    {
        'name': 'api_key',
        'title': "API Token",
        'prompt': "This available in your Okta settings",
        'type': 'str',
        'secret': True,
        'required': True
    },
]

LANDING_LOG_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ')
]

LANDING_USER_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ')
]

LANDING_GROUP_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ')
]


def connect(connection_name, options):
    table_name = f'okta_{connection_name}'
    landing_log_table = f'data.{table_name}_connection'
    landing_user_table = f'data.{table_name}_users_connection'
    landing_group_table = f'data.{table_name}_groups_connection'

    comment = yaml_dump(
        module='okta',
        **options
    )

    db.create_table(name=landing_log_table, cols=LANDING_LOG_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_log_table} TO ROLE {SA_ROLE}')

    db.create_table(name=landing_user_table, cols=LANDING_USER_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_user_table} TO ROLE {SA_ROLE}')

    db.create_table(name=landing_group_table, cols=LANDING_GROUP_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_group_table} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': "Okta ingestion table, user table, group table created!",
    }


def ingest(table_name, options):
    ingest_type = ''
    if table_name.endswith('_USERS_CONNECTION'):
        ingest_type = 'users'
    elif table_name.endswith('_GROUPS_CONNECTION'):
        ingest_type = 'groups'
    else:
        ingest_type = 'logs'

    landing_table = f'data.{table_name}'
    api_key = options['api_key']
    subdomain = options['subdomain']

    url = {
        'users': f'https://{subdomain}.okta.com/api/v1/users',
        'groups': f'https://{subdomain}.okta.com/api/v1/groups',
        'logs': f'https://{subdomain}.okta.com/api/v1/logs'
    }

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'SSWS {api_key}'
    }

    timestamp = datetime.datetime.utcnow()

    if ingest_type == 'groups':
        response = requests.get(url=url[ingest_type], headers=headers)

        result = response.json()

        for row in result:
            row['users'] = requests.get(url=row['_links']['users']['href'], headers=headers).json()

        db.insert(
            landing_table,
            values=[(row, timestamp) for row in result],
            select='PARSE_JSON(column1), column2'
        )

        log.info(f'Inserted {len(result)} rows.')
        yield len(result)
    else:
        while 1:
            response = requests.get(url=url[ingest_type], headers=headers)
            if response.status_code != 200:
                log.error('OKTA REQUEST FAILED: ', response.text)
                return

            result = response.json()
            if result == []:
                break

            if ingest_type == 'logs':
                db.insert(
                    landing_table,
                    values=[(row, row['published']) for row in result],
                    select='PARSE_JSON(column1), column2'
                )
            else:
                db.insert(
                    landing_table,
                    values=[(row, timestamp) for row in result],
                    select='PARSE_JSON(column1), column2'
                )

            log.info(f'Inserted {len(result)} rows.')
            yield len(result)

            url[ingest_type] = ''
            links = requests.utils.parse_header_links(response.headers['Link'])
            for link in links:
                if link['rel'] == 'next':
                    url[ingest_type] = link['url']

            if len(url[ingest_type]) == 0:
                break
