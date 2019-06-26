"""Okta System Log
Collect Okta activity logs
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

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

LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ')
]


def connect(connection_name, options):
    table_name = f'okta_{connection_name}_connection'
    landing_table = f'data.{table_name}'
    api_key = options['api_key']
    subdomain = options['subdomain']

    comment = f'''
---
module: okta
api_key: {api_key}
subdomain: {subdomain}
'''

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "Okta ingestion table created!",
    }


def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    api_key = options['api_key']
    subdomain = options['subdomain']

    url = f'https://{subdomain}.okta.com/api/v1/logs'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'SSWS {api_key}'
    }

    ts = db.fetch_latest(landing_table, 'event_time')
    if ts is None:
        log.error(
            "Unable to find a timestamp of most recent Okta log, "
            "defaulting to one hour ago"
        )
        ts = datetime.datetime.now() - datetime.timedelta(hours=1)

    params = {'since': ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")}

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
            select='PARSE_JSON(column1), column2'
        )

        log.info(f'Inserted {len(result)} rows.')
        yield len(result)

        url = response.headers['Link'].split(', ')[1].split(';')[0][1:-1]
