"""Okta
collects Okta API v1 logs into a single-VARIANT table
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE
from runners.config import DC_METADATA_TABLE

import datetime
import requests

CONNECTION_OPTIONS = [
    {
        'name': 'subdomain',
        'type': 'str',
        'postfix': '.okta.com',
        'prefix': 'https://',
        'placeholder': 'domain_name',
        'required': True
    },
    {
        'name': 'api_key',
        'type': 'str',
        'secret': True,
        'required': True
    },
]

OKTA_LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ')
]


def connect(connection_name, options):
    table_name = f'okta_{connection_name}_connection'
    landing_table = f'data.{table_name}'
    api_key = options["api_key"]
    subdomain = options["subdomain"]

    comment = f"""
---
module: okta
api_key: {api_key}
subdomain: {subdomain}
"""

    db.create_table(name=landing_table, cols=OKTA_LANDING_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {table_name} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "Okta ingestion table created!",
    }


def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    api_key = options["api_key"]
    subdomain = options["subdomain"]

    url = f"https://{subdomain}.okta.com/api/v1/logs"
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'SSWS {api_key}'
    }

    metadata = {
        'TYPE': 'Okta',
        'START_TIME': datetime.datetime.utcnow(),
        'LANDING_TABLE': table_name,
        'ROW_COUNT': {
            'INSERTED': 0,
        }
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
        try:
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

            metadata['ROW_COUNT']['INSERTED'] += len(result)

            url = response.headers['Link'].split(', ')[1].split(';')[0][1:-1]

        except Exception as e:
            log.error("Error loading Okta logs: ", e)
            db.record_metadata(metadata, table=DC_METADATA_TABLE, e=e)
            return

    db.record_metadata(metadata, table=DC_METADATA_TABLE)


def test(name):
    yield {
        'check': 'everything works',
        'success': True,
    }
