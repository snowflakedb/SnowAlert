"""Okta
collects X into a single-VARIANT table
"""

from runners.helpers import db, log
from runners.config import CONNECTOR_METADATA_TABLE

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


def connect(name, options):
    table_name = f'okta_{name}_connection'

    comment = f"""
---
module: okta
name: {table_name}
api_key: {options['api_key']}
subdomain: {options['subdomain']}
"""

    db.create_table(name=table_name, cols=OKTA_LANDING_TABLE_COLUMNS, comment=comment)
    return {
        'newStage': 'finalized',
        'newMessage': "Okta ingestion table created!",
    }


def ingest(table_name, options):
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'SSWS {options["api_key"]}'
    }

    url = f"https://{options['subdomain']}.okta.com/api/v1/logs"

    timestamp_query = f"""
    SELECT event_time
    FROM data.{table_name}
    WHERE event_time IS NOT NULL
    ORDER BY event_time DESC
    LIMIT 1
    """

    metadata = {
        'START_TIME': datetime.datetime.utcnow(),
        'TYPE': 'Okta',
        'TARGET_TABLE': table_name
    }

    try:
        ts = next(db.fetch(timestamp_query))['EVENT_TIME']

    except Exception:
        log.error(
            "Unable to find a timestamp of most recent Okta log, "
            "defaulting to one hour ago"
        )
        ts = datetime.datetime.now() - datetime.timedelta(hours=1)

    params = {'since': ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")}

    loaded = 0

    while 1:
        try:
            response = requests.get(url=url, headers=headers, params=params)
            result = response.json()
            if response.status_code != 200:
                log.fatal('OKTA REQUEST FAILED: ', response.text)
            loaded += process_logs(result, table_name)
            if result == []:
                break
            url = response.headers['Link'].split(', ')[1].split(';')[0][1:-1]

        except Exception as e:
            log.error("Error with Okta logs: ", e)
            db.record_metadata(metadata, table=CONNECTOR_METADATA_TABLE, e=e)

    metadata['ROW_COUNT'] = {'INSERTED': loaded}
    db.record_metadata(metadata, table=CONNECTOR_METADATA_TABLE)


def process_logs(logs, table_name):
    output = [(row, row['published']) for row in logs]
    db.insert(
        f"data.{table_name}",
        output,
        select='PARSE_JSON(column1), column2'
    )
    return len(output)


def test(name):
    yield {
        'check': 'everything works',
        'success': True,
    }
