from runners.helpers import db, log
from runners.config import CONNECTOR_METADATA_TABLE

import datetime
import requests

CONNECTION_OPTIONS = [
    {'name': 'subdomain', 'type': 'str'},
    {'name': 'api_key', 'type': 'str', 'secret': False},
]

OKTA_LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ')
]


def connect(name, options):
    name = f'OKTA_{name}'

    comment = f"""
---
name: {name}
module: okta
api_key: {options['api_key']}
subdomain: {options['subdomain']}
"""

    results = {}
    try:
        db.create_table(name=name + "_CONNECTION", cols=OKTA_LANDING_TABLE_COLUMNS, comment=comment)
        results['events_table'] = 'success'
    except Exception as e:
        results['events_table'] = 'failure'
        results['exception'] = e
        return results

    return results


def ingest(name, options):
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'SSWS {options["api_key"]}'
    }

    url = f"https://{options['subdomain']}.okta.com/api/v1/logs"

    timestamp_query = f"""
    SELECT EVENT_TIME from DATA.{name}_CONNECTION
    WHERE EVENT_TIME IS NOT NULL
    order by EVENT_TIME desc
    limit 1
    """

    metadata = {
        'START_TIME': datetime.datetime.utcnow(),
        'TYPE': 'Okta',
        'QUERY_NAME': f'{name}_Okta'
    }

    try:
        ts = next(db.fetch(timestamp_query), [None])[0]
        if ts:
            ts = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        else:
            ts = datetime.datetime.now() - datetime.timedelta(hours=1)
            ts = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        log.info(ts)

    except Exception:
        log.error("Unable to find a timestamp of most recent okta log, defaulting to one hour ago")
        ts = datetime.datetime.now() - datetime.timedelta(hours=1)
        ts = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    timestamp = {'since': ts}
    loaded = 0

    while 1:
        log.info(f"url is ${url}")
        try:
            r = requests.get(url=url, headers=headers, params=timestamp)
            if r.status_code != 200:
                log.fatal('OKTA REQUEST FAILED: ', r.text)
            loaded += process_logs(r.json(), name)
            if r.json == []:
                break
            url = r.headers['Link'].split(', ')[1].split(';')[0][1:-1]
        except Exception as e:
            log.error("Error with Okta logs: ", e)
            db.record_metadata(metadata, table=CONNECTOR_METADATA_TABLE, e=e)

    metadata['ROW_COUNT'] = {'INSERTED': loaded}
    db.record_metadata(metadata, table=CONNECTOR_METADATA_TABLE)


def process_logs(logs, name):
    output = [(row, row['published']) for row in logs]
    db.insert(f"DATA.{name}_CONNECTION", output, select='PARSE_JSON(column1), column2')
    return len(output)
