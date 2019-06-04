from runners.helpers import db, log
from runners.config import CONNECTOR_METADATA_TABLE

import datetime
import requests

CONNECTION_OPTIONS = {
    'subdomain': {'type': 'str'},
    'api_key': {'type': 'str', 'secret': True},
}

OKTA_LANDING_TABLE = """
(raw VARIANT,
 event_time TIMESTAMP_LTZ
)
"""


def connect(name, options):
    comment = f"""
---
name: {name}
source: okta
api_key_secret: {options['api_key']}
subdomain: {options['subdomain']}
"""

    results = {}
    try:
        db.create_table(name=name+"_CONNECTION", cols=OKTA_LANDING_TABLE, comment=comment)
        results['events_table'] = 'success'
    except Exception as e:
        results['events_table'] = 'failure'
        results['exception'] = e
        return results

    return results


def ingest(name, options):
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': f'SSWS {options["api_key"]}'}
    url = f"https://{options['subdomain']}.okta.com/api/v1/logs"

    timestamp_query = f"""
    SELECT EVENT_TIME from DATA.{name}_CONNECTION
    WHERE EVENT_TIME IS NOT NULL
    order by EVENT_TIME desc
    limit 1
    """

    metadata = {'START_TIME': datetime.datetime.utcnow(), 'TYPE': 'Okta', 'QUERY_NAME': f'{name}_Okta'}

    try:
        _, ts = db.connect_and_fetchall(timestamp_query)
        log.info(ts)
        ts = ts[0][0]
        ts = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        log.info(ts)
        if len(ts) < 1:
            log.error("The okta timestamp is too short or doesn't exist; defaulting to one hour ago")
            ts = datetime.datetime.now() - datetime.timedelta(hours=1)
            ts = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    except Exception as e:
        log.error("Unable to find a timestamp of most recent okta log, defaulting to one hour ago", e)
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
            if len(r.text) == 2:
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
