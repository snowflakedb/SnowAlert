'''Greathorn Event Log
Use API key to regularly catch up on Greathorn Events
'''

from datetime import datetime, timedelta
import json
import os
import requests
import time

import fire
from dateutil.parser import parse as parse_date

from connectors.utils import Bearer
from runners.helpers import db, log


RUN_LIMIT = timedelta(minutes=10)

CONNECTION_OPTIONS = [
    {
        'name': 'api_key',
        'title': "API Token",
        'prompt': "This available in your GreatHorn",
        'type': 'str',
        'secret': True,
        'required': True,
    },
    {
        'name': 'lookback',
        'title': "Look back period",
        'prompt': "How far to back to load on first run",
        'type': 'str',
        'default': '-1h',
        'required': False,
    },
]


LANDING_EVENTS_TABLE_COLUMNS = [
    ('recorded_at', 'TIMESTAMP_LTZ'),
    ('raw', 'VARIANT'),
    ('timestamp', 'TIMESTAMP_LTZ'),
    ('event_id', 'NUMBER'),
    ('source', 'STRING'),
    ('origin', 'STRING'),
    ('ip', 'STRING'),
    ('dmarc', 'STRING'),
    ('dkim', 'STRING'),
    ('spf', 'STRING'),
]


def connect(connection_name, options):
    table_name = f'greathorn_logs_{connection_name}'
    landing_events_table = f'data.{table_name}_connection'

    db.create_table(
        name=landing_events_table,
        cols=LANDING_EVENTS_TABLE_COLUMNS,
        comment=yaml_dump(module='greathorn_logs', **options),
        rw_role=ROLE,
    )

    return {
        'newStage': 'finalized',
        'newMessage': "Events table for Greathorn created!",
    }


def ingest(table_name, options, dryrun=False):
    landing_table = f'data.{table_name}'

    # https://greathorn.readme.io/reference (ask support for password)
    url = 'https://api.greathorn.com/v2/search/events'
    api_key = options['api_key']
    lookback = options['lookback']

    query = f'SELECT MAX(event_id::NUMBER) id FROM {landing_table}'
    last_id = next(db.fetch(query), {}).get('ID')
    filter = {'minEventId': last_id}

    start = datetime.now()
    offset = 0
    while True:
        log.info(f"<= loading from offset={offset:04} (last_id={last_id})")

        res = requests.post(
            url,
            auth=Bearer(api_key),
            json={
                'limit': 200,
                'offset': offset,
                'sortDir': 'asc',
                'filters': [filter],
            },
        )
        if res.status_code != 200:
            log.error('res.status_code != 200', res.text)
            return

        response = res.json()
        total = response['total']
        results = response['results']
        yield len(results)

        last = results[-1]
        last_ts = last['timestamp']
        last_id = last['eventId']

        log.info(
            f"=> {len(results)}/{response['total']} rows,"
            f" offset {offset:04}"
            f" goes to id={last_id} timestamp={last_ts}"
        )

        db.insert(
            landing_table,
            [
                {
                    'raw': a,
                    'recorded_at': parse_date(res.headers['Date']),
                    'timestamp': parse_date(a['timestamp']),
                    'event_id': a['eventId'],
                    'source': a['source'],
                    'origin': a['origin'],
                    'ip': a['ip'],
                    'spf': a['spf'],
                    'dkim': a['dkim'],
                    'dmarc': a['dmarc'],
                }
                for a in results
            ],
            dryrun=dryrun,
        )

        offset += len(results)

        ran_too_long = datetime.now() - start > RUN_LIMIT
        if offset > 10000 or len(results) < 200 or ran_too_long:
            log.info(f"That's all Folks!")
            break


def main(
    table_name='greathorn_logs_connection',
    api_key=os.environ.get('GH_TOKEN'),
    dryrun=True,
):
    return ingest(table_name, {'api_key': api_key, 'lookback': '-1h'}, dryrun=dryrun)


if __name__ == '__main__':
    fire.Fire(main)
