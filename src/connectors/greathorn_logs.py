'''Greathorn Event Log
Use API key to regularly catch up on Greathorn Events
'''

import requests
import os
import json
from datetime import datetime
from datetime import timedelta
import time

import fire
from dateutil.parser import parse as parse_date

from connectors.utils import Bearer
from runners.helpers import db, log

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
    ('event_id', 'STRING'),
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

    # https://greathorn.zendesk.com/hc/en-us/articles/115000893911-Threat-Platform-API-Reference
    url = 'https://api.greathorn.com/v2/search/events'
    api_key = options['api_key']
    lookback = options['lookback']

    starttime = db.fetch_latest(landing_table, 'timestamp', default='-1h').replace(
        microsecond=0
    ) + timedelta(milliseconds=1)
    endtime = datetime.now(tz=starttime.tzinfo).replace(microsecond=0)
    offset = 0

    while True:
        res = requests.post(
            url,
            auth=Bearer(api_key),
            json={
                'limit': 200,
                'offset': offset,
                'filters': [
                    {
                        'startDate': starttime.isoformat(),
                        'endDate': endtime.isoformat(),
                    }
                ],
            },
        )
        if res.status_code != 200:
            log.error('res.status_code != 200', res.text)
            return

        response = res.json()
        total = response['total']
        results = response['results']
        yield len(results)

        if offset == 0:
            log.info(
                f"[{starttime.isoformat()}, {endtime.isoformat()}] has {total} events"
            )

        if total > 9000:  # 10k is max & messages have ~30s delay
            endtime -= (endtime - starttime) / 2
            log.info(f"new endtime {endtime}")
            continue

        if not results:
            log.info(f"That's all Folks!")
            break

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

        log.info(f"offset {offset} desc from {parse_date(results[-1]['timestamp'])}")


def main(
    table_name='greathorn_logs_connection', api_key=os.environ.get('GH_TOKEN'), dryrun=True
):
    return ingest(table_name, {'api_key': api_key, 'lookback': '-1h'}, dryrun=dryrun)


if __name__ == '__main__':
    fire.Fire(main)
