#!/usr/bin/env python

import datetime
import fire
import os
from multiprocessing import Pool
from typing import Any, Dict

from runners.config import (
    POOLSIZE,
    RUN_ID,
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    ALERT_QUERY_POSTFIX,
    CLOUDWATCH_METRICS,
)
from runners.helpers import db, log


# Three envars, two ways to set window properties:
# 1. by start & end: (SA_ALERT_FROM_TIME, SA_ALERT_TO_TIME)
# 2. by end & duration: (SA_ALERT_TO_TIME, SA_ALERT_CUTOFF_MINUTES)

# SA_ALERT_TO_TIME default is current time
# SA_ALERT_CUTOFF_MINUTES default is 90

ALERT_CUTOFF_MINUTES = int(os.environ.get('SA_ALERT_CUTOFF_MINUTES', -90))
if ALERT_CUTOFF_MINUTES > 0:
    ALERT_CUTOFF_MINUTES = -ALERT_CUTOFF_MINUTES

ALERTS_TO_TIME = os.environ.get('SA_ALERT_TO_TIME', 'CURRENT_TIMESTAMP')
ALERTS_FROM_TIME = os.environ.get(
    'SA_ALERT_FROM_TIME', f'DATEADD(minute, {ALERT_CUTOFF_MINUTES}, {ALERTS_TO_TIME})'
)


CALL_AQR = f'''CALL results.alert_queries_runner(
  '{{rule_name}}',
  '{ALERTS_FROM_TIME}',
  '{ALERTS_TO_TIME}'
)
;'''


def call_aqr(rule_name):
    return next(db.fetch(CALL_AQR.format(rule_name=rule_name))).get(
        'ALERT_QUERIES_RUNNER'
    )


def create_alerts(rule_name: str) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {
        'QUERY_NAME': rule_name,
        'RUN_ID': RUN_ID,
        'ATTEMPTS': 1,
        'START_TIME': datetime.datetime.utcnow(),
        'ROW_COUNT': {'INSERTED': 0, 'UPDATED': 0},
    }

    try:
        res = call_aqr(rule_name)
        metadata['ROW_COUNT'] = {
            'INSERTED': res['merge_alerts_result']['number of rows inserted'],
            'UPDATED': res['merge_alerts_result']['number of rows updated'],
        }

    except Exception as e:
        db.record_metadata(metadata, table=QUERY_METADATA_TABLE, e=e)
        return metadata

    db.record_metadata(metadata, table=QUERY_METADATA_TABLE)

    log.info(f"{rule_name} done.")

    return metadata


def main(rule_name=None):
    RUN_METADATA = {
        'RUN_ID': RUN_ID,
        'RUN_TYPE': 'ALERT QUERY',
        'START_TIME': datetime.datetime.utcnow(),
    }
    if rule_name:
        metadata = [create_alerts(rule_name)]
    else:
        rules = list(db.load_rules(ALERT_QUERY_POSTFIX))
        metadata = Pool(POOLSIZE).map(create_alerts, rules)

    RUN_METADATA['ROW_COUNT'] = {
        'INSERTED': sum(q['ROW_COUNT']['INSERTED'] for q in metadata),
        'UPDATED': sum(q['ROW_COUNT']['UPDATED'] for q in metadata),
    }
    db.record_metadata(RUN_METADATA, table=RUN_METADATA_TABLE)

    try:
        if CLOUDWATCH_METRICS:
            log.metric(
                'Run',
                'SnowAlert',
                [{'Name': 'Component', 'Value': 'Alert Query Runner'}],
                1,
            )
    except Exception as e:
        log.error("Cloudwatch metric logging failed: ", e)


if __name__ == '__main__':
    fire.Fire(main)
