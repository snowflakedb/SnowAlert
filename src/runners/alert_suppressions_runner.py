#!/usr/bin/env python

import datetime
from typing import List

from runners.config import (
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    ALERT_SQUELCH_POSTFIX,
    CLOUDWATCH_METRICS,
    RUN_ID,
)
from runners.helpers import db, log

CALL_ASR = f'''
CALL results.alert_suppressions_runner()
'''


def main():
    start_time = datetime.datetime.utcnow()
    res = next(db.fetch(CALL_ASR)).get('ALERT_SUPPRESSIONS_RUNNER')
    for c in res['suppression_counts']:
        db.record_metadata(
            {
                'QUERY_NAME': c['RULE'],
                'RUN_ID': res['run_id'],
                'ATTEMPTS': 1,
                'START_TIME': start_time,
                'ROW_COUNT': {'SUPPRESSED': c['COUNT']},
            },
            table=QUERY_METADATA_TABLE,
        )

    total_updated = res['merge_result']['number of rows updated']
    total_suppressed = sum(c['COUNT'] for c in res['suppression_counts'])

    db.record_metadata(
        {
            'RUN_TYPE': 'ALERT SUPPRESSION',
            'START_TIME': start_time,
            'RUN_ID': RUN_ID,
            'ROW_COUNT': {
                'PASSED': total_updated - total_suppressed,
                'SUPPRESSED': total_suppressed,
            },
        },
        table=RUN_METADATA_TABLE,
    )

    try:
        if CLOUDWATCH_METRICS:
            log.metric(
                'Run',
                'SnowAlert',
                [{'Name': 'Component', 'Value': 'Alert Suppression Runner'}],
                1,
            )
    except Exception as e:
        log.error("Cloudwatch metric logging failed: ", e)


if __name__ == '__main__':
    main()
