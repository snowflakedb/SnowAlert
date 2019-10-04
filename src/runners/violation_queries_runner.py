#!/usr/bin/env python

import datetime

from .config import (
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    VIOLATION_QUERY_POSTFIX,
    CLOUDWATCH_METRICS,
    RUN_ID,
)
from .helpers import db, log

METADATA_RECORDS = []


def main(rules_postfix=VIOLATION_QUERY_POSTFIX):
    RUN_METADATA = {
        'RUN_TYPE': 'VIOLATION QUERY',
        'START_TIME': datetime.datetime.utcnow(),
        'RUN_ID': RUN_ID,
    }

    # Force warehouse resume so query runner doesn't have a bunch of queries waiting for warehouse resume
    for query_name in db.load_rules(VIOLATION_QUERY_POSTFIX):
        metadata = {
            'QUERY_NAME': query_name,
            'RUN_ID': RUN_ID,
            'ATTEMPTS': 1,
            'START_TIME': datetime.datetime.utcnow(),
        }
        try:
            insert_count = db.insert_violations_query_run(query_name)
        except Exception as e:
            log.info(f"{query_name} threw an exception.")
            insert_count = 0
            metadata['EXCEPTION'] = e

        metadata['ROW_COUNT'] = {
            'INSERTED': insert_count,
        }
        db.record_metadata(metadata, table=QUERY_METADATA_TABLE)
        log.info(f"{query_name} done.")
        METADATA_RECORDS.append(metadata)

    RUN_METADATA['ROW_COUNT'] = {
        'INSERTED': sum(r['ROW_COUNT']['INSERTED'] for r in METADATA_RECORDS),
    }
    db.record_metadata(RUN_METADATA, table=RUN_METADATA_TABLE)

    if CLOUDWATCH_METRICS:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Violation Query Runner'}], 1)


if __name__ == '__main__':
    main()
