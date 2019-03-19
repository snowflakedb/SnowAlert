#!/usr/bin/env python

import datetime

from runners.config import (
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    VIOLATION_QUERY_POSTFIX,
    CLOUDWATCH_METRICS,
    RUN_ID,
)
from runners.helpers import db, log


def main():
    RUN_METADATA = {
        'RUN_TYPE': 'VIOLATION QUERY',
        'START_TIME': datetime.datetime.utcnow(),
        'RUN_ID': RUN_ID,
    }

    # Force warehouse resume so query runner doesn't have a bunch of queries waiting for warehouse resume
    ctx = db.connect_and_execute("ALTER SESSION SET use_cached_result=FALSE;")
    for query_name in db.load_rules(ctx, VIOLATION_QUERY_POSTFIX):
        metadata = {
            'QUERY_NAME': query_name,
            'RUN_ID': RUN_ID,
            'ATTEMPTS': 1,
            'START_TIME': datetime.datetime.utcnow(),
        }
        insert_count, update_count = db.insert_violations_query_run(query_name)
        metadata['ROW_COUNT'] = {
            'INSERTED': insert_count,
            'UPDATED': update_count,
        }
        log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE)

    log.metadata_record(ctx, RUN_METADATA, table=RUN_METADATA_TABLE)

    if CLOUDWATCH_METRICS:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Violation Query Runner'}], 1)


if __name__ == '__main__':
    main()
