#!/usr/bin/env python

import datetime

from runners.config import (
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    VIOLATION_QUERY_POSTFIX,
    CLOUDWATCH_METRICS,
)
from runners.helpers import db, log


def run_query(query_name):
    metadata = {}
    metadata['QUERY_NAME'] = query_name
    metadata['RUN_ID'] = RUN_ID
    metadata['ATTEMPTS'] = 1
    metadata['START_TIME'] = datetime.datetime.utcnow()
    ctx = db.insert_violations_query_run(query_name)
    log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE)


def main():
    # Force warehouse resume so query runner doesn't have a bunch of queries waiting for warehouse resume
    RUN_METADATA = {}
    RUN_METADATA['RUN_TYPE'] = 'VIOLATION QUERY'
    RUN_METADATA['START_TIME'] = datetime.datetime.utcnow()
    RUN_METADATA['RUN_ID'] = RUN_ID
    ctx = db.connect_and_execute("ALTER SESSION SET use_cached_result=FALSE;")
    for query_name in db.load_rules(ctx, VIOLATION_QUERY_POSTFIX):
        run_query(query_name)

    log.metadata_record(ctx, RUN_METADATA, table=RUN_METADATA_TABLE)

    if CLOUDWATCH_METRICS:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Violation Query Runner'}], 1)


if __name__ == '__main__':
    main()
