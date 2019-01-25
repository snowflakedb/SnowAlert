#!/usr/bin/env python

import datetime
import json
import os
import uuid

from runners.config import (
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    RULES_SCHEMA,
    VIOLATION_QUERY_POSTFIX,
    CLOUDWATCH_METRICS,
)
from runners.helpers import db, log

RUN_ID = uuid.uuid4().hex


def snowalert_query(query_name):
    time_filter_unit = os.environ.get('time_filter_unit', 'day')
    time_filter_amount = -1 * int(os.environ.get('time_filter_amount', 1))

    log.info(f"{query_name} processing...")

    ctx, results = db.connect_and_fetchall(f"""
        SELECT OBJECT_CONSTRUCT(*) FROM {RULES_SCHEMA}.{query_name}
        WHERE alert_time > DATEADD({time_filter_unit}, {time_filter_amount}, CURRENT_TIMESTAMP())
    """)

    log.info(f"{query_name} done.")
    return [json.dumps(json.loads(res[0])) for res in results], ctx


def run_query(query_name):
    metadata = {}
    metadata['QUERY_NAME'] = query_name
    metadata['RUN_ID'] = RUN_ID
    metadata['ATTEMPTS'] = 1
    metadata['START_TIME'] = datetime.datetime.utcnow()
    results, ctx = snowalert_query(query_name)
    log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE)
    db.insert_violations(ctx, results)


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
