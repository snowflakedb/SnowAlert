#!/usr/bin/env python

import datetime
from multiprocessing import Pool

from .config import (
    POOLSIZE,
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    VIOLATION_QUERY_POSTFIX,
    CLOUDWATCH_METRICS,
    RUN_ID,
)
from .helpers import db, log


def create_violations(rule_name):
        metadata = {
            'QUERY_NAME': rule_name,
            'RUN_ID': RUN_ID,
            'ATTEMPTS': 1,
            'START_TIME': datetime.datetime.utcnow(),
        }
        try:
            insert_count = db.insert_violations_query_run(rule_name)
        except Exception as e:
            log.info(f"{rule_name} threw an exception.")
            insert_count = 0
            metadata['EXCEPTION'] = e

        metadata['ROW_COUNT'] = {'INSERTED': insert_count}
        db.record_metadata(metadata, table=QUERY_METADATA_TABLE)
        log.info(f"{rule_name} done.")

        return metadata


def main(rules_postfix=VIOLATION_QUERY_POSTFIX):
    RUN_METADATA = {
        'RUN_TYPE': 'VIOLATION QUERY',
        'START_TIME': datetime.datetime.utcnow(),
        'RUN_ID': RUN_ID,
        'ROW_COUNT': {'INSERTED': 0},
    }
    QUERY_METADATA_RECORDS = []

    # Force warehouse resume so query runner doesn't have a bunch of queries waiting for warehouse resume
    rules = list(db.load_rules(rules_postfix))
    QUERY_METADATA_RECORDS += list(Pool(POOLSIZE).map(create_violations, rules))

    RUN_METADATA['ROW_COUNT'] = {
        'INSERTED': sum(r['ROW_COUNT']['INSERTED'] for r in QUERY_METADATA_RECORDS)
    }
    db.record_metadata(RUN_METADATA, table=RUN_METADATA_TABLE)

    if CLOUDWATCH_METRICS:
        log.metric(
            'Run',
            'SnowAlert',
            [{'Name': 'Component', 'Value': 'Violation Query Runner'}],
            1,
        )


if __name__ == '__main__':
    main()
