#!/usr/bin/env python

import datetime

from runners.config import (
    CLOUDWATCH_METRICS,
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    VIOLATION_SQUELCH_POSTFIX,
    RUN_ID,
)
from runners.helpers import db, log


VIOLATION_SUPPRESSION_QUERY = f"""
MERGE INTO results.violations AS target
USING rules.{{squelch_name}} AS squelch
ON squelch.id=target.id
WHEN MATCHED THEN UPDATE
  SET target.suppressed='true'
    , target.suppression_rule='{{squelch_name}}'
"""

SET_SUPPRESSED_FALSE = f"""
UPDATE results.violations
SET suppressed=FALSE
WHERE suppressed IS NULL
"""

RULE_METADATA_RECORDS = []


def run_suppression(squelch_name):
    metadata = {
        'QUERY_NAME': squelch_name,
        'RUN_ID': RUN_ID,
        'ATTEMPTS': 1,
        'START_TIME': datetime.datetime.utcnow(),
        'ROW_COUNT': {'SUPPRESSED': 0},
    }
    log.info(f"{squelch_name} processing...")
    try:
        query = VIOLATION_SUPPRESSION_QUERY.format(squelch_name=squelch_name)
        num_violations_suppressed = next(db.fetch(query))['number of rows updated']
        log.info(f"{squelch_name} updated {num_violations_suppressed} rows.")
        metadata['ROW_COUNT']['SUPPRESSED'] = num_violations_suppressed
        db.record_metadata(metadata, table=QUERY_METADATA_TABLE)
        RULE_METADATA_RECORDS.append(metadata)

    except Exception as e:
        db.record_metadata(metadata, table=QUERY_METADATA_TABLE, e=e)
        log.error("Suppression query {squelch_name} execution failed.", e)

    print(f"Suppression query {squelch_name} executed")


def main():
    RUN_METADATA = {
        'RUN_TYPE': 'VIOLATION SUPPRESSION',
        'START_TIME': datetime.datetime.utcnow(),
        'RUN_ID': RUN_ID,
    }

    for squelch_name in db.load_rules(VIOLATION_SQUELCH_POSTFIX):
        run_suppression(squelch_name)

    num_violations_passed = next(db.fetch(SET_SUPPRESSED_FALSE))[
        'number of rows updated'
    ]
    RUN_METADATA['ROW_COUNT'] = {
        'SUPPRESSED': sum(
            rmr['ROW_COUNT']['SUPPRESSED'] for rmr in RULE_METADATA_RECORDS
        ),
        'PASSED': num_violations_passed,
    }
    db.record_metadata(RUN_METADATA, table=RUN_METADATA_TABLE)

    if CLOUDWATCH_METRICS:
        log.metric(
            'Run',
            'SnowAlert',
            [{'Name': 'Component', 'Value': 'Violation Suppression Runner'}],
            1,
        )


if __name__ == '__main__':
    main()
