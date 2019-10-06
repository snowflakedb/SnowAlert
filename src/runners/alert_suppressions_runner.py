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

OLD_SUPPRESSION_QUERY = f"""
MERGE INTO results.alerts AS target
USING(rules.{{suppression_name}}) AS s
ON target.alert:ALERT_ID = s.alert:ALERT_ID
WHEN MATCHED THEN UPDATE
SET target.SUPPRESSED = 'true'
  , target.SUPPRESSION_RULE = '{{suppression_name}}'
"""

SUPPRESSION_QUERY = f"""
MERGE INTO results.alerts AS target
USING(rules.{{suppression_name}}) AS s
ON target.alert:ALERT_ID = s.id
WHEN MATCHED THEN UPDATE
SET target.SUPPRESSED = 'true'
  , target.SUPPRESSION_RULE = '{{suppression_name}}'
"""

SET_SUPPRESSED_FALSE = f"""
UPDATE results.alerts
SET suppressed=FALSE
WHERE suppressed IS NULL;
"""

METADATA_HISTORY: List = []


def run_suppression_query(squelch_name):
    try:
        query = SUPPRESSION_QUERY.format(suppression_name=squelch_name)
        return next(db.fetch(query, fix_errors=False))['number of rows updated']
    except Exception:
        log.info(
            f"{squelch_name} warning: query broken, might need 'id' column, trying 'alert:ALERT_ID'."
        )
        query = OLD_SUPPRESSION_QUERY.format(suppression_name=squelch_name)
        return next(db.fetch(query))['number of rows updated']


def run_suppressions(squelch_name):
    log.info(f"{squelch_name} processing...")
    metadata = {
        'QUERY_NAME': squelch_name,
        'RUN_ID': RUN_ID,
        'ATTEMPTS': 1,
        'START_TIME': datetime.datetime.utcnow(),
    }

    try:
        suppression_count = run_suppression_query(squelch_name)
        log.info(f"{squelch_name} updated {suppression_count} rows.")
        metadata['ROW_COUNT'] = {'SUPPRESSED': suppression_count}
        db.record_metadata(metadata, table=QUERY_METADATA_TABLE)

    except Exception as e:
        metadata['ROW_COUNT'] = {'SUPPRESSED': 0}
        db.record_metadata(metadata, table=QUERY_METADATA_TABLE, e=e)

    METADATA_HISTORY.append(metadata)
    log.info(f"{squelch_name} done.")


def main(squelch_name=None):
    RUN_METADATA = {
        'RUN_TYPE': 'ALERT SUPPRESSION',
        'START_TIME': datetime.datetime.utcnow(),
        'RUN_ID': RUN_ID,
    }

    rules = (
        db.load_rules(ALERT_SQUELCH_POSTFIX) if squelch_name is None else [squelch_name]
    )
    for squelch_name in rules:
        run_suppressions(squelch_name)

    num_rows_updated = next(db.fetch(SET_SUPPRESSED_FALSE))['number of rows updated']
    log.info(
        f'All suppressions done, {num_rows_updated} remaining alerts marked suppressed=FALSE.'
    )

    RUN_METADATA['ROW_COUNT'] = {
        'PASSED': num_rows_updated,
        'SUPPRESSED': sum(m['ROW_COUNT']['SUPPRESSED'] for m in METADATA_HISTORY),
    }

    db.record_metadata(RUN_METADATA, table=RUN_METADATA_TABLE)

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
