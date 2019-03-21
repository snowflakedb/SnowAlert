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


def flag_remaining_alerts(ctx):
    try:
        ctx.cursor().execute(f"UPDATE results.violations SET suppressed=FALSE WHERE suppressed IS NULL;")
    except Exception as e:
        log.error("Failed to flag remaining alerts as unsuppressed", e)


def run_suppression(squelch_name):
    metadata = {
        'QUERY_NAME': squelch_name,
        'RUN_ID': RUN_ID,
        'ATTEMPTS': 1,
        'START_TIME': datetime.datetime.utcnow(),
        'ROW_COUNT': {
            'SUPPRESSED': 0,  # because of bug below. fix when bug fixed.
        }
    }
    ctx = db.connect()
    print(f"Received suppression {squelch_name}")
    try:
        ctx.cursor().execute(f"""
            MERGE INTO results.violations t
            USING(SELECT result:EVENT_HASH AS event_hash FROM rules.{squelch_name}) s
            ON t.result:EVENT_HASH=s.event_hash
            WHEN MATCHED THEN UPDATE
            SET t.suppressed='true', t.suppression_rule='{squelch_name}';
        """)
        log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE)
    except Exception as e:
        log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE, e=e)
        log.error("Suppression query {squelch_name} execution failed.", e)
        pass

    print(f"Suppression query {squelch_name} executed")


def main():
    RUN_METADATA = {
        'RUN_TYPE': 'VIOLATION SUPPRESSION',
        'START_TIME': datetime.datetime.utcnow(),
        'RUN_ID': RUN_ID,
        'ROW_COUNT': {
            'SUPPRESSED': 0,  # because of bug above. fix when bug fixed.
        }
    }

    ctx = db.connect()
    for squelch_name in db.load_rules(ctx, VIOLATION_SQUELCH_POSTFIX):
        run_suppression(squelch_name)
    flag_remaining_alerts(ctx)

    log.metadata_record(ctx, RUN_METADATA, table=RUN_METADATA_TABLE)

    if CLOUDWATCH_METRICS:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Violation Suppression Runner'}], 1)


if __name__ == '__main__':
    main()
