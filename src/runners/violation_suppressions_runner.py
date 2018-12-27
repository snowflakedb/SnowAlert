#!/usr/bin/env python

import datetime
import uuid

from .config import (
    VIOLATIONS_TABLE,
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    RULES_SCHEMA,
    VIOLATION_SQUELCH_POSTFIX,
    CLOUDWATCH_METRICS,
)
from .helpers import db, log

RUN_METADATA = {'QUERY_HISTORY': [], 'RUN_TYPE': 'VIOLATION SUPPRESSIONS'}  # Contains metadata about this run
RUN_ID = uuid.uuid4().hex


def flag_remaining_alerts(ctx):
    try:
        ctx.cursor().execute(f"UPDATE {VIOLATIONS_TABLE} SET suppressed=FALSE WHERE suppressed IS NULL;")
    except Exception as e:
        log.fatal("Failed to flag remaining alerts as unsuppressed", e)


def run_suppression(squelch_name):
    metadata = {}
    metadata['QUERY_NAME'] = squelch_name
    metadata['RUN_ID'] = RUN_ID
    metadata['ATTEMPTS'] = 1
    metadata['START_TIME'] = datetime.datetime.utcnow()
    ctx = db.connect()
    print(f"Received suppression {squelch_name}")
    try:
        ctx.cursor().execute(f"""
            MERGE INTO {VIOLATIONS_TABLE} t
            USING(SELECT result:EVENT_HASH AS event_hash FROM {RULES_SCHEMA}.{squelch_name}) s
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
    RUN_METADATA = {}
    RUN_METADATA['RUN_TYPE'] = 'VIOLATION SUPPRESSION'
    RUN_METADATA['START_TIME'] = datetime.datetime.utcnow()
    RUN_METADATA['RUN_ID'] = RUN_ID

    ctx = db.connect()
    for squelch_name in db.load_rules(ctx, VIOLATION_SQUELCH_POSTFIX):
        run_suppression(squelch_name)
    flag_remaining_alerts(ctx)

    log.metadata_record(ctx, RUN_METADATA, table=RUN_METADATA_TABLE)

    if CLOUDWATCH_METRICS:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Violation Suppression Runner'}], 1)


if __name__ == '__main__':
    main()
