#!/usr/bin/env python

import datetime
from config import VIOLATIONS_TABLE, METADATA_TABLE, RULES_SCHEMA, VIOLATION_SQUELCH_POSTFIX, CLOUDWATCH_METRICS
from helpers import log
from helpers.db import connect, load_rules
import json

RUN_METADATA = {'QUERY_HISTORY': [], 'RUN_TYPE': 'VIOLATION SUPPRESSIONS'}  # Contains metadata about this run


def flag_remaining_alerts(ctx):
    try:
        ctx.cursor().execute(f"UPDATE {VIOLATIONS_TABLE} SET suppressed=FALSE WHERE suppressed IS NULL;")
    except Exception as e:
        log.fatal("Failed to flag remaining alerts as unsuppressed", e)


def run_suppression(squelch_name):
    metadata = {}
    metadata['NAME'] = squelch_name
    metadata['START_TIME'] = datetime.datetime.utcnow()
    ctx = connect()
    print(f"Received suppression {squelch_name}")
    try:
        ctx.cursor().execute(f"""
            MERGE INTO {VIOLATIONS_TABLE} t
            USING(SELECT result:EVENT_HASH AS event_hash FROM {RULES_SCHEMA}.{squelch_name}) s
            ON t.result:EVENT_HASH=s.event_hash
            WHEN MATCHED THEN UPDATE
            SET t.suppressed='true', t.suppression_rule='{squelch_name}';
        """)
    except Exception as e:
        log.metadata_fill(metadata, status='failure', rows=0)
        RUN_METADATA['QUERY_HISTORY'].append(metadata)
        log.fatal("Suppression query {squelch_name} execution failed.", e)
        pass

    print(f"Suppression query {squelch_name} executed")
    log.metadata_fill(metadata, status='success', rows=ctx.cursor().rowcount)
    RUN_METADATA['QUERY_HISTORY'].append(metadata)


def record_metadata(ctx, metadata):
    metadata['RUN_START_TIME'] = str(metadata['RUN_START_TIME'])   # We wantd them to be objects for mathing
    metadata['RUN_END_TIME'] = str(metadata['RUN_END_TIME'])       # then convert to string for json serializing
    metadata['RUN_DURATION'] = str(metadata['RUN_DURATION'])

    statement = f'''
        INSERT INTO {METADATA_TABLE}
            (event_time, v) select '{metadata['RUN_START_TIME']}',
            PARSE_JSON(column1) from values('{json.dumps(metadata)}')
        '''
    try:
        log.info("Recording run metadata...")
        ctx.cursor().execute(statement)
    except Exception as e:
        log.fatal("Metadata failed to log", e)
        # log_failure(ctx, "Metadata Logging", e, event_data=metadata, description="The run metadata failed to log")


def main():
    RUN_METADATA['RUN_START_TIME'] = datetime.datetime.utcnow()
    ctx = connect()
    for squelch_name in load_rules(ctx, VIOLATION_SQUELCH_POSTFIX):
        run_suppression(squelch_name)
    flag_remaining_alerts(ctx)

    RUN_METADATA['RUN_END_TIME'] = datetime.datetime.utcnow()
    RUN_METADATA['RUN_DURATION'] = RUN_METADATA['RUN_END_TIME'] - RUN_METADATA['RUN_START_TIME']
    record_metadata(ctx, RUN_METADATA)
    if CLOUDWATCH_METRICS:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Violation Suppression Runner'}], 1)


if __name__ == '__main__':
    main()
