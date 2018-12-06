#!/usr/bin/env python

import datetime
import json
import os

from config import VIOLATIONS_TABLE, METADATA_TABLE, RULES_SCHEMA, VIOLATION_QUERY_POSTFIX, CLOUDWATCH_METRICS
from helpers.db import connect_and_fetchall, connect_and_execute, load_rules
from helpers import log

RUN_METADATA = {'QUERY_HISTORY': [], 'RUN_TYPE': 'VIOLATION QUERIES'}  # Contains metadata about this run


def log_alerts(ctx, alerts):
    output_column = os.environ.get('output_column', 'result')
    time_column = os.environ.get('time_column', 'alert_time')

    if len(alerts):
        ctx.cursor().execute(
            f"""
            INSERT INTO {VIOLATIONS_TABLE} ({time_column}, {output_column})
            SELECT PARSE_JSON(column1):ALERT_TIME,
                   PARSE_JSON(column1)
            FROM VALUES {", ".join(["(%s)"] * len(alerts))};
            """,
            alerts
        )


def snowalert_query(query_name):
    time_filter_unit = os.environ.get('time_filter_unit', 'day')
    time_filter_amount = -1 * int(os.environ.get('time_filter_amount', 1))

    log.info(f"{query_name} processing...")

    ctx, results = connect_and_fetchall(f"""
        SELECT OBJECT_CONSTRUCT(*) FROM {RULES_SCHEMA}.{query_name}
        WHERE alert_time > DATEADD({time_filter_unit}, {time_filter_amount}, CURRENT_TIMESTAMP())
    """)

    log.info(f"{query_name} done.")
    return results, ctx


def process_results(results, ctx, query_name, metadata):
    alerts = []
    for res in results:
        jres = json.loads(res[0])
        alerts.append(json.dumps(jres))
    log.metadata_fill(metadata, status='success', rows=ctx.cursor().rowcount)
    log_alerts(ctx, alerts)


def run_query(query_name):
    metadata = {}
    metadata['NAME'] = query_name
    metadata['START_TIME'] = datetime.datetime.utcnow()
    results, ctx = snowalert_query(query_name)
    process_results(results, ctx, query_name, metadata)
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
        log.info("Recording run metadata.")
        ctx.cursor().execute(statement)
    except Exception as e:
        log.fatal("Metadata failed to log", e)
        # log_failure(ctx, "Metadata Logging", e, event_data=metadata, description="The run metadata failed to log")


def main():
    # Force warehouse resume so query runner doesn't have a bunch of queries waiting for warehouse resume
    RUN_METADATA['RUN_START_TIME'] = datetime.datetime.utcnow()
    ctx = connect_and_execute("ALTER SESSION SET use_cached_result=FALSE;")
    for query_name in load_rules(ctx, VIOLATION_QUERY_POSTFIX):
        run_query(query_name)

    RUN_METADATA['RUN_END_TIME'] = datetime.datetime.utcnow()
    RUN_METADATA['RUN_DURATION'] = RUN_METADATA['RUN_END_TIME'] - RUN_METADATA['RUN_START_TIME']
    record_metadata(ctx, RUN_METADATA)
    if CLOUDWATCH_METRICS:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Violation Query Runner'}], 1)


if __name__ == '__main__':
    main()
