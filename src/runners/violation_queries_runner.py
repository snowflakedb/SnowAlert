#!/usr/bin/env python

import datetime
import json
import os
import uuid

from config import VIOLATIONS_TABLE, QUERY_METADATA_TABLE, RUN_METADATA_TABLE, RULES_SCHEMA, VIOLATION_QUERY_POSTFIX, CLOUDWATCH_METRICS
from helpers.db import connect_and_fetchall, connect_and_execute, load_rules
from helpers import log

RUN_ID = uuid.uuid4().hex


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
    log_alerts(ctx, alerts)


def run_query(query_name):
    metadata = {}
    metadata['QUERY_NAME'] = query_name
    metadata['RUN_ID'] = RUN_ID
    metadata['ATTEMPTS'] = 1
    metadata['START_TIME'] = datetime.datetime.utcnow()
    results, ctx = snowalert_query(query_name)
    log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE)
    process_results(results, ctx, query_name, metadata)


def main():
    # Force warehouse resume so query runner doesn't have a bunch of queries waiting for warehouse resume
    RUN_METADATA = {}
    RUN_METADATA['RUN_TYPE'] = 'VIOLATION QUERY'
    RUN_METADATA['START_TIME'] = datetime.datetime.utcnow()
    RUN_METADATA['RUN_ID'] = RUN_ID
    ctx = connect_and_execute("ALTER SESSION SET use_cached_result=FALSE;")
    for query_name in load_rules(ctx, VIOLATION_QUERY_POSTFIX):
        run_query(query_name)

    log.metadata_record(ctx, RUN_METADATA, table=RUN_METADATA_TABLE)

    if CLOUDWATCH_METRICS:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Violation Query Runner'}], 1)


if __name__ == '__main__':
    main()
