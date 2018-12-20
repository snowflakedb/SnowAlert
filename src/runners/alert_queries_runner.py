#!/usr/bin/env python

import json
import hashlib
import uuid
import datetime
from typing import Dict, Tuple

from config import ALERTS_TABLE, QUERY_METADATA_TABLE, RUN_METADATA_TABLE, RULES_SCHEMA, RESULTS_SCHEMA, ALERT_QUERY_POSTFIX, CLOUDWATCH_METRICS
from helpers import log
from helpers.db import connect_and_execute, execute, load_rules

GROUPING_CUTOFF = f"DATEADD(minute, -90, CURRENT_TIMESTAMP())"
RUN_ID = uuid.uuid4().hex


def alert_group(alert) -> str:
    return hashlib.md5(
        f"{alert['OBJECT']}{alert['DESCRIPTION']}".encode('utf-8')
    ).hexdigest()


# Deduplicate events if similar exists within GROUPING_CUTOFF
def get_existing_alerts(ctx, alert_type) -> Dict[str, Tuple[object, int, bool]]:
    alert_map = {}
    recent_alerts = ctx.cursor().execute(
        f"SELECT alert, counter FROM {ALERTS_TABLE} "
        f"WHERE TRY_CAST(alert:EVENT_TIME::STRING AS TIMESTAMP_NTZ) >= {GROUPING_CUTOFF}"
        f"  AND alert:QUERY_NAME = '{alert_type}';"
    ).fetchall()

    for alert in recent_alerts:
        current_alert = json.loads(alert[0])
        alert_map[alert_group(current_alert)] = [current_alert, alert[1], False]

    return alert_map


# Check if the proposed alert was already created recently, and update its counter
def alert_exists(alert_map, new_alert):
    key = alert_group(new_alert)
    if key in alert_map:
        alert_map[key][1] = alert_map[key][1] + 1
        alert_map[key][2] = True
        return True
    else:
        alert_map[key] = [new_alert, 1, False]
        return False


# After checking all recent alerts, update counter for alerts that were duplicated
def update_recent_alerts(ctx, alert_map):
    update_array = []
    update_array_length = 0
    for key in alert_map:
        if alert_map[key][2]:
            update_array.extend([alert_map[key][0]['ALERT_ID'], alert_map[key][1]])
            update_array_length = update_array_length + 1
    if update_array_length:
        format_string = ", ".join(["(%s, %s)"] * update_array_length)
        execute(ctx, f"DROP TABLE IF EXISTS {RESULTS_SCHEMA}.counter_table;")
        execute(ctx, f"CREATE TEMPORARY TABLE {RESULTS_SCHEMA}.counter_table(ALERT_ID string, COUNTER number);")
        ctx.cursor().execute(
            f"INSERT INTO {RESULTS_SCHEMA}.counter_table (ALERT_ID, COUNTER) VALUES {format_string};",
            update_array
        )
        ctx.cursor().execute(
            f"MERGE INTO {ALERTS_TABLE} s"
            f" USING {RESULTS_SCHEMA}.counter_table t"
            f" ON s.alert:ALERT_ID = t.ALERT_ID WHEN MATCHED THEN UPDATE"
            f" SET s.COUNTER = t.COUNTER;"
        )


def log_alerts(ctx, alerts):
    if len(alerts):
        print("Recording alerts.")
        format_string = ", ".join(["(%s)"] * len(alerts))
        try:
            ctx.cursor().execute((
                f'INSERT INTO {ALERTS_TABLE}(alert_time, event_time, alert) '
                f'SELECT PARSE_JSON(column1):ALERT_TIME, PARSE_JSON(column1):EVENT_TIME, PARSE_JSON(column1) '
                f'FROM values {format_string};'),
                alerts)
        except Exception as e:
            log.fatal("Failed to log alert", e)
            pass
    else:
        print("No alerts to log.")


def log_failure(ctx, query_name, e, event_data=None, description=None):
    if event_data is None:
        event_data = f"The query '{query_name}' failed to execute with error: {e!r}"

    if description is None:
        description = f"The query '{query_name}' failed to execute with error: {e!r}"

    alerts = [json.dumps({
        'ALERT_ID': uuid.uuid4().hex,
        'QUERY_ID': '3a3d173a64ca4fcab2d13ac3e6d08522',
        'QUERY_NAME': 'Query Runner Failure',
        'ENVIRONMENT': 'Queries',
        'SOURCES': ['Query Runner'],
        'ACTOR': 'Query Runner',
        'OBJECT': query_name,
        'ACTION': 'Query Execution',
        'TITLE': 'Query Runner Failure',
        'ALERT_TIME': str(datetime.datetime.utcnow()),
        'EVENT_TIME': str(datetime.datetime.utcnow()),
        'EVENT_DATA': event_data,
        'DESCRIPTION': description,
        'DETECTOR': 'Query Runner',
        'SEVERITY': 'High'
    })]
    try:
        log_alerts(ctx, alerts)
        log.info("Query failure logged.", e)

    except Exception as e:
        log.fatal("Failed to log query failure", e)


def snowalert_query(ctx, query_name: str):
    log.info(f"{query_name} processing...")
    metadata = {}
    metadata['QUERY_NAME'] = query_name
    metadata['RUN_ID'] = RUN_ID
    metadata['ATTEMPTS'] = 1

    metadata['START_TIME'] = datetime.datetime.utcnow()
    attempt = 0
    while attempt <= 1:
        try:
            attempt += 1
            query = f'''
                SELECT OBJECT_CONSTRUCT(*) FROM {RULES_SCHEMA}.{query_name}
                WHERE event_time > {GROUPING_CUTOFF}
            '''
            results = ctx.cursor().execute(query).fetchall()

        except Exception as e:
            if attempt > 1:
                log_failure(ctx, query_name, e)
                log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE, e=e)
                return results

            else:
                log.info(f"Query {query_name} failed to run, retrying...")
                metadata['ATTEMPTS'] += 1

    log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE)
    log.info(f"{query_name} done.")
    return results


def process_results(ctx, results, query_name):
    alerts = []
    recent_alerts = get_existing_alerts(ctx, query_name)
    for res in results:
        jres = json.loads(res[0])
        if 'OBJECT' not in jres or 'DESCRIPTION' not in jres:
            log.error(f'OBJECT and DESCRIPTION required in {jres}')
            continue

        jres['ALERT_ID'] = uuid.uuid4().hex
        if not alert_exists(recent_alerts, jres):
            alerts.append(json.dumps(jres))
    log_alerts(ctx, alerts)
    update_recent_alerts(ctx, recent_alerts)


def query_for_alerts(ctx, query_name: str):
    results = snowalert_query(ctx, query_name)
    process_results(ctx, results, query_name)


def main(rule_name=None):
    # Force warehouse resume so query runner doesn't have a bunch of queries waiting for warehouse resume
    RUN_METADATA = {}
    RUN_METADATA['RUN_TYPE'] = 'ALERT QUERY'
    RUN_METADATA['START_TIME'] = datetime.datetime.utcnow()
    RUN_METADATA['RUN_ID'] = RUN_ID
    ctx = connect_and_execute("ALTER SESSION SET USE_CACHED_RESULT=FALSE;")
    if rule_name:
        query_for_alerts(ctx, rule_name)
    else:
        for query_name in load_rules(ctx, ALERT_QUERY_POSTFIX):
            query_for_alerts(ctx, query_name)

    log.metadata_record(ctx, RUN_METADATA, table=RUN_METADATA_TABLE)

    if CLOUDWATCH_METRICS:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Alert Query Runner'}], 1)


if __name__ == '__main__':
    main()
