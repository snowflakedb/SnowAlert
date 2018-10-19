#!/bin/env python3

import json
import hashlib
import uuid
import datetime

from config import ALERTS_TABLE, RULES_SCHEMA, RESULTS_SCHEMA, ALERT_QUERY_POSTFIX
from helpers import log
from helpers.db import connect_and_execute, load_rules


GROUPING_PERIOD = 1 * 60 * 60  # Group events within one hour periods


# Grab alerts from past grouping_period amount of time
def get_recent_alerts(ctx, alert_type):
    alert_map = {}
    recent_alerts = ctx.cursor().execute(
        f"SELECT alert, counter FROM {ALERTS_TABLE} "
        f"WHERE try_cast(alert:EVENT_TIME::string as timestamp_ntz) >= DATEADD('second', (-1 * {GROUPING_PERIOD}), CURRENT_TIMESTAMP()) "
        f"  AND alert:AlertType = '{alert_type}';"
    ).fetchall()

    for alert in recent_alerts:
        current_alert = json.loads(alert[0])
        key = hashlib.md5((current_alert['OBJECT'] + current_alert['DESCRIPTION']).encode('utf-8')).hexdigest()
        alert_map[key] = [current_alert, alert[1], False]

    return alert_map


# Check if the proposed alert was already created recently, and update its counter
def alert_exists(alert_map, new_alert):
    uniq = new_alert['OBJECT'] + new_alert['DESCRIPTION']
    key = hashlib.md5(uniq.encode('utf-8')).hexdigest()
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
        ctx.cursor().execute(f"CREATE TEMPORARY TABLE {RESULTS_SCHEMA}.counter_table(ALERT_ID string, COUNTER number);")
        ctx.cursor().execute(
            f"INSERT INTO {RESULTS_SCHEMA}.counter_table (ALERT_ID, COUNTER) VALUES {format_string};",
            update_array
        )
        ctx.cursor().execute(
            f"MERGE INTO {RESULTS_SCHEMA}.alerts s"
            f" USING {RESULTS_SCHEMA}.counter_table t"
            f" ON s.alert:ALERT_ID = t.ALERT_ID WHEN MATCHED THEN UPDATE"
            f" SET s.COUNTER = t.COUNTER;"
        )


def log_alerts(ctx, alerts):
    if len(alerts):
        print("Logging alerts...")
        format_string = ", ".join(["(%s)"] * len(alerts))
        try:
            ctx.cursor().execute((
                f'INSERT INTO {ALERTS_TABLE}(alert_time, alert) '
                f'SELECT PARSE_JSON(column1):ALERT_TIME, PARSE_JSON(column1) '
                f'FROM values {format_string};'),
                alerts)
        except Exception as e:
            log.fatal("Failed to log alert", e)
    else:
        print("No alerts to log.")


def log_failure(ctx, query_name, e):
    alerts = [json.dumps({
        'ALERT_ID': uuid.uuid4().hex,
        'QUERY_ID': '3a3d173a64ca4fcab2d13ac3e6d08522',
        'QUERY_NAME': 'Query Runner Failure',
        'ENVIRONMENT': 'Queries',
        'SOURCES': 'Query Runner',
        'ACTOR': 'Query Runner',
        'OBJECT': query_name,
        'ACTION': 'Query Execution',
        'TITLE': 'Query Runner Failure',
        'ALERT_TIME': str(datetime.datetime.utcnow()),
        'EVENT_TIME': str(datetime.datetime.utcnow()),
        'EVENT_DATA': f"The query '{query_name}' failed to execute with error: {e!r}",
        'DESCRIPTION': f"The query '{query_name}' failed to execute with error: {e!r}",
        'DETECTOR': 'Query Runner',
        'SEVERITY': 'High'
    })]
    try:
        log_alerts(ctx, alerts)
        log.fatal("Query failure successfully logged", e)
    except Exception as e:
        log.fatal("Failed to log query failure", e)


def snowalert_query(query_name: str):
    print(f"Received query {query_name}")

    ctx = connect_and_execute()

    try:
        query = f'''
            SELECT OBJECT_CONSTRUCT(*) from {RULES_SCHEMA}.{query_name}
            WHERE EVENT_TIME > dateadd(minute, -90, current_timestamp())
        '''
        results = ctx.cursor().execute(query).fetchall()
    except Exception as e:
        log_failure(ctx, query_name, e)

    print(f"Query {query_name} executed")
    return results, ctx


def process_results(results, ctx, query_name):
    alerts = []
    recent_alerts = get_recent_alerts(ctx, query_name)
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


def query_for_alerts(query_name: str):
    results, ctx = snowalert_query(query_name)
    process_results(results, ctx, query_name)


def main():
    # Force warehouse resume so query runner doesn't have a bunch of queries waiting for warehouse resume
    ctx = connect_and_execute("ALTER SESSION SET USE_CACHED_RESULT=FALSE;")
    for query_name in load_rules(ctx, ALERT_QUERY_POSTFIX):
        query_for_alerts(query_name)


if __name__ == '__main__':
    main()
