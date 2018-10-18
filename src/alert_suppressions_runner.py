#!/bin/env python3

import json
import uuid
import datetime
from typing import List

from config import ALERTS_TABLE, RULES_SCHEMA, ALERT_SQUELCH_POSTFIX
from helpers import log
from helpers.db import connect, load_rules


def log_alerts(ctx, alerts):
    """We don't usually log alerts in the suppression runner, but we want the runner to create an alert if a
    suppression fails to execute.
    """
    if len(alerts):
        print("Logging alerts...")
        format_string = ", ".join(["(%s)"] * len(alerts))
        try:
            ctx.cursor().execute(
                f'''
                INSERT INTO {ALERTS_TABLE} (alert_time, alert)
                SELECT PARSE_JSON(column1):ALERT_TIME,
                       PARSE_JSON(column1)
                FROM VALUES {format_string};
                ''',
                alerts
            )
        except Exception as e:
            log.fatal("Failed to log alert", e)
    else:
        print("No alerts to log.")


def log_failure(ctx, suppression_name, e):
    alert = {}
    alert['ALERT_ID'] = uuid.uuid4().hex
    alert['QUERY_ID'] = 'b1d02051dd2c4d62bb75274f2ee5996a'
    alert['QUERY_NAME'] = 'Suppression Runner Failure'
    alert['ENVIRONMENT'] = 'Suppressions'
    alert['SOURCES'] = 'Suppression Runner'
    alert['ACTOR'] = 'Suppression Runner'
    alert['OBJECT'] = suppression_name
    alert['ACTION'] = 'Suppression Execution'
    alert['TITLE'] = 'Suppression Runner Failure'
    alert['EVENT_TIME'] = str(datetime.datetime.utcnow())
    alert['ALERT_TIME'] = str(datetime.datetime.utcnow())
    alert['DESCRIPTION'] = f"The suppression '{suppression_name}' failed to execute with error: {e}"
    alert['DETECTOR'] = 'Suppression Runner'
    alert['EVENT_DATA'] = f"The suppression '{suppression_name}' failed to execute with error: {e}"
    alert['SEVERITY'] = 'High'
    alerts = []
    alerts.append(json.dumps(alert))
    try:
        log_alerts(ctx, alerts)
        log.fatal(f"Suppression {suppression_name} failure successfully logged", e)
    except Exception as e:
        print(f"Failed to log suppression failure")
        log.fatal("Failed to log suppression failure", e)


def do_suppression(suppression_name, ctx):
    # set alert instances matching the suppression to suppressed
    query = f"""
        MERGE INTO {ALERTS_TABLE} t
        USING({RULES_SCHEMA}.{suppression_name}) s
        ON t.alert:ALERT_ID = s.alert:ALERT_ID
        WHEN MATCHED THEN UPDATE
        SET t.SUPPRESSED = 'true', t.SUPPRESSION_RULE = '{suppression_name}';
    """
    ctx.cursor().execute(query)


def run_suppressions(squelch_name):
    print(f"Received suppression {squelch_name}")

    ctx = connect()

    try:
        do_suppression(squelch_name, ctx)
    except Exception as e:
        log_failure(ctx, squelch_name, e)

    print(f"Suppression query {squelch_name} executed")


def flag_remaining_alerts(ctx) -> List[str]:
    try:
        query = f"UPDATE {ALERTS_TABLE} SET suppressed=FALSE WHERE suppressed IS NULL;"
        suppression_view_list = ctx.cursor().execute(query)
    except Exception as e:
        log.fatal("Failed to flag remaining alerts as unsuppressed", e)

    return [name[1] for name in suppression_view_list]


def main():
    ctx = connect()
    for squelch_name in load_rules(ctx, ALERT_SQUELCH_POSTFIX):
        run_suppressions(squelch_name)
    flag_remaining_alerts(ctx)


if __name__ == '__main__':
    main()
