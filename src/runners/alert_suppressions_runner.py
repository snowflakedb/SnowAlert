#!/usr/bin/env python

import json
import uuid
import datetime
from typing import List

from runners.config import (
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    ALERT_SQUELCH_POSTFIX,
    CLOUDWATCH_METRICS,
    RUN_ID
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


def log_alerts(ctx, alerts):
    """We don't usually log alerts in the suppression runner, but we want the runner to create an alert if a
    suppression fails to execute.
    """
    if len(alerts):
        print("Recording alerts.")
        format_string = ", ".join(["(%s)"] * len(alerts))
        try:
            ctx.cursor().execute(
                f'''
                INSERT INTO results.alerts (alert_time, alert)
                SELECT PARSE_JSON(column1):ALERT_TIME,
                       PARSE_JSON(column1)
                FROM VALUES {format_string};
                ''',
                alerts
            )
        except Exception as e:
            log.error("Failed to log alert", e)
    else:
        print("No alerts to log.")


def log_failure(ctx, suppression_name, e, event_data=None, description=None):
    if event_data is None:
        event_data = f"The suppression '{suppression_name}' failed to execute with error: {e}"

    if description is None:
        description = f"The suppression '{suppression_name}' failed to execute with error: {e}"

    alerts = [json.dumps({
        'ALERT_ID': uuid.uuid4().hex,
        'QUERY_ID': 'b1d02051dd2c4d62bb75274f2ee5996a',
        'QUERY_NAME': 'Suppression Runner Failure',
        'ENVIRONMENT': 'Suppressions',
        'SOURCES': 'Suppression Runner',
        'ACTOR': 'Suppression Runner',
        'OBJECT': suppression_name,
        'ACTION': 'Suppression Execution',
        'TITLE': 'Suppression Runner Failure',
        'EVENT_TIME': str(datetime.datetime.utcnow()),
        'ALERT_TIME': str(datetime.datetime.utcnow()),
        'DESCRIPTION': description,
        'DETECTOR': 'Suppression Runner',
        'EVENT_DATA': event_data,
        'SEVERITY': 'High',
    })]

    try:
        log_alerts(ctx, alerts)
        log.error(f"{suppression_name} failure successfully logged", e)

    except Exception as e:
        log.error("Failed to log suppression failure", e)


def run_suppression_query(squelch_name):
    try:
        query = SUPPRESSION_QUERY.format(suppression_name=squelch_name)
        return next(db.fetch(query, fix_errors=False))['number of rows updated']
    except Exception:
        log.info(f"{squelch_name} warning: query broken, might need 'id' column, trying 'alert:ALERT_ID'.")
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

    ctx = db.connect()

    try:
        suppression_count = run_suppression_query(squelch_name)
        log.info(f"{squelch_name} updated {suppression_count} rows.")
        metadata['ROW_COUNT'] = {'SUPPRESSED': suppression_count}
        log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE)

    except Exception as e:
        log_failure(ctx, squelch_name, e)
        metadata['ROW_COUNT'] = {'SUPPRESSED': 0}
        log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE, e=e)

    METADATA_HISTORY.append(metadata)
    log.info(f"{squelch_name} done.")


def main():
    RUN_METADATA = {
        'RUN_TYPE': 'ALERT SUPPRESSION',
        'START_TIME': datetime.datetime.utcnow(),
        'RUN_ID': RUN_ID,
    }

    ctx = db.connect()
    for squelch_name in db.load_rules(ctx, ALERT_SQUELCH_POSTFIX):
        run_suppressions(squelch_name)

    num_rows_updated = next(db.fetch(ctx, SET_SUPPRESSED_FALSE))['number of rows updated']
    log.info(f'All suppressions done, {num_rows_updated} remaining alerts marked suppressed=FALSE.')

    RUN_METADATA['ROW_COUNT'] = {
        'PASSED': num_rows_updated,
        'SUPPRESSED': sum(m['ROW_COUNT']['SUPPRESSED'] for m in METADATA_HISTORY),
    }

    log.metadata_record(ctx, RUN_METADATA, table=RUN_METADATA_TABLE)

    if CLOUDWATCH_METRICS:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Alert Suppression Runner'}], 1)


if __name__ == '__main__':
    main()
