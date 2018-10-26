#!/usr/bin/env python

import json
from plugins import create_jira
import datetime
import uuid

from helpers import log
from helpers.db import connect_and_execute

from config import ALERTS_TABLE, DATABASE, WAREHOUSE, CLOUDWATCH_METRICS


def log_alerts(ctx, alerts):
    """We don't normally create alerts in this function, but we want to be able to push an alert to the table if
    something goes wrong.
    """
    if len(alerts):
        print("Logging alerts...")
        try:
            ctx.cursor().execute(
                f'''
                INSERT INTO {ALERTS_TABLE}(alert_time, alert)
                SELECT PARSE_JSON(column1):ALERT_TIME,
                       PARSE_JSON(column1)
                FROM VALUES {", ".join(["(%s)"] * len(alerts))};
                ''',
                alerts
            )
        except Exception as e:
            log.fatal("Failed to log alert", e)
    else:
        print("No alerts to log.")


def log_failure(ctx, alert, e):
    alerts = [json.dumps({
        'ALERT_ID': uuid.uuid4().hex,
        'QUERY_ID': 'db9fa0d114d54b5ca1a195e34fb8752b',
        'QUERY_NAME': 'Alert Handler Failure',
        'ENVIRONMENT': 'SnowAlert',
        'SOURCES': 'Alerts Table',
        'ACTOR': 'Alert Handler',
        'OBJECT': alert['ALERT_ID'],
        'ACTION': 'The Alert Handler failed to create a ticket',
        'TITLE': 'Alert Handler Failure',
        'EVENT_TIME': str(datetime.datetime.utcnow()),
        'ALERT_TIME': str(datetime.datetime.utcnow()),
        'DESCRIPTION': f"The alert with ID '{alert['ALERT_ID']}' failed to create with error: {e!r}",
        'DETECTOR': 'Alert Handler',
        'EVENT_DATA': str(alert),
        'SEVERITY': 'High'
    })]
    try:
        log_alerts(ctx, alerts)
        ctx.cursor().execute(f"DELETE FROM {ALERTS_TABLE} where ALERT:ALERT_ID = '{alert['ALERT_ID']}';")
    except Exception as e:
        log.fatal("Failed to log alert creation failure", e)


def get_new_alerts(connection):
    get_alerts_query = f'SELECT * FROM {ALERTS_TABLE} WHERE ticket IS NULL AND suppressed=FALSE LIMIT 100'
    results = connection.cursor().execute(get_alerts_query).fetchall()
    print('Found', len(results), 'new alerts.')
    return results


def record_ticket_id(connection, ticket_id, alert_id):
    query = f"UPDATE {ALERTS_TABLE} SET ticket='{ticket_id}' WHERE alert:ALERT_ID='{alert_id}'"
    print('Updating alert table:', query)
    connection.cursor().execute(query)


def main():
    ctx = connect_and_execute([
        f'USE WAREHOUSE {WAREHOUSE};',
        f'USE DATABASE {DATABASE};',
    ])
    alerts = get_new_alerts(ctx)
    print('Found', len(alerts), 'new alerts to handle.')

    for row in alerts:
        try:
            alert = json.loads(row[0])
        except Exception as e:
            log.error("Failed unexepctedly", e)
            continue
        print('Creating ticket for alert', alert)

        # Create a new ticket in JIRA for the alert
        try:
            ticket_id = create_jira.create_jira_ticket(
                alert_id=alert['ALERT_ID'],
                query_id=alert['QUERY_ID'],
                query_name=alert['QUERY_NAME'],
                environment=alert['ENVIRONMENT'],
                sources=alert['SOURCES'],
                actor=alert['ACTOR'],
                object=alert['OBJECT'],
                action=alert['ACTION'],
                title=alert['TITLE'],
                event_time=alert['EVENT_TIME'],
                alert_time=alert['ALERT_TIME'],
                description=alert['DESCRIPTION'],
                detector=alert['DETECTOR'],
                event_data=alert['EVENT_DATA'],
                severity=alert['SEVERITY'])
        except Exception as e:
            log.error(e, f"Failed to create ticket for alert {alert}")
            log_failure(ctx, alert, e)
            continue

        # Record the new ticket id in the alert table
        record_ticket_id(ctx, ticket_id, alert['ALERT_ID'])
    if {CLOUDWATCH_METRICS}:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Alert Handler'}], 1)


if __name__ == "__main__":
    import os
    if os.environ.get('JIRA_USER'):
        main()
