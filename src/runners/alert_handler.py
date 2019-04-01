#!/usr/bin/env python

import json
import datetime
import uuid

from .config import CLOUDWATCH_METRICS
from .helpers import db, log
from .plugins import create_jira


def log_alerts(ctx, alerts):
    """We don't normally create alerts in this function, but we want to be able to push an alert to the table if
    something goes wrong.
    """
    if len(alerts):
        print("Recording alerts.")
        try:
            ctx.cursor().execute(
                f'''
                INSERT INTO results.alerts (alert_time, alert)
                SELECT PARSE_JSON(column1):ALERT_TIME,
                       PARSE_JSON(column1)
                FROM VALUES {", ".join(["(%s)"] * len(alerts))};
                ''',
                alerts
            )
        except Exception as e:
            log.error("Failed to log alert", e)

    else:
        print("No alerts to log.")


def log_failure(ctx, alert, e):
    alerts = [json.dumps({
        'ALERT_ID': uuid.uuid4().hex,
        'QUERY_ID': 'db9fa0d114d54b5ca1a195e34fb8752b',
        'QUERY_NAME': 'Alert Handler Failure',
        'ENVIRONMENT': 'SnowAlert',
        'SOURCES': ['Alerts Table'],
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
        ctx.cursor().execute(f"DELETE FROM results.alerts where ALERT:ALERT_ID = '{alert['ALERT_ID']}';")
    except Exception as e:
        log.error("Failed to log alert creation failure", e)


def get_new_alerts(ctx):
    get_alerts_query = f"""
        SELECT * FROM results.alerts
        WHERE ticket IS NULL
        AND suppressed=FALSE
        ORDER BY EVENT_TIME ASC LIMIT 100
        """
    results = ctx.cursor().execute(get_alerts_query).fetchall()
    print(f'Found {len(results)} new alerts.')
    return results


def record_ticket_id(ctx, ticket_id, alert_id):
    query = f"UPDATE results.alerts SET ticket='{ticket_id}' WHERE alert:ALERT_ID='{alert_id}'"
    print('Updating alert table:', query)
    ctx.cursor().execute(query)

# We get a list of alerts that don't have tickets. For each alert, check the correlation_id of the alert; if there is no
# alert that has that correlation_id and a ticket_id, create a ticket for that alert. if there is an alert with a matching
# correlation_id and a ticket_id, update the body of the ticket with the new alert instead.


def main():
    ctx = db.connect()
    alerts = get_new_alerts(ctx)
    log.info(f'Found {len(alerts)} new alerts to handle.')

    for row in alerts:
        alert = json.loads(row[0])
        try:
            correlation_id = row[7]
        except Exception:
            log.info(f"Warning: no VARCHAR CORRELATION_ID column in alerts table")
            try:
                ticket_id = create_jira.create_jira_ticket(alert)
            except Exception as e:
                log.error(e, f"Failed to create ticket for alert {alert}")
                log_failure(ctx, alert, e)
                continue
            continue

        log.info('Creating ticket for alert', alert)

        CORRELATION_QUERY = f"""
            SELECT *
            FROM results.alerts
            WHERE correlation_id = '{correlation_id}'
              AND ticket IS NOT NULL
            ORDER BY EVENT_TIME DESC
            LIMIT 1
        """

        # We check against the correlation ID for alerts in that correlation with the same ticket
        correlated_results = ctx.cursor().execute(CORRELATION_QUERY).fetchall() if correlation_id else []

        log.info(f"Discovered {len(correlated_results)} correlated results")

        if len(correlated_results) > 0:

            # There is a correlation with a ticket that exists, so we should append to that ticket
            ticket_id = correlated_results[0][3]
            ticket_status = create_jira.check_ticket_status(ticket_id)

            if ticket_status == 'To Do':
                try:
                    create_jira.append_to_body(ticket_id, alert)
                except Exception as e:
                    log.error(f"Failed to append alert {alert['ALERT_ID']} to ticket {ticket_id}.", e)
                    try:
                        ticket_id = create_jira.create_jira_ticket(alert)
                    except Exception as e:
                        log.error(e, f"Failed to create ticket for alert {alert}")
                        log_failure(ctx, alert, e)
                    continue
            else:
                # The ticket is already in progress, we shouldn't change it
                # Create a new ticket in JIRA for the alert
                try:
                    ticket_id = create_jira.create_jira_ticket(alert)
                except Exception as e:
                    log.error(e, f"Failed to create ticket for alert {alert}")
                    log_failure(ctx, alert, e)
                    continue
        else:
            # There is no correlation with a ticket that exists
            # Create a new ticket in JIRA for the alert
            try:
                ticket_id = create_jira.create_jira_ticket(alert)
            except Exception as e:
                log.error(e, f"Failed to create ticket for alert {alert}")
                log_failure(ctx, alert, e)
                continue

        # Record the new ticket id in the alert table
        record_ticket_id(ctx, ticket_id, alert['ALERT_ID'])

    try:
        if CLOUDWATCH_METRICS:
            log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Alert Handler'}], 1)
    except Exception as e:
        log.error("Cloudwatch metric logging failed", e)


if __name__ == "__main__":
    import os
    if os.environ.get('JIRA_USER'):
        main()
