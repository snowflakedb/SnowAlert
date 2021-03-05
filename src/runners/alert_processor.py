#!/usr/bin/env python

import json
import uuid

from .helpers import db, log

import snowflake.connector

CORRELATION_PERIOD = -60


UPDATE_ALERT_CORRELATION_ID = f"""
UPDATE results.alerts
SET correlation_id='{{correlation_id}}'
WHERE alert:EVENT_TIME > DATEADD(minutes, {CORRELATION_PERIOD}, '{{event_time}}')
  AND alert:ALERT_ID='{{alert_id}}'
"""

GET_CORRELATED_ALERT = f"""
SELECT *
FROM results.alerts
WHERE alert:ACTOR::STRING = %s
  AND (alert:OBJECT::STRING = %s OR alert:ACTION::STRING = %s)
  AND correlation_id IS NOT NULL
  AND NOT IS_NULL_VALUE(alert:ACTOR)
  AND suppressed = FALSE
  AND event_time > DATEADD(minutes, {CORRELATION_PERIOD}, '{{time}}')
ORDER BY event_time DESC
LIMIT 1
"""

GET_ALERTS_WITHOUT_CORREALTION_ID = f"""
SELECT *
FROM results.alerts
WHERE correlation_id IS NULL
  AND suppressed = FALSE
  AND alert_time > DATEADD(hour, -2, CURRENT_TIMESTAMP())
"""


def get_correlation_id(ctx, alert):
    try:
        alert_actor = alert['ACTOR']
        alert_object = alert['OBJECT']
        alert_action = alert['ACTION']
        time = str(alert['EVENT_TIME'])

        # TODO: make robust by using data.alerts view
        if type(alert_actor) is list:
            actor_str = '","'.join(str(a) for a in alert_actor)
            alert_actor = f'["{actor_str}"]'

        if type(alert_object) is list:
            obj_str = '","'.join(str(o) for o in alert_object)
            alert_object = f'["{obj_str}"]'

        if type(alert_action) is list:
            action_str = '","'.join(str(a) for a in alert_action)
            alert_action = f'["{action_str}"]'

    except KeyError as e:
        log.error(f"Alert missing a required field: {e.args[0]}", e)
        return uuid.uuid4().hex

    # select the most recent alert which matches the correlation logic
    query = GET_CORRELATED_ALERT.format(time=time)

    try:
        match = list(db.fetch(query, params=[alert_actor, alert_object, alert_action]))
    except Exception as e:
        log.error("Failed unexpectedly while getting correlation matches", e)
        match = []

    correlation_id = (
        match[0]['CORRELATION_ID']
        if len(match) > 0 and 'CORRELATION_ID' in match[0]
        else uuid.uuid4().hex
    )

    return correlation_id


def assess_correlation(ctx):
    try:
        alerts = ctx.cursor().execute(GET_ALERTS_WITHOUT_CORREALTION_ID).fetchall()
    except snowflake.connector.errors.ProgrammingError:
        log.info("Unable to get correlation_id, skipping grouping.")
        return None

    for row in alerts:
        try:
            alert_body = json.loads(row[0])
        except Exception as e:
            log.error(
                "Failed unexpectedly while loading alert inside alert_processor.assess_correlation",
                e,
            )
            continue

        alert_id = alert_body['ALERT_ID']
        correlation_id = get_correlation_id(ctx, alert_body)
        event_time = str(alert_body['EVENT_TIME'])
        log.info(f"the correlation id for alert {alert_id} is {correlation_id}")

        try:
            ctx.cursor().execute(UPDATE_ALERT_CORRELATION_ID.format(**locals()))
            log.info("correlation id successfully updated")
        except Exception as e:
            log.error(f"Failed to update alert {alert_id} with new correlation id", e)
            continue


def main():
    ctx = db.connect()
    assess_correlation(ctx)
