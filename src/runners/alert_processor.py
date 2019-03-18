#!/usr/bin/env python

import json
import uuid

from .config import ALERTS_TABLE, DATABASE
from .helpers import db, log

CORRELATION_PERIOD = -60


def get_correlation_id(ctx, alert):
    try:
        actor = str(alert['ACTOR'])
        object = str(alert['OBJECT'])
        action = str(alert['ACTION'])
        time = str(alert['EVENT_TIME'])

        # todo make robust
        if type(object) is list:
            o = "', '".join(object)
            object = f"ARRAY_CONSTRUCT('{o}')"

    except Exception as e:
        log.error(f"Alert missing a required field: {e.args[0]}", e)
        return uuid.uuid4().hex

    # select the most recent alert which matches the correlation logic

    query = f"""select * from {ALERTS_TABLE}
    where alert:ACTOR = '%s'
    and (alert:OBJECT = '%s' or alert:ACTION = '%s')
    and correlation_ID is not null
    and suppressed = false
    and event_time > dateadd(minutes, {CORRELATION_PERIOD}, '{time}')
    order by event_time desc
    limit 1
    """

    try:
        match = ctx.cursor().execute(query, actor, object, action).fetchall()
    except Exception as e:
        log.error("Failed unexpectedly while getting correlation matches", e)
        match = []

    correlation_id = match[0][7] if len(match) > 0 and len(match[0]) > 7 else uuid.uuid4().hex

    return correlation_id


def assess_correlation(ctx):

    get_alerts = f"""select * from {ALERTS_TABLE}
    where correlation_id is null
    and suppressed = false
    and alert_time > dateadd(hour, -2, current_timestamp())
    """

    try:
        alerts = ctx.cursor().execute(get_alerts).fetchall()
        # alerts = db.fetch(ctx, get_alerts)
        # log.info(f"Found {len(alerts)} for correlation")
    except Exception as e:
        log.error("Unable to get correlation_id, skipping grouping", e)
        return None

    for row in alerts:
        try:
            alert_body = json.loads(row[0])
        except Exception as e:
            log.error("Failed unexpectedly while loading alert inside alert_processor.assess_correlation", e)
            continue

        alert_id = alert_body['ALERT_ID']
        correlation_id = get_correlation_id(ctx, alert_body)
        log.info(f"the correlation id for alert {alert_id} is {correlation_id}")

        q = f"""UPDATE {ALERTS_TABLE} SET correlation_ID = '{correlation_id}'
                WHERE ALERT:ALERT_ID = '{alert_id}'
            """

        try:
            ctx.cursor().execute(q)
            log.info("correlation id successfully updated")
        except Exception as e:
            log.error(f"Failed to update alert {alert_id} with new correlation id", e)
            continue


def main():
    ctx = db.connect_and_execute(f'USE DATABASE {DATABASE};')
    assess_correlation(ctx)
