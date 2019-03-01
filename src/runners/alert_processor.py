#!/usr/bin/env python

import json
import uuid

from .config import ALERTS_TABLE, DATABASE
from .helpers import db, log

# After alerts are created but before they get turned into tickets, they get processed; this processing step is where logic like alert grouping is applied.

# Alert grouping is the process by which separate alerts are determined to be related and grouped together. Alerts which are grouped should share a GROUP_ID.
# Two alerts are related if a) they happen within one hour of each other, b) they share an ACTOR, and c) they share either an ACTION or an OBJECT.

# Note that grouping is divorced from how alerts are represented in something like a Jira ticket; a group might be represented over two or more jira tickets
# while still being considered a single group.

GROUPING_PERIOD = -60


def get_group_id(ctx, alert):
    # In order to define a group, two alerts need to happen within an hour of each other, share an actor, and share either an action or an object
    # if any of these fields are missing, the alert is not going to be useful for investigation
    try:
        actor = alert['ACTOR']
        object = alert['OBJECT']
        action = alert['ACTION']
        time = alert['EVENT_TIME']
    except Exception as e:
        log.error(f"Alert missing a required field: {e.args[0]}", e)
        return uuid.uuid4().hex

    # select the most recent alert which matches the grouping logic

    query = f"""select * from {ALERTS_TABLE}
    where alert:ACTOR = '{actor}'
    and (alert:OBJECT = '{object}' or alert:ACTION = '{action}')
    and GROUP_ID is not null
    and event_time > dateadd(minutes, {GROUPING_PERIOD}, '{time}')
    order by event_time desc
    limit 1
    """

    try:
        match = ctx.cursor().execute(query).fetchall()
    except Exception as e:
        log.error("Failed unexpectedly while getting group matches", e)

    if len(match) > 0:
        try:
            group_id = match[0][7]
        except Exception:
            group_id = uuid.uuid4().hex
    else:
        group_id = uuid.uuid4().hex

    return group_id


# group id is going to be a column inside the json of the alert, which means we can't easily replace just that part in sql, we need to modify the whole thing.
# this probably means deconstructing the alert into json, modifying the json, and then updating the table with the new json. This might be a bit tricky.

def assess_grouping(ctx):

    get_alerts = f"""select * from {ALERTS_TABLE}
    where GROUP_ID is null
    and alert_time > dateadd(hour, -2, current_timestamp())
    """

    alerts = ctx.cursor().execute(get_alerts).fetchall()
    log.info(f"Found {len(alerts)} for grouping")

    for row in alerts:
        try:
            alert_body = json.loads(row[0])
        except Exception as e:
            log.error("Failed unexpectedly while loading alert inside alert_processor.assess_grouping", e)
            continue

        alert_id = alert_body['ALERT_ID']
        group_id = get_group_id(ctx, alert_body)
        log.info(f"the group id for alert {alert_id} is {group_id}")

        q = f"""UPDATE {ALERTS_TABLE} SET GROUP_ID = '{group_id}'
                WHERE ALERT:ALERT_ID = '{alert_id}'
            """

        try:
            ctx.cursor().execute(q)
            log.info("group id successfully updated")
        except Exception as e:
            log.error(f"Failed to update alert {alert_id} with new group id", e)
            continue


def main():
    ctx = db.connect_and_execute(f'USE DATABASE {DATABASE};')
    assess_grouping(ctx)
