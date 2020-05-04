#!/usr/bin/env python

import datetime
import os
import pytz
from multiprocessing import Pool
from typing import Any, Dict

from runners.config import (
    POOLSIZE,
    RUN_ID,
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    ALERT_QUERY_POSTFIX,
    CLOUDWATCH_METRICS,
)
from runners.helpers import db, log


# There are three time window properties that will be combined in different ways depending on which are present.
# - SA_ALERT_FROM_TIME: a timestamp specifying the start of the window. If omitted, use SA_ALERT_CUTOFF_MINUTES relative
# to SA_ALERT_TO_TIME.
# - SA_ALERT_TO_TIME: a timestamp specifying the end of the window. If omitted, use the current timestamp.
# - SA_ALERT_CUTOFF_MINUTES: the length of the window relative to SA_ALERT_TO_TIME; only used if SA_ALERT_FROM_TIME is
# not present. Defaults to 90.
#
# So, the possible combinations to specify a window are:
# - All options omitted: use the last 90 minutes.
# - SA_ALERT_CUTOFF_MINUTES=x: use the last x minutes.
# - SA_ALERT_FROM_TIME=x: use the window from x to now.
# - SA_ALERT_TO_TIME=x: use the 90 minutes before / up to x.
# - SA_ALERT_TO_TIME=x, SA_ALERT_CUTOFF_MINUTES=y: use the y minutes before / up to x.
# - SA_ALERT_FROM_TIME=x, SA_ALERT_TO_TIME=y: use the window x to y.

# We'll also do some simple checks to prevent against SQL injections, since these values will be inserted as-is into
# the query.

# TODO this should probably be moved to a method, but there may be other logic that depends on these global variables
# or the fact that this would only be executed once on import in its current location (as opposed to calling an
# initialization method from the main method which could end up getting repeated, for example).

ALERT_CUTOFF_MINUTES = int(os.environ.get('SA_ALERT_CUTOFF_MINUTES', -90))
if ALERT_CUTOFF_MINUTES > 0:
    ALERT_CUTOFF_MINUTES = -ALERT_CUTOFF_MINUTES

timestamp_format = '%Y-%m-%d %H:%M:%S.%f'

# Get a single, consistent timestamp to use for the "to" time that will be the same for every alert query.
# This will return a timestamp_tz that will be compatible with any timestamp_ntz queries that also run,
# as it will be in the DB's default TZ.
current_timestamp_from_db = db.execute('select current_timestamp::string').fetchall()[0][0]

ALERTS_TO_TIME = os.environ.get('SA_ALERT_TO_TIME')
if ALERTS_TO_TIME:
    datetime.datetime.strptime(ALERTS_TO_TIME, timestamp_format)  # this just ensures the date string is valid
    ALERTS_TO_TIME = f"'{ALERTS_TO_TIME}'"
else:
    ALERTS_TO_TIME = f"'{current_timestamp_from_db}'"

ALERTS_FROM_TIME = os.environ.get('SA_ALERT_FROM_TIME')
if ALERTS_FROM_TIME:
    datetime.datetime.strptime(ALERTS_FROM_TIME, timestamp_format)
    ALERTS_FROM_TIME = f"'{ALERTS_FROM_TIME}'"
else:
    ALERTS_FROM_TIME = f"DATEADD(minute, {ALERT_CUTOFF_MINUTES}, {ALERTS_TO_TIME})"


RUN_ALERT_QUERY = f"""
CREATE TRANSIENT TABLE results.RUN_{RUN_ID}_{{query_name}} AS
SELECT OBJECT_CONSTRUCT(
         'ALERT_ID', UUID_STRING(),
         'QUERY_NAME', '{{query_name}}',
         'QUERY_ID', IFNULL(QUERY_ID::VARIANT, PARSE_JSON('null')),
         'ENVIRONMENT', IFNULL(ENVIRONMENT::VARIANT, PARSE_JSON('null')),
         'SOURCES', IFNULL(SOURCES::VARIANT, PARSE_JSON('null')),
         'ACTOR', IFNULL(ACTOR::VARIANT, PARSE_JSON('null')),
         'OBJECT', IFNULL(OBJECT::VARIANT, PARSE_JSON('null')),
         'ACTION', IFNULL(ACTION::VARIANT, PARSE_JSON('null')),
         'TITLE', IFNULL(TITLE::VARIANT, PARSE_JSON('null')),
         'EVENT_TIME', IFNULL(EVENT_TIME::VARIANT, PARSE_JSON('null')),
         'ALERT_TIME', IFNULL(ALERT_TIME::VARIANT, PARSE_JSON('null')),
         'DESCRIPTION', IFNULL(DESCRIPTION::VARIANT, PARSE_JSON('null')),
         'DETECTOR', IFNULL(DETECTOR::VARIANT, PARSE_JSON('null')),
         'EVENT_DATA', IFNULL(EVENT_DATA::VARIANT, PARSE_JSON('null')),
         'SEVERITY', IFNULL(SEVERITY::VARIANT, PARSE_JSON('null')),
         'HANDLERS', IFNULL(OBJECT_CONSTRUCT(*):HANDLERS::VARIANT, PARSE_JSON('null'))
       ) AS alert
     , alert_time
     , event_time
     , 1 AS counter
FROM rules.{{query_name}}
WHERE event_time BETWEEN {{from_time_sql}} AND {{to_time_sql}}
"""


MERGE_ALERTS = f"""MERGE INTO results.alerts AS alerts USING (

  SELECT ANY_VALUE(alert) AS alert
       , SUM(counter) AS counter
       , MIN(alert_time) AS alert_time
       , MIN(event_time) AS event_time

  FROM results.{{new_alerts_table}}
  GROUP BY alert:OBJECT, alert:DESCRIPTION

) AS new_alerts

ON (
  alerts.alert:OBJECT = new_alerts.alert:OBJECT
  AND alerts.alert:DESCRIPTION = new_alerts.alert:DESCRIPTION
  AND alerts.alert:EVENT_TIME > {{from_time_sql}}
)

WHEN MATCHED
THEN UPDATE SET counter = alerts.counter + new_alerts.counter

WHEN NOT MATCHED
THEN INSERT (alert, counter, alert_time, event_time)
     VALUES (
       new_alerts.alert,
       new_alerts.counter,
       new_alerts.alert_time,
       new_alerts.event_time
    )
;
"""


def merge_alerts(query_name, from_time_sql):
    log.info(f"{query_name} processing...")

    sql = MERGE_ALERTS.format(
        query_name=query_name,
        from_time_sql=from_time_sql,
        new_alerts_table=f"RUN_{RUN_ID}_{query_name}",
    )
    result = db.execute(sql, fix_errors=False).fetchall()
    created_count, updated_count = result[0]
    log.info(f"{query_name} created {created_count}, updated {updated_count} rows.")
    return created_count, updated_count


def create_alerts(rule_name: str) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {
        'QUERY_NAME': rule_name,
        'RUN_ID': RUN_ID,
        'ATTEMPTS': 1,
        'START_TIME': datetime.datetime.utcnow(),
        'ROW_COUNT': {'INSERTED': 0, 'UPDATED': 0},
    }

    try:
        db.execute(
            RUN_ALERT_QUERY.format(
                query_name=rule_name,
                from_time_sql=ALERTS_FROM_TIME,
                to_time_sql=ALERTS_TO_TIME,
            ),
            fix_errors=False,
        )
        insert_count, update_count = merge_alerts(rule_name, ALERTS_FROM_TIME)
        metadata['ROW_COUNT'] = {'INSERTED': insert_count, 'UPDATED': update_count}
        db.execute(f"DROP TABLE results.RUN_{RUN_ID}_{rule_name}")

    except Exception as e:
        db.record_metadata(metadata, table=QUERY_METADATA_TABLE, e=e)
        return metadata

    db.record_metadata(metadata, table=QUERY_METADATA_TABLE)

    log.info(f"{rule_name} done.")

    return metadata


def main(rule_name=None):
    RUN_METADATA = {
        'RUN_ID': RUN_ID,
        'RUN_TYPE': 'ALERT QUERY',
        'START_TIME': datetime.datetime.utcnow(),
    }
    if rule_name:
        metadata = [create_alerts(rule_name)]
    else:
        rules = list(db.load_rules(ALERT_QUERY_POSTFIX))
        metadata = Pool(POOLSIZE).map(create_alerts, rules)

    RUN_METADATA['ROW_COUNT'] = {
        'INSERTED': sum(q['ROW_COUNT']['INSERTED'] for q in metadata),
        'UPDATED': sum(q['ROW_COUNT']['UPDATED'] for q in metadata),
    }
    db.record_metadata(RUN_METADATA, table=RUN_METADATA_TABLE)

    try:
        if CLOUDWATCH_METRICS:
            log.metric(
                'Run',
                'SnowAlert',
                [{'Name': 'Component', 'Value': 'Alert Query Runner'}],
                1,
            )
    except Exception as e:
        log.error("Cloudwatch metric logging failed: ", e)


if __name__ == '__main__':
    main()
