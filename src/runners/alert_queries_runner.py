#!/usr/bin/env python

import json
import uuid
import datetime
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
from runners.utils import groups_of


GROUPING_CUTOFF = f"DATEADD(minute, -90, CURRENT_TIMESTAMP())"


def log_alerts(alerts):
    if len(alerts):
        print("Recording alerts.")
        try:
            VALUES_INSERT_LIMIT = 16384
            for alert_group in groups_of(VALUES_INSERT_LIMIT, alerts):
                db.insert_alerts(list(alert_group))

        except Exception as e:
            log.error("Failed to log alert", e)

    else:
        print("No alerts to log.")


def log_failure(query_name, e, event_data=None, description=None):
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
        log_alerts(alerts)
        log.info("Query failure logged.", e)

    except Exception as e:
        log.error("Failed to log query failure", e)


RUN_ALERT_QUERY = f"""
CREATE TRANSIENT TABLE results.RUN_{RUN_ID}_{{query_name}} AS
SELECT OBJECT_CONSTRUCT(
         'ALERT_ID', UUID_STRING(),
         'QUERY_NAME', '{{query_name}}',
         'QUERY_ID', IFNULL(QUERY_ID, PARSE_JSON('null')),
         'ENVIRONMENT', IFNULL(ENVIRONMENT, PARSE_JSON('null')),
         'SOURCES', IFNULL(SOURCES, PARSE_JSON('null')),
         'ACTOR', IFNULL(ACTOR, PARSE_JSON('null')),
         'OBJECT', IFNULL(OBJECT, PARSE_JSON('null')),
         'ACTION', IFNULL(ACTION, PARSE_JSON('null')),
         'TITLE', IFNULL(TITLE, PARSE_JSON('null')),
         'EVENT_TIME', IFNULL(EVENT_TIME, PARSE_JSON('null')),
         'ALERT_TIME', IFNULL(ALERT_TIME, PARSE_JSON('null')),
         'DESCRIPTION', IFNULL(DESCRIPTION, PARSE_JSON('null')),
         'DETECTOR', IFNULL(DETECTOR, PARSE_JSON('null')),
         'EVENT_DATA', IFNULL(EVENT_DATA, PARSE_JSON('null')),
         'SEVERITY', IFNULL(SEVERITY, PARSE_JSON('null')),
         'HANDLERS', IFNULL(OBJECT_CONSTRUCT(*):HANDLERS, PARSE_JSON('null'))
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
  AND alerts.alert:event_time > {{from_time_sql}}
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
    result = db.execute(sql).fetchall()
    created_count, updated_count = result[0]
    log.info(f"{query_name} created {created_count}, updated {updated_count} rows.")
    return created_count, updated_count


def create_alerts(rule_name: str) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {
        'QUERY_NAME': rule_name,
        'RUN_ID': RUN_ID,
        'ATTEMPTS': 1,
        'START_TIME': datetime.datetime.utcnow(),
        'ROW_COUNT': {
            'INSERTED': 0,
            'UPDATED': 0,
        }
    }

    try:
        db.execute(RUN_ALERT_QUERY.format(
            query_name=rule_name,
            from_time_sql="DATEADD(minute, -90, CURRENT_TIMESTAMP())",
            to_time_sql="CURRENT_TIMESTAMP()",
        ))
        insert_count, update_count = merge_alerts(rule_name, GROUPING_CUTOFF)
        metadata['ROW_COUNT'] = {
            'INSERTED': insert_count,
            'UPDATED': update_count,
        }
        db.execute(f"DROP TABLE results.RUN_{RUN_ID}_{rule_name}")

    except Exception as e:
        log_failure(rule_name, e)
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
        create_alerts(rule_name)
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
            log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Alert Query Runner'}], 1)
    except Exception as e:
        log.error("Cloudwatch metric logging failed: ", e)


if __name__ == '__main__':
    main()
