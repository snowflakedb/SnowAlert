#!/usr/bin/env python

import json
import hashlib
import uuid
import datetime
from typing import Any, Dict, List, Tuple

from runners.config import (
    RUN_ID,
    QUERY_METADATA_TABLE,
    RUN_METADATA_TABLE,
    ALERT_QUERY_POSTFIX,
    CLOUDWATCH_METRICS,
)
from runners.helpers import db, log
from runners.utils import groups_of


GROUPING_CUTOFF = f"DATEADD(minute, -90, CURRENT_TIMESTAMP())"
QUERY_HISTORY: List = []


def alert_group(alert) -> str:
    return hashlib.md5(
        f"{alert['OBJECT']}{alert['DESCRIPTION']}".encode('utf-8')
    ).hexdigest()


def log_alerts(ctx, alerts):
    if len(alerts):
        print("Recording alerts.")
        try:
            VALUES_INSERT_LIMIT = 16384
            for alert_group in groups_of(VALUES_INSERT_LIMIT, alerts):
                db.insert_alerts(list(filter(None, alert_group)))

        except Exception as e:
            log.error("Failed to log alert", e)

    else:
        print("No alerts to log.")


def log_failure(ctx, query_name, e, event_data=None, description=None):
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
        log_alerts(ctx, alerts)
        log.info("Query failure logged.", e)

    except Exception as e:
        log.error("Failed to log query failure", e)


def create_alerts(ctx, rule_name: str) -> Tuple[int, int]:
    metadata: Dict[str, Any] = {
        'QUERY_NAME': rule_name,
        'RUN_ID': RUN_ID,
        'ATTEMPTS': 1,
        'START_TIME': datetime.datetime.utcnow(),
    }

    try:
        insert_count, update_count = db.insert_alerts_query_run(rule_name, GROUPING_CUTOFF)
        metadata['ROW_COUNT'] = {
            'INSERTED': insert_count,
            'UPDATED': update_count,
        }

    except Exception as e:
        log_failure(ctx, rule_name, e)
        log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE, e=e)
        return 0, 0

    log.metadata_record(ctx, metadata, table=QUERY_METADATA_TABLE)
    QUERY_HISTORY.append(metadata)

    log.info(f"{rule_name} done.")

    return insert_count, update_count


def main(rule_name=None):
    RUN_METADATA = {
        'RUN_ID': RUN_ID,
        'RUN_TYPE': 'ALERT QUERY',
        'START_TIME': datetime.datetime.utcnow(),
    }
    ctx = db.connect_and_execute("ALTER SESSION SET USE_CACHED_RESULT=FALSE;")
    if rule_name:
        create_alerts(ctx, rule_name)
    else:
        for rule_name in db.load_rules(ctx, ALERT_QUERY_POSTFIX):
            create_alerts(ctx, rule_name)

    RUN_METADATA['ROW_COUNT'] = {
        'INSERTED': sum(q['ROW_COUNT']['INSERTED'] for q in QUERY_HISTORY),
        'UPDATED': sum(q['ROW_COUNT']['UPDATED'] for q in QUERY_HISTORY),
    }
    log.metadata_record(ctx, RUN_METADATA, table=RUN_METADATA_TABLE)

    try:
        if CLOUDWATCH_METRICS:
            log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Alert Query Runner'}], 1)
    except Exception as e:
        log.error("Cloudwatch metric logging failed: ", e)


if __name__ == '__main__':
    main()
