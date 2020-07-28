#!/usr/bin/env python

import importlib

from runners.config import CLOUDWATCH_METRICS
from runners.helpers import db, log
from runners.utils import apply_some, json_dumps

GET_ALERTS_QUERY = f"""
SELECT *
FROM results.alerts
WHERE IFF(alert:HANDLERS IS NULL, ticket IS NULL, handled IS NULL)
  AND suppressed=FALSE
ORDER BY event_time ASC
LIMIT 1000
"""


def get_new_alerts(ctx):
    results = db.fetch(GET_ALERTS_QUERY)
    return results


def record_status(results, alert_id):
    try:
        db.execute(
            f"UPDATE results.alerts "
            f"SET handled=PARSE_JSON(%s) "
            f"WHERE alert:ALERT_ID='{alert_id}'",
            params=[json_dumps(results)]
        )
    except Exception as e:
        log.error(e, f"Failed to update alert {alert_id} with status {results}")


def main():
    ctx = db.connect()
    alert_rows = list(get_new_alerts(ctx))
    log.info(f'Found {len(alert_rows)} new alerts to handle.')

    for alert_row in alert_rows:
        alert = alert_row['ALERT']
        results = []

        handlers = alert.get('HANDLERS')
        for handler in ['jira'] if handlers is None else handlers:
            if handler is None:
                results.append(None)

            else:
                if type(handler) is str:
                    handler = {'type': handler}

                if 'type' not in handler:
                    result = {
                        'success': False,
                        'error': 'missing type key',
                        'details': handler,
                    }

                else:
                    handler_type = handler['type']

                    handler_kwargs = handler.copy()
                    handler_kwargs.update(
                        {
                            'alert': alert,
                            'correlation_id': alert_row.get('CORRELATION_ID'),
                            'alert_count': alert_row['COUNTER'],
                        }
                    )

                    try:
                        handler_module = importlib.import_module(
                            f'runners.handlers.{handler_type}'
                        )
                        result = {
                            'success': True,
                            'details': apply_some(
                                handler_module.handle, **handler_kwargs
                            ),
                        }

                    except Exception as e:
                        result = {'success': False, 'details': e}

                results.append(result)

        record_status(results, alert['ALERT_ID'])

    try:
        if CLOUDWATCH_METRICS:
            log.metric(
                'Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Alert Handler'}], 1
            )
    except Exception as e:
        log.error("Cloudwatch metric logging failed", e)


if __name__ == "__main__":
    import os

    if os.environ.get('JIRA_USER'):
        main()
