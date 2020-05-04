#!/usr/bin/env python

import importlib

from .config import CLOUDWATCH_METRICS
from .helpers import db, log
from .utils import apply_some, json_dumps

GET_ALERTS_QUERY = f"""
SELECT *
FROM results.alerts a1
WHERE IFF(alert:HANDLERS IS NULL, ticket IS NULL, handled IS NULL or 
           (select count(*) from results.alerts a2, lateral flatten(input => handled)
               where a2.correlation_id = a1.correlation_id and value:success::boolean = FALSE) > 0)
  AND suppressed=FALSE
ORDER BY event_time ASC
LIMIT 1000
"""


def get_new_alerts(ctx):
    results = db.fetch(GET_ALERTS_QUERY)
    return results


def record_status(results, alert_id):
    query = f"UPDATE results.alerts SET handled=%s WHERE alert:ALERT_ID='{alert_id}'"
    log.info('Updating alert table:', query)
    try:
        db.execute(query, params=json_dumps(results))
    except Exception as e:
        log.error(e, f"Failed to update alert {alert_id} with status {results}")


def main():
    ctx = db.connect()
    alert_rows = list(get_new_alerts(ctx))
    log.info(f'Found {len(alert_rows)} new alerts to handle.')

    for alert_row in alert_rows:
        alert = alert_row['ALERT']

        handled = alert_row['HANDLED']
        if handled:
            log.info(f'Found failed handle attempt for correlation ID {alert_row["CORRELATION_ID"]}')

        results = []

        handlers = alert.get('HANDLERS')
        handler_index = 0
        for handler in ['jira'] if handlers is None else handlers:
            if handler is None:
                results.append(None)

            else:
                if handled and handled[handler_index]['success']:
                    log.info(f'Skipping previously successful handler {handler}')
                    results.append(handled[handler_index])
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
                            log.error(e, 'handler failed')
                            result = {'success': False, 'details': e}

                    results.append(result)

            handler_index += 1

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
