"""SnowAlert Pager Duty handler

usage: OBJECT_CONSTRUCT( 'type', 'pd' )
      in alert query handlers column
"""

import os

from pdpyras import EventsAPISession, PDClientError

from runners.helpers import log
from runners.helpers import vault

# default value (input severity is replaced for it if not in the list) should be the last in the list
severityDictionary = ['critical', 'error', 'warning', 'info', 'unknown']

# used for testing, should probably go into a test, keeping here for now
testAlert = {
    'TITLE': 'test SnowAlert',
    'DETECTOR': 'SnowAlert',
    'SEVERITY': 'info',
    'QUERY_ID': 'CT_DELETE_LOG_GROUP',
    'DESCRIPTION': 'S: Subject Verb Predicate at 2020-03-08 03:29:50.987 Z',
}


def handle(
    alert,
    summary=None,
    source=None,
    dedup_key=None,
    severity=None,
    custom_details=None,
    pd_api_token=None,
):
    if 'PD_API_TOKEN' not in os.environ and pd_api_token is None:
        log.error(f"No PD_API_TOKEN in env, skipping handler.")
        return None

    pd_token_ct = pd_api_token or os.environ['PD_API_TOKEN']
    pd_token = vault.decrypt_if_encrypted(pd_token_ct)

    pds = EventsAPISession(pd_token)

    summary = summary or alert['DESCRIPTION']

    source = source or alert['DETECTOR']

    severity = severity or alert['SEVERITY']
    if severity not in severityDictionary:
        log.warn(
            f"Set severity to {severityDictionary[-1]}, "
            f"supplied {severity} is not in allowed values: {severityDictionary}"
        )
        severity = severityDictionary[-1]

    custom_details = custom_details or alert

    try:
        response = pds.trigger(
            summary, source, dedup_key, severity, custom_details=alert
        )
        log.info(f"triggered PagerDuty alert \"{summary}\" at severity {severity}")
        return response
    except PDClientError as e:
        log.error(f"Cannot trigger PagerDuty alert: {e.msg}")
        return None
