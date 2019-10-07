import requests
from requests.auth import HTTPBasicAuth
import datetime
import json
from os import environ

from runners.helpers import db, log

ZENGRC_ID = environ.get('ZENGRC_ID')
ZENGRC_SECRET = environ.get('ZENGRC_SECRET')
ZENGRC_URL = environ.get('ZENGRC_URL')
ZENGRC_TABLE = environ.get('ZENGRC_TABLE')
TIMESTAMP = str(datetime.datetime.utcnow())


# 5 minutes short a day, so that we don't bump start up every other day
GET_FRESH_ENTRIES_QUERY = f"""
SELECT raw FROM {ZENGRC_TABLE}
WHERE event_time > DATEADD(MINUTE, -24*60-5, CURRENT_TIMESTAMP())
"""

ENDPOINTS = [
    '/api/v2/assessments',
    '/api/v2/audits',
    '/api/v2/issues',
    '/api/v2/requests',
    '/api/v2/risks',
]


def get(path):
    url = ZENGRC_URL + path
    return requests.get(url, auth=HTTPBasicAuth(ZENGRC_ID, ZENGRC_SECRET))


def process_endpoint(endpoint):
    log.info(f"starting {endpoint}")
    json_body = {'links': {'next': {'href': endpoint}}}
    page = 1
    while json_body['links']['next'] is not None:
        log.info(f"Getting page {str(page)}")

        r = get(json_body['links']['next']['href'])
        if r.status_code != 200:
            log.error(f"Ingest request for {endpoint} failed", r.text)
            db.record_failed_ingestion(ZENGRC_TABLE, r, TIMESTAMP)
            break

        json_body = r.json()
        data = [[json.dumps(i), TIMESTAMP] for i in json_body['data']]
        try:
            db.insert(ZENGRC_TABLE, data, select='PARSE_JSON(column1), column2')
            page += 1
        except Exception as e:
            log.error(e)


def main():
    reqenv = {'ZENGRC_ID', 'ZENGRC_SECRET', 'ZENGRC_URL', 'ZENGRC_TABLE'}
    missingenv = reqenv - set(environ)
    if missingenv:
        log.fatal(f"missing env vars: {missingenv}")

    print("starting")

    last_time = list(db.fetch(GET_FRESH_ENTRIES_QUERY))

    if len(last_time) == 0:
        for e in ENDPOINTS:
            process_endpoint(e)
    else:
        log.info("Not time to ingest ZenGRC data")


if __name__ == "__main__":
    main()
