import requests
from requests.auth import HTTPBasicAuth
import datetime
import json
import os

from runners.helpers import db, log

ZENGRC_ID = os.environ.get('ZENGRC_ID')
ZENGRC_SECRET = os.environ.get('ZENGRC_SECRET')
ZENGRC_URL = os.environ.get('ZENGRC_URL')
ZENGRC_TABLE = os.environ.get('ZENGRC_TABLE')
TIMESTAMP = str(datetime.datetime.utcnow())


def get_next(json_body):
    url_tail = json_body['links']['next']['href']
    return requests.get(ZENGRC_URL+url_tail, auth=HTTPBasicAuth(ZENGRC_ID, ZENGRC_SECRET))


def process_endpoint(endpoint):
    log.info(f"starting {endpoint}")
    json_body = {'links': {'next': {'href': endpoint}}}
    page = 1
    while json_body['links']['next'] is not None:
        log.info(f"Getting page {str(page)}")
        r = get_next(json_body)

        if r.status_code != 200:
            log.error(f"Ingest request for {endpoint} failed", r.text)
            db.record_failed_ingestion(ZENGRC_TABLE, r, TIMESTAMP)
            break

        json_body = json.loads(r.text)
        data = [[json.dumps(i), TIMESTAMP] for i in json_body['data']]
        try:
            db.insert(ZENGRC_TABLE, data, select='PARSE_JSON(column1), column2')
            page += 1 if len(data) > 0 else 0
        except Exception as e:
            log.error(e)


def main():
    print("starting")
    endpoints = ['/api/v2/assessments',
                 '/api/v2/audits',
                 '/api/v2/issues',
                 '/api/v2/requests',
                 '/api/v2/risks']

    # 1435 is five minutes short of one day, to prevent slow drift of ingest time.
    last_time = list(db.fetch(f"""SELECT raw FROM {ZENGRC_TABLE}
                where EVENT_TIME > DATEADD(MINUTE, -1435, CURRENT_TIMESTAMP())"""))

    if len(last_time) == 0:
        for e in endpoints:
            process_endpoint(e)
    else:
        log.info("Not time to ingest ZenGRC data")


if __name__ == "__main__":
    main()
