import requests
import base64
import datetime
import json
import os

from runners.helpers import db, log

ZENGRC_ID = os.environ.get('ZENGRC_ID')
ZENGRC_SECRET = os.environ.get('ZENGRC_SECRET')
ZENGRC_URL = os.environ.get('ZENGRC_URL')
ZENGRC_TABLE = os.environ.get('ZENGRC_TABLE')
TIMESTAMP = str(datetime.datetime.utcnow())


def gen_headers(id, secret):
    auth = base64.b64encode(f'{id}:{secret}'.encode()).decode()
    return {"Authorization": f"Basic {auth}"}


def loop(endpoint):
    print(f"starting {endpoint}")
    j = {'links': {'next': {'href': endpoint}}}
    headers = gen_headers(ZENGRC_ID, ZENGRC_SECRET)
    page = 1
    while j['links']['next'] is not None:
        print(f"Getting page {str(page)}")
        r = requests.get(ZENGRC_URL+j['links']['next']['href'], headers=headers)

        if r.status_code != 200:
            log.error(f"Ingest request for {endpoint} failed", r.text)
            db.ingest_request_failed(ZENGRC_TABLE, r, TIMESTAMP)
            break

        j = json.loads(r.text)
        data = [[json.dumps(i), TIMESTAMP] for i in j['data']]
        if len(data) > 0:
            try:
                query = f"INSERT INTO {ZENGRC_TABLE} select parse_json(column1), column2 from values "
                query = query + ", ".join(["(%s)"] * len(data))
                db.execute(query, params=data)
                page += 1
            except Exception as e:
                print(e)


def main():
    print("starting")
    endpoints = ['/api/v2/assessments',
                 '/api/v2/audits',
                 '/api/v2/issues',
                 '/api/v2/requests',
                 '/api/v2/risks']

    # 1435 is five minutes short of one day, to prevent slow drift of ingest time.
    last_time = list(db.fetch(f"""SELECT raw FROM {ZENGRC_TABLE}
                where EVENT_TIME < DATEADD(MINUTE, 1435, CURRENT_TIMESTAMP())"""))

    if len(last_time) == 0:
        for e in endpoints:
            loop(e)
    else:
        log.info("Not time to ingest ZenGRC data")


if __name__ == "__main__":
    main()
