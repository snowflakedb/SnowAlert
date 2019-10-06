import datetime
import requests
from os import environ
from runners.helpers import db, log

AGARI_TOKEN = environ.get('AGARI_TOKEN')
AGARI_SECRET = environ.get('AGARI_SECRET')
AGARI_TABLE = environ.get('AGARI_TABLE')

AGARI_TRUST_CUTOFF = 5.0

URLS = ['https://api.agari.com/v1/ep/messages']


# Agari provides bearer auth tokens,
def gen_headers():
    r = requests.post(
        'https://api.agari.com/oauth/token',
        data={'client_id': AGARI_TOKEN, 'client_secret': AGARI_SECRET},
    )
    token = r.json()['access_token']
    return {'Authorization': f'Bearer {token}'}


def load_data(messages):
    data = [(m, m['date']) for m in messages]
    try:
        db.insert(AGARI_TABLE, data, select='PARSE_JSON(column1), column2')
    except Exception as e:
        log.error("failed to ingest data", e)


def get_newest_timestamp():
    # check table in snowflake and get most recent timestamp
    query = f"SELECT raw FROM {AGARI_TABLE} ORDER BY event_time DESC LIMIT 1"
    try:
        return list(db.fetch(query))[0]['RAW']['date']
    except Exception:
        log.error("no earlier data found")
        return None


def enrich(message, headers):
    url = f"https://api.agari.com/v1/ep/messages/{message['id']}"
    r = requests.get(url=url, headers=headers)
    data = r.json()
    return data['message']


def process_endpoint(url):
    params = {'start_date': get_newest_timestamp(), 'limit': 100, 'offset': 0}
    headers = gen_headers()

    # If there's no start date defined. Agari defaults to one day ago.
    if params['start_date'] is None:
        params.pop('start_date')

    while True:
        r = requests.get(url=url, params=params, headers=headers)
        data = r.json()
        log.info(params)

        if r.status_code != 200:
            log.error(f"Ingest request for {url} failed.")
            db.record_failed_ingestion(AGARI_TABLE, r, datetime.datetime.utcnow())
            break

        for message in data['messages']:
            if float(message['message_trust_score']) <= AGARI_TRUST_CUTOFF:
                message['details'] = enrich(message, headers)

        load_data(data['messages'])

        if params['offset'] == 9900:  # Maximum offset is 9900
            params = {'start_date': get_newest_timestamp(), 'limit': 100, 'offset': 0}

        elif (
            len(data['messages']) == params['limit']
        ):  # if we got the limit of messages, get the next page
            params['offset'] += params['limit']
        else:
            break


def main():
    reqenv = {'AGARI_TOKEN', 'AGARI_SECRET', 'AGARI_TABLE'}
    missingenv = reqenv - set(environ)
    if missingenv:
        log.fatal(f"missing env vars: {missingenv}")

    for url in URLS:
        process_endpoint(url)


if __name__ == '__main__':
    if db.connect():
        main()
