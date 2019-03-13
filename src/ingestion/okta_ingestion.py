import alooma_pysdk
import datetime
from runners.helpers import db, log
import json
from os import environ
import requests

INPUT_TOKEN = environ.get('ALOOMA_OKTA_TOKEN')
ALOOMA_SDK = alooma_pysdk.PythonSDK(INPUT_TOKEN)
OKTA_API_KEY = environ.get('OKTA_API_KEY')

HEADERS = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': OKTA_API_KEY}


# This should at some point be made more modular so that it works with a bunch of ETL tools and not just alooma.


def process_logs(logs):
    for l in logs:
        ALOOMA_SDK.report(l)


def get_timestamp():

    # Once pipelines are more strongly integrated with the installer, this table should be a variable
    timestamp_query = """
        SELECT V:published from SECURITY.ALOOMA.OKTA
        order by V:published desc
        limit 1
        """
    try:
        ts = db.connect_and_fetchall(timestamp_query)[0][0]
        if len(ts) < 1:
            log.error("The okta timestamp is too short or doesn't exist; defaulting to one hour ago")
            ts = datetime.datetime.now() - datetime.timedelta(hours=1)
            ts = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    except Exception as e:
        log.error("Unable to find a timestamp of most recent okta log, defaulting to one hour ago", e)
        ts = datetime.datetime.now() - datetime.timedelta(hours=1)
        ts = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    return {'since': ts}


def main():
    url = 'https://snowbiz.okta.com/api/v1/logs'
    print("starting loop")
    timestamp = get_timestamp()
    while 1:
        print(f"url is ${url}")
        try:
            r = requests.get(url=url, headers=HEADERS, params=timestamp)
            process_logs(json.loads(r.text))
            print(len(r.text))
            if len(r.text) == 2:
                break
            url = r.headers['Link'].split(', ')[1].split(';')[0][1:-1]
        except Exception as e:
            print(e)

    alooma_pysdk.terminate()
