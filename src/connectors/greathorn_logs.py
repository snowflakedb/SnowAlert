# from runners.helpers import db, log
# from runners.helpers.dbconfig import ROLE as SA_ROLE
# from .utils import yaml_dump

import requests
import os
import json
from datetime import datetime
from datetime import timedelta
import time

import pprint

PAGE_SIZE=200

CONNECTION_OPTIONS = [
    {
        'name': 'api_key',
        'title': "API Token",
        'prompt': "This available in your GreatHorn",
        'type': 'str',
        'secret': True,
        'required': True,
    },
]

LANDING_EVENTS_TABLE_COLUMNS = [('raw', 'VARIANT', 'RECORDED_AT', 'TIMESTAMP_LTZ' )]

# def connect(connection_name, options):
#     table_name = f'greathorn_{connection_name}'
#     landing_events_table = f'data.{table_name}_events_connection'

#     db.create_table(
#         name=landing_events_table,
#         cols=LANDING_EVENTS_TABLE_COLUMNS,
#         comment=yaml_dump(module='duo', **options),
#         rw_role=ROLE,
#     )

#     return {
#         'newStage': 'finalized',
#         'newMessage': "Events table for Greathorn created!",
#     }

def ingest(table_name, options,  dryrun=False):
    landing_table = f'data.{table_name}'
    url = "https://api.greathorn.com/v2/search/events"
    #token = options['api_key']
    token = os.environ["GH_TOKEN"]

    # starttime = db.fetch_latest(landing_table, 'event_time')
    # if ts is None:
    #     log.error(
    #         "Unable to find a timestamp of most recent Okta log, "
    #         "defaulting to one hour ago"
    #     )
    #     
    starttime = (datetime.utcnow() - timedelta(hours=1))
    endtime = datetime.utcnow()
    f_filters = [{"startDate":starttime.strftime("%Y-%m-%d, %H:%M:%S"), "endDate":endtime.strftime("%Y-%m-%d, %H:%M:%S")}]
    print(f"Today is {starttime}")
    offset = 0

    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json",
    }
    
    while True: 
        print(f'Starttime: {starttime}, endtime:{endtime}, offset:{offset}')
        first_data = json.dumps( { "limit": PAGE_SIZE, "offset": offset, "filters": f_filters } )
        response = requests.post(url, headers=headers, data=first_data)
        if response.status_code != 200:
            print(f"Not 200, {response.status_code}")
            print(f"{response.text}")
            yield 0
        
        results=response.json()
        if results['total'] > 9500:
            print('We\'ve covered too much time, lets cut in half')
            #Cut the time in half - start at the same place, but end halfway there. 
            timediff = endtime - starttime
            halfthetime = timediff/2
            endtime -= halfthetime
            f_filters = [{"startDate":starttime.strftime("%Y-%m-%d, %H:%M:%S"), "endDate":endtime.strftime("%Y-%m-%d, %H:%M:%S")}]
            offset = 0 
            yield 0


        events = results['results']

        len_events=len(events)
        if len_events == 0:
            if (datetime.utcnow() - timedelta(minutes=5)) >= endtime:
                #We have ended but we're still not up to current. 
                timediff = endtime - starttime
                starttime = endtime
                endtime += timediff
                f_filters = [{"startDate":starttime.strftime("%Y-%m-%d, %H:%M:%S"), "endDate":endtime.strftime("%Y-%m-%d, %H:%M:%S")}]
                offset = 0
                yield 0
            else:
                break

        # db.insert(
        #     landing_table,
        #     [{'raw': event, 'recorded_at': timestamp} for event in events],
        #     dryrun=dryrun
        # )
        #log.info(f'Inserted {len_events} rows.')
        offset += len_events
        print(f"Total {results['total']} and offset: {offset}")
        yield len_events










token = os.environ["GH_TOKEN"]
options  = {'api_key': token }
for l in ingest('haha', options):
    print(l)