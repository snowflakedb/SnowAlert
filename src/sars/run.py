#!/usr/bin/env python
import csv
import sys
import logging
import os
import re
import multiprocessing
from os import environ
from queue import Queue

from runners.helpers import db


def pull_aws_data():
    finished = False
    offset = 0
    limit = 1000000
    finished = False
    aws_writer = None
    with open('aws_inventory.csv', 'w') as fou:
        while not finished:
            data = db.fetch(
                f'''SELECT distinct INSTANCE:InstanceId::string as instance_id
                , min(distinct case when value:"Key"='SFROLE' then value:"Value" else NULL end ) as role FROM (
    SELECT distinct INSTANCE FROM SNOWALERT.BASE_DATA.EC2_INSTANCE_SNAPSHOTS_T where timestamp > dateadd(day,-30,current_timestamp)
  and INSTANCE:"Tags" not like '%{{"Key":"SFROLE","Value":"XP"}}%' 
  and INSTANCE:"Tags" not like '%{{"Key":"SFROLE","Value":"IMS_PENDING_SHUTDOWN"}}%'
    ), lateral flatten(input=>INSTANCE:"Tags")
    group by instance_id  having ROLE != 'XP' AND ROLE != 'IMS_PENDING_SHUTDOWN' limit {limit} offset {offset}'''
            )
            num_results = 0
            for row in data:
                num_results += 1
                if aws_writer is None:
                    aws_writer = csv.DictWriter(fou, row.keys())
                    aws_writer.writeheader()
                aws_writer.writerow(row)
            if num_results < limit:
                finished = True
            offset += limit


def grab_osquery_details(deployments):
    osquery_schema = environ.get('SECURITY_SCHEMA')
    osquery_query = db.fetch("SHOW VIEWS LIKE 'osquery_v' IN {}".format(osquery_schema))
    query_text = None
    for row in osquery_query:
        query_text = row["text"]
    query_text = query_text.split('union all')
    for query in query_text:
        deployments.append(re.findall('from (.*)', query)[0])


def query_snowflake(query):
    global writer  # , lock
    finished = False
    offset = 0
    limit = 10000000
    while not finished:
        num_results = 0
        query_with_limit = query + " limit %s offset %s" % (limit, offset)
        data = db.fetch(query_with_limit)
        for row in data:
            num_results += 1
            # with lock:
            if writer is None:
                writer = csv.DictWriter(sys.stdout, row.keys())
                writer.writeheader()
            writer.writerow(row)
        if num_results < limit:
            finished = True
        offset += limit


pull_aws_data()
deployments = []
grab_osquery_details(deployments)

queries = []
for i in deployments:
    queries.append(
        "select raw:\"columns\":\"path\"::string as process, date_trunc(day, event_time) as day, raw:\"instance_id\" as instance_id, count(*) as hits from {} where event_time >= dateadd(day,-35,current_timestamp) AND event_time < dateadd(minute,-60,current_timestamp) AND NAME like 'process_events' group by 1,2,3 order by DAY, PROCESS, INSTANCE_ID".format(
            i
        )
    )


def init(l):
    global lock, writer
    lock = l
    writer = None


# l = multiprocessing.Lock()
# pool = multiprocessing.Pool(len(deployments),initializer=init, initargs=(l,))

# results = pool.map(query_snowflake, queries)
# pool.close()
# pool.join()
writer = None
for query in queries:
    query_snowflake(query)
