#!/usr/bin/env python
print("RUN.PY")
import csv
import sys
import logging
import os
import re
import multiprocessing
from queue import Queue
from runners.helpers import db

print("Starting")
def pull_aws_data():
    conn = db.connect()
    cur = conn.cursor()
    finished = False
    offset = 0
    limit = 1000000
    finished = False
    aws_writer=None
    with open('aws_inventory.csv','w') as fou:
        while(not finished):
            data = db.fetch(conn, "SELECT distinct instance_id::string as instance_id, min(distinct case when value:\"Key\"='SFROLE' then value:\"Value\" else NULL end ) as role FROM (SELECT instance_id,role FROM (SELECT distinct data:InstanceId as instance_id,  data:\"Tags\" as role, DATA:NetworkInterfaces as networkinterfaces FROM (SELECT distinct data FROM AWS_INVENTORY.SNAPSHOTS.INSTANCES where snapshot_at > dateadd(day,-30,current_timestamp)), lateral flatten(input => DATA:BlockDeviceMappings) WHERE DATA:\"Tags\" not like '%%{\"Key\":\"SFROLE\",\"Value\":\"XP\"}%%' and DATA:\"Tags\" not like '%%{\"Key\":\"SFROLE\",\"Value\":\"IMS_PENDING_SHUTDOWN\"}%%'),lateral flatten(input => NETWORKINTERFACES)), lateral flatten(input => role) group by instance_id  limit %s offset %s" % (limit,offset))
            num_results=0
            for row in data:
                num_results+=1
                if aws_writer is None:
                    aws_writer=csv.DictWriter(fou , row.keys())
                    aws_writer.writeheader()
                aws_writer.writerow(row)
            if num_results < limit:
                finished = True
            offset += limit

def grab_osquery_details(deployments):
    print("Inside function")
    conn = db.connect()
    cur = conn.cursor()
    osquery_query = db.fetch(conn, "SHOW VIEWS LIKE 'osquery_v' IN SECURITY.PROD")
    query_text = None
    print(type(osquery_query))
    for row in osquery_query:
        query_text = row["text"]
    query_text = query_text.split('union all')
    for query in query_text:
        deployments.append(re.findall('from (.*)', query)[0])



def query_snowflake(query):
    global writer #, lock
    finished = False
    offset = 0
    limit = 10000000
    while(not finished):
        num_results = 0
        conn = db.connect()
        #conn.cursor().execute("USE WAREHOUSE SNOWHOUSE;")
        query_with_limit = query + " limit %s offset %s" % (limit, offset)
        data = db.fetch(conn, query_with_limit)
        for row in data:
            num_results += 1
            #with lock:
            if writer is None:
                writer = csv.DictWriter(sys.stdout , row.keys())
                writer.writeheader()
            writer.writerow(row)
        if(num_results < limit):
            finished = True
        offset += limit
            
        

pull_aws_data()

deployments = []
grab_osquery_details(deployments)

queries = []
for i in deployments:
    queries.append("select raw:\"columns\":\"path\"::string as process, date_trunc(day, event_time) as day, raw:\"instance_id\" as instance_id, count(*) as num_starts from {} where event_time >= dateadd(day,-1,current_timestamp) AND event_time < dateadd(minute,-60,current_timestamp) AND NAME like 'process_events' group by 1,2,3 order by DAY, PROCESS, INSTANCE_ID".format(i))
    #queries.append("select DAY, PROCESS::string as PROCESS, INSTANCE_ID::string as INSTANCE_ID, NUM_STARTS AS HITS from {} order by DAY, PROCESS, INSTANCE_ID".format(i))




def init(l):
    global lock, writer
    lock = l
    writer = None
#l = multiprocessing.Lock()
#pool = multiprocessing.Pool(len(deployments),initializer=init, initargs=(l,))

#results = pool.map(query_snowflake, queries)
#pool.close()
#pool.join()
writer = None
for query in queries:
    query_snowflake(query)

