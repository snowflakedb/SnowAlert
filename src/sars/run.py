#!/usr/bin/python3
import csv
import sys
import logging
import os
from runners.helpers import db
import multiprocessing
from queue import Queue

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

def query_snowflake(query):
    global writer, lock
    finished = False
    offset = 0
    limit = 100000
    while(not finished):
        num_results = 0
        conn = db.connect()
        #conn.cursor().execute("USE WAREHOUSE SNOWHOUSE;")
        query_with_limit = query + " limit %s offset %s" % (limit, offset)
        data = db.fetch(conn, query_with_limit)
        for row in data:
            num_results += 1
            with lock:
                if writer is None:
                    writer = csv.DictWriter(sys.stdout , row.keys())
                    writer.writeheader()
                writer.writerow(row)
        if(num_results < limit):
            finished = True
        offset += limit
            
        

pull_aws_data()

queries = []


deployments=[]
#deployments.append("awsuseast1citadel")
deployments.append("awsuseast1goldman")
deployments.append("awsuseast1att")
deployments.append("azwesteurope")
deployments.append("prod1_capone")
deployments.append("va_capone")
#deployments.append("prod1")
#deployments.append("dev")
#deployments.append("au")
#deployments.append("eu")
#deployments.append("ie")
#deployments.append("va")

for i in deployments:
    queries.append("select DAY, PROCESS, INSTANCE_ID, NUM_STARTS AS HITS from SNOWALERT.DATA.{} order by DAY, PROCESS, INSTANCE_ID".format(i))




def init(l):
    global lock, writer
    lock = l
    writer = None
l = multiprocessing.Lock()
pool = multiprocessing.Pool(len(deployments),initializer=init, initargs=(l,))

results = pool.map(query_snowflake, queries)
pool.close()
pool.join()