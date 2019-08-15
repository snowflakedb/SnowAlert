#!/usr/bin/env python
print("WRITE BACK")
import csv
import sys
writer=None

from queue import Queue

from runners.helpers import db
print("Starting")
reader = csv.reader(sys.stdin)


columns = ["%s STRING" %col if len(col)>2 else "ID_COL STRING" for col in  next(reader)] 

values_list = ['(' + ','.join(["'%s'" % x for x in data]) + ')' for data in reader]
query= "insert into SNOWALERT.DATA.PROCESS_BASELINE VALUES %s" %', '.join(values_list)
create_query = "CREATE OR REPLACE TABLE SNOWALERT.DATA.PROCESS_BASELINE (%s) COPY GRANTS" %','.join(columns )
#print(create_query)
conn = db.connect()
conn.cursor().execute("USE ROLE SECURITY_ENGINEER;")
conn.cursor().execute("TRUNCATE TABLE IF EXISTS SNOWALERT.DATA.PROCESS_BASELINE")
print("Table truncated")
conn.cursor().execute(create_query)
conn.cursor().execute(query)
print("Values inserted")
conn.cursor().execute("GRANT INSERT,DELETE,UPDATE,SELECT ON TABLE SNOWALERT.DATA.PROCESS_BASELINE TO ROLE APP_SNOWALERT")
print("Grants added")
conn.cursor().commit()
conn.cursor().close()