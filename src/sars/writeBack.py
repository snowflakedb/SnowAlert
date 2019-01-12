#!/usr/local/bin/python
import csv
import sys
writer=None
from runners.helpers import db

reader = csv.reader(sys.stdin)


columns = ["%s STRING" %col if len(col)>2 else "ID_COL STRING" for col in  next(reader)] 

values_list = ['(' + ','.join(["'%s'" % x for x in data]) + ')' for data in reader]
query= "insert into SNOWALERT.DATA.PROCESS_BASELINE VALUES %s" %', '.join(values_list)
print(query)
create_query = "CREATE OR REPLACE TABLE SNOWALERT.DATA.PROCESS_BASELINE (%s)" %','.join(columns )
conn = db.connect()
conn.cursor().execute("USE ROLE APP_SNOWALERT;")
conn.cursor().execute("DROP TABLE IF EXISTS SNOWALERT.DATA.PROCESS_BASELINE")
conn.cursor().execute(create_query)
conn.cursor().execute(query)
conn.cursor().close()