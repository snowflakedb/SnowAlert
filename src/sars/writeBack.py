#!/usr/bin/env python
import csv
import sys
writer=None
from queue import Queue
from runners.helpers import db
reader = csv.reader(sys.stdin)

columns = ["%s STRING" %col if len(col)>2 else "ID_COL STRING" for col in  next(reader)] 
values_list = ['(' + ','.join(["'%s'" % x for x in data]) + ')' for data in reader]
query= "insert into SNOWALERT.DATA.PROCESS_BASELINE VALUES %s" %', '.join(values_list)
create_query = "CREATE OR REPLACE TABLE SNOWALERT.DATA.PROCESS_BASELINE (%s) COPY GRANTS" %','.join(columns )

db.execute("TRUNCATE TABLE IF EXISTS SNOWALERT.DATA.PROCESS_BASELINE")
db.execute(create_query)
db.execute(query)
db.execute("GRANT INSERT,DELETE,UPDATE,SELECT ON TABLE SNOWALERT.DATA.PROCESS_BASELINE TO ROLE SECURITY_ENGINEER")
