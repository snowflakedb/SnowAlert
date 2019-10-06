#!/usr/bin/env python
import csv
import sys

writer = None
from queue import Queue
from runners.helpers import db

reader = csv.reader(sys.stdin)

columns = [
    "%s STRING" % col if len(col) > 2 else "ID_COL STRING" for col in next(reader)
]
values_list = ['(' + ','.join(["'%s'" % x for x in data]) + ')' for data in reader]

db.execute(
    "CREATE OR REPLACE TABLE SNOWALERT.DATA.PROCESS_BASELINE (%s) COPY GRANTS"
    % ','.join(columns)
)
db.execute(
    "insert into SNOWALERT.DATA.PROCESS_BASELINE VALUES %s" % ', '.join(values_list)
)
