from runners import alert_queries_runner
from runners import alert_suppressions_runner
from runners import alert_handler
from runners.helpers import db
import datetime

import unittest

CTX = db.connect()


def setup():
    t = datetime.datetime.now()
    alert_queries_runner.main()
    #alert_suppressions_runner.main()
    #alert_handler.main()


#  We need a test which checks to make sure that an alert was created properly

def alert_test():
    # How do we assert that the result returned here is from the most recent test run?
    query = """
            select alert from snowalert.results.alerts
            where alert:QUERY_ID = 'd839e4d0695c4a9db582c681f87b6ced'
            order by alert_time desc
            limit 1
            """
    alert = list(db.fetch(CTX, query))[0]
    print(alert)

    print(alert['ALERT_TIME'])

    return False


# We need a test to make sure a ticket was created properly

def ticket_test():

    return False


def main():
    setup()
    alert_test()


if __name__ == '__main__':
    main()
